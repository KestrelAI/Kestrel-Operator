package ingestion

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type WorkloadIngester struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	workloadChan    chan *v1.Workload
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
}

// NewWorkloadIngester creates a new workload ingester using modern informer factory
func NewWorkloadIngester(logger *zap.Logger, workloadChan chan *v1.Workload) (*WorkloadIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory - this is the modern way
	// It automatically handles caching, reconnections, and resource management
	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	return &WorkloadIngester{
		clientset:       clientset,
		logger:          logger,
		workloadChan:    workloadChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}, nil
}

// StartSync starts the workload ingester and signals when initial sync is complete
func (wi *WorkloadIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	wi.logger.Info("Starting workload ingester with modern informer factory")

	// Set up informers for different workload types
	wi.setupDeploymentInformer()
	wi.setupStatefulSetInformer()
	wi.setupDaemonSetInformer()
	wi.setupReplicaSetInformer()
	wi.setupJobInformer()
	wi.setupCronJobInformer()
	wi.setupPodInformer()

	// Send initial inventory before starting informers
	if err := wi.sendInitialWorkloadInventory(ctx); err != nil {
		wi.logger.Error("Failed to send initial workload inventory", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers - this replaces the manual controller.Run calls
	wi.informerFactory.Start(wi.stopCh)

	// Wait for all caches to sync before processing events
	wi.logger.Info("Waiting for workload informer caches to sync...")
	if !cache.WaitForCacheSync(wi.stopCh,
		wi.informerFactory.Apps().V1().Deployments().Informer().HasSynced,
		wi.informerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
		wi.informerFactory.Apps().V1().DaemonSets().Informer().HasSynced,
		wi.informerFactory.Apps().V1().ReplicaSets().Informer().HasSynced,
		wi.informerFactory.Batch().V1().Jobs().Informer().HasSynced,
		wi.informerFactory.Batch().V1().CronJobs().Informer().HasSynced,
		wi.informerFactory.Core().V1().Pods().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for workload informer caches to sync")
	}
	wi.logger.Info("All workload informer caches synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	wi.safeClose()
	wi.logger.Info("Stopped workload ingester")
	return nil
}

// Stop stops the workload ingester
func (wi *WorkloadIngester) Stop() {
	wi.safeClose()
}

// safeClose safely closes the stop channel only once
func (wi *WorkloadIngester) safeClose() {
	wi.mu.Lock()
	defer wi.mu.Unlock()
	if !wi.stopped {
		close(wi.stopCh)
		wi.stopped = true
	}
}

// Modern informer setup methods - much cleaner than the old cache.NewInformer approach
func (wi *WorkloadIngester) setupDeploymentInformer() {
	deploymentInformer := wi.informerFactory.Apps().V1().Deployments().Informer()

	_, err := deploymentInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				serviceAccount := deployment.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default" // Kubernetes defaults to "default" if not specified
				}
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if deployment, ok := newObj.(*appsv1.Deployment); ok {
				serviceAccount := deployment.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				serviceAccount := deployment.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add deployment event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupStatefulSetInformer() {
	statefulSetInformer := wi.informerFactory.Apps().V1().StatefulSets().Informer()

	_, err := statefulSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if statefulSet, ok := obj.(*appsv1.StatefulSet); ok {
				serviceAccount := statefulSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if statefulSet, ok := newObj.(*appsv1.StatefulSet); ok {
				serviceAccount := statefulSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if statefulSet, ok := obj.(*appsv1.StatefulSet); ok {
				serviceAccount := statefulSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add statefulset event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupDaemonSetInformer() {
	daemonSetInformer := wi.informerFactory.Apps().V1().DaemonSets().Informer()

	_, err := daemonSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if daemonSet, ok := obj.(*appsv1.DaemonSet); ok {
				serviceAccount := daemonSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if daemonSet, ok := newObj.(*appsv1.DaemonSet); ok {
				serviceAccount := daemonSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if daemonSet, ok := obj.(*appsv1.DaemonSet); ok {
				serviceAccount := daemonSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add daemonset event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupReplicaSetInformer() {
	replicaSetInformer := wi.informerFactory.Apps().V1().ReplicaSets().Informer()

	_, err := replicaSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if replicaSet, ok := obj.(*appsv1.ReplicaSet); ok {
				serviceAccount := replicaSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if replicaSet, ok := newObj.(*appsv1.ReplicaSet); ok {
				serviceAccount := replicaSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if replicaSet, ok := obj.(*appsv1.ReplicaSet); ok {
				serviceAccount := replicaSet.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add replicaset event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupJobInformer() {
	jobInformer := wi.informerFactory.Batch().V1().Jobs().Informer()

	_, err := jobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if job, ok := obj.(*batchv1.Job); ok {
				serviceAccount := job.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(job.ObjectMeta, "Job", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if job, ok := newObj.(*batchv1.Job); ok {
				serviceAccount := job.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(job.ObjectMeta, "Job", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if job, ok := obj.(*batchv1.Job); ok {
				serviceAccount := job.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(job.ObjectMeta, "Job", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add job event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupCronJobInformer() {
	cronJobInformer := wi.informerFactory.Batch().V1().CronJobs().Informer()

	_, err := cronJobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if cronJob, ok := obj.(*batchv1.CronJob); ok {
				serviceAccount := cronJob.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "CREATE", serviceAccount)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if cronJob, ok := newObj.(*batchv1.CronJob); ok {
				serviceAccount := cronJob.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "UPDATE", serviceAccount)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if cronJob, ok := obj.(*batchv1.CronJob); ok {
				serviceAccount := cronJob.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName
				if serviceAccount == "" {
					serviceAccount = "default"
				}
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "DELETE", serviceAccount)
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add cronjob event handler", zap.Error(err))
	}
}

func (wi *WorkloadIngester) setupPodInformer() {
	podInformer := wi.informerFactory.Core().V1().Pods().Informer()

	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				// Only track standalone pods (no owner references)
				if len(pod.OwnerReferences) == 0 {
					serviceAccount := pod.Spec.ServiceAccountName
					if serviceAccount == "" {
						serviceAccount = "default"
					}
					wi.sendWorkload(pod.ObjectMeta, "Pod", "CREATE", serviceAccount)
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				if len(pod.OwnerReferences) == 0 {
					serviceAccount := pod.Spec.ServiceAccountName
					if serviceAccount == "" {
						serviceAccount = "default"
					}
					wi.sendWorkload(pod.ObjectMeta, "Pod", "UPDATE", serviceAccount)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				if len(pod.OwnerReferences) == 0 {
					serviceAccount := pod.Spec.ServiceAccountName
					if serviceAccount == "" {
						serviceAccount = "default"
					}
					wi.sendWorkload(pod.ObjectMeta, "Pod", "DELETE", serviceAccount)
				}
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add pod event handler", zap.Error(err))
	}
}

// sendWorkloadWithServiceAccount sends a workload event to the stream with service account information
func (wi *WorkloadIngester) sendWorkload(meta metav1.ObjectMeta, kind, action, serviceAccount string) {
	workload := &v1.Workload{
		Name:           meta.Name,
		Namespace:      meta.Namespace,
		Uid:            string(meta.UID),
		Kind:           kind,
		Labels:         meta.Labels,
		CreatedAt:      timestamppb.New(meta.CreationTimestamp.Time),
		Action:         stringToAction(action),
		ServiceAccount: serviceAccount,
	}

	select {
	case wi.workloadChan <- workload:
		wi.logger.Debug("Sent workload event with service account",
			zap.String("name", workload.Name),
			zap.String("namespace", workload.Namespace),
			zap.String("kind", workload.Kind),
			zap.String("serviceAccount", workload.ServiceAccount),
			zap.String("action", workload.Action.String()))
	default:
		wi.logger.Warn("Workload channel full, dropping event",
			zap.String("name", workload.Name),
			zap.String("namespace", workload.Namespace),
			zap.String("kind", workload.Kind),
			zap.String("serviceAccount", workload.ServiceAccount),
			zap.String("action", workload.Action.String()))
	}
}

// sendInitialWorkloadInventory sends all existing workloads to the server
func (wi *WorkloadIngester) sendInitialWorkloadInventory(ctx context.Context) error {
	wi.logger.Info("Sending initial workload inventory using direct API calls")

	// Get all existing workloads using direct API calls (not cached)
	if err := wi.sendExistingDeployments(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingStatefulSets(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingDaemonSets(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingReplicaSets(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingJobs(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingCronJobs(ctx); err != nil {
		return err
	}
	if err := wi.sendExistingPods(ctx); err != nil {
		return err
	}

	wi.logger.Info("Completed sending initial workload inventory")
	return nil
}

// Helper functions to send existing workloads using direct API calls
func (wi *WorkloadIngester) sendExistingDeployments(ctx context.Context) error {
	deployments, err := wi.clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %w", err)
	}

	for _, deployment := range deployments.Items {
		serviceAccount := deployment.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(deployment.ObjectMeta, "Deployment", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingStatefulSets(ctx context.Context) error {
	statefulSets, err := wi.clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list statefulsets: %w", err)
	}

	for _, statefulSet := range statefulSets.Items {
		serviceAccount := statefulSet.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingDaemonSets(ctx context.Context) error {
	daemonSets, err := wi.clientset.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list daemonsets: %w", err)
	}

	for _, daemonSet := range daemonSets.Items {
		serviceAccount := daemonSet.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingReplicaSets(ctx context.Context) error {
	replicaSets, err := wi.clientset.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list replicasets: %w", err)
	}

	for _, replicaSet := range replicaSets.Items {
		serviceAccount := replicaSet.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingJobs(ctx context.Context) error {
	jobs, err := wi.clientset.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, job := range jobs.Items {
		serviceAccount := job.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(job.ObjectMeta, "Job", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingCronJobs(ctx context.Context) error {
	cronJobs, err := wi.clientset.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list cronjobs: %w", err)
	}

	for _, cronJob := range cronJobs.Items {
		serviceAccount := cronJob.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "CREATE", serviceAccount)
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingPods(ctx context.Context) error {
	pods, err := wi.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range pods.Items {
		// Only send standalone pods (no owner references)
		if len(pod.OwnerReferences) == 0 {
			serviceAccount := pod.Spec.ServiceAccountName
			if serviceAccount == "" {
				serviceAccount = "default"
			}
			wi.sendWorkload(pod.ObjectMeta, "Pod", "CREATE", serviceAccount)
		}
	}
	return nil
}
