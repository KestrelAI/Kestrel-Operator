package ingestion

import (
	"context"
	"fmt"
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

// Start begins watching for workload changes using modern informer factory
func (wi *WorkloadIngester) Start(ctx context.Context) error {
	return wi.StartWithInitialSync(ctx, nil)
}

// StartWithInitialSync starts the workload ingester and signals when initial sync is complete
func (wi *WorkloadIngester) StartWithInitialSync(ctx context.Context, syncDone chan<- error) error {
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
	close(wi.stopCh)
	wi.logger.Info("Stopped workload ingester")
	return nil
}

// Stop stops the workload ingester
func (wi *WorkloadIngester) Stop() {
	close(wi.stopCh)
}

// Modern informer setup methods - much cleaner than the old cache.NewInformer approach
func (wi *WorkloadIngester) setupDeploymentInformer() {
	deploymentInformer := wi.informerFactory.Apps().V1().Deployments().Informer()

	_, err := deploymentInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if deployment, ok := newObj.(*appsv1.Deployment); ok {
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				wi.sendWorkload(deployment.ObjectMeta, "Deployment", "DELETE")
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
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if statefulSet, ok := newObj.(*appsv1.StatefulSet); ok {
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if statefulSet, ok := obj.(*appsv1.StatefulSet); ok {
				wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "DELETE")
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
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if daemonSet, ok := newObj.(*appsv1.DaemonSet); ok {
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if daemonSet, ok := obj.(*appsv1.DaemonSet); ok {
				wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "DELETE")
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
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if replicaSet, ok := newObj.(*appsv1.ReplicaSet); ok {
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if replicaSet, ok := obj.(*appsv1.ReplicaSet); ok {
				wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "DELETE")
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
				wi.sendWorkload(job.ObjectMeta, "Job", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if job, ok := newObj.(*batchv1.Job); ok {
				wi.sendWorkload(job.ObjectMeta, "Job", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if job, ok := obj.(*batchv1.Job); ok {
				wi.sendWorkload(job.ObjectMeta, "Job", "DELETE")
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
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if cronJob, ok := newObj.(*batchv1.CronJob); ok {
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if cronJob, ok := obj.(*batchv1.CronJob); ok {
				wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "DELETE")
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
					wi.sendWorkload(pod.ObjectMeta, "Pod", "CREATE")
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				if len(pod.OwnerReferences) == 0 {
					wi.sendWorkload(pod.ObjectMeta, "Pod", "UPDATE")
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				if len(pod.OwnerReferences) == 0 {
					wi.sendWorkload(pod.ObjectMeta, "Pod", "DELETE")
				}
			}
		},
	})
	if err != nil {
		wi.logger.Error("Failed to add pod event handler", zap.Error(err))
	}
}

// sendWorkload sends a workload event to the stream
func (wi *WorkloadIngester) sendWorkload(meta metav1.ObjectMeta, kind, action string) {
	workload := &v1.Workload{
		Name:      meta.Name,
		Namespace: meta.Namespace,
		Uid:       string(meta.UID),
		Kind:      kind,
		Labels:    meta.Labels,
		CreatedAt: timestamppb.New(meta.CreationTimestamp.Time),
		Action:    action,
	}

	select {
	case wi.workloadChan <- workload:
		wi.logger.Debug("Sent workload event",
			zap.String("name", workload.Name),
			zap.String("namespace", workload.Namespace),
			zap.String("kind", workload.Kind),
			zap.String("action", workload.Action))
	default:
		wi.logger.Warn("Workload channel full, dropping event",
			zap.String("name", workload.Name),
			zap.String("namespace", workload.Namespace),
			zap.String("kind", workload.Kind),
			zap.String("action", workload.Action))
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
		wi.sendWorkload(deployment.ObjectMeta, "Deployment", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingStatefulSets(ctx context.Context) error {
	statefulSets, err := wi.clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list statefulsets: %w", err)
	}

	for _, statefulSet := range statefulSets.Items {
		wi.sendWorkload(statefulSet.ObjectMeta, "StatefulSet", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingDaemonSets(ctx context.Context) error {
	daemonSets, err := wi.clientset.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list daemonsets: %w", err)
	}

	for _, daemonSet := range daemonSets.Items {
		wi.sendWorkload(daemonSet.ObjectMeta, "DaemonSet", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingReplicaSets(ctx context.Context) error {
	replicaSets, err := wi.clientset.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list replicasets: %w", err)
	}

	for _, replicaSet := range replicaSets.Items {
		wi.sendWorkload(replicaSet.ObjectMeta, "ReplicaSet", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingJobs(ctx context.Context) error {
	jobs, err := wi.clientset.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, job := range jobs.Items {
		wi.sendWorkload(job.ObjectMeta, "Job", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingCronJobs(ctx context.Context) error {
	cronJobs, err := wi.clientset.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list cronjobs: %w", err)
	}

	for _, cronJob := range cronJobs.Items {
		wi.sendWorkload(cronJob.ObjectMeta, "CronJob", "CREATE")
	}
	return nil
}

func (wi *WorkloadIngester) sendExistingPods(ctx context.Context) error {
	pods, err := wi.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range pods.Items {
		// Only track standalone pods (no owner references)
		if len(pod.OwnerReferences) == 0 {
			wi.sendWorkload(pod.ObjectMeta, "Pod", "CREATE")
		}
	}
	return nil
}
