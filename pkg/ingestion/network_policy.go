package ingestion

import (
	"context"
	"fmt"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// NetworkPolicyIngester handles the ingestion of network policies from a Kubernetes cluster
type NetworkPolicyIngester struct {
	clientset         *kubernetes.Clientset
	logger            *zap.Logger
	networkPolicyChan chan *v1.NetworkPolicy
	informerFactory   informers.SharedInformerFactory
	stopCh            chan struct{}
}

// NewNetworkPolicyIngester creates a new NetworkPolicyIngester instance with channel support
func NewNetworkPolicyIngester(logger *zap.Logger, networkPolicyChan chan *v1.NetworkPolicy) (*NetworkPolicyIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory
	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	return &NetworkPolicyIngester{
		clientset:         clientset,
		logger:            logger,
		networkPolicyChan: networkPolicyChan,
		informerFactory:   informerFactory,
		stopCh:            make(chan struct{}),
	}, nil
}

// IngestNetworkPolicies fetches all network policies from the cluster
func (npi *NetworkPolicyIngester) IngestNetworkPolicies(ctx context.Context) ([]networkingv1.NetworkPolicy, error) {
	policies, err := npi.clientset.
		NetworkingV1().
		NetworkPolicies(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list network policies: %v", err)
	}

	return policies.Items, nil
}

// GetNetworkPolicy fetches a specific network policy by name and namespace
func (npi *NetworkPolicyIngester) GetNetworkPolicy(ctx context.Context, namespace, name string) (*networkingv1.NetworkPolicy, error) {
	return npi.clientset.NetworkingV1().NetworkPolicies(namespace).Get(ctx, name, metav1.GetOptions{})
}

// StartSync starts the network policy ingester and signals when initial sync is complete
func (npi *NetworkPolicyIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	if npi.logger == nil || npi.networkPolicyChan == nil {
		return fmt.Errorf("ingester not initialized with channel support, use NewNetworkPolicyIngesterWithChannel")
	}

	npi.logger.Info("Starting network policy ingester with modern informer factory")

	// Set up network policy informer
	npi.setupNetworkPolicyInformer()

	// Send initial inventory before starting informers
	if err := npi.sendInitialNetworkPolicyInventory(ctx); err != nil {
		npi.logger.Error("Failed to send initial network policy inventory", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers
	npi.informerFactory.Start(npi.stopCh)

	// Wait for all caches to sync before processing events
	npi.logger.Info("Waiting for network policy informer cache to sync...")
	if !cache.WaitForCacheSync(npi.stopCh,
		npi.informerFactory.Networking().V1().NetworkPolicies().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for network policy informer cache to sync")
	}
	npi.logger.Info("Network policy informer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	close(npi.stopCh)
	npi.logger.Info("Stopped network policy ingester")
	return nil
}

// Stop stops the network policy ingester
func (npi *NetworkPolicyIngester) Stop() {
	if npi.stopCh != nil {
		close(npi.stopCh)
	}
}

// setupNetworkPolicyInformer sets up the modern network policy informer
func (npi *NetworkPolicyIngester) setupNetworkPolicyInformer() {
	networkPolicyInformer := npi.informerFactory.Networking().V1().NetworkPolicies().Informer()

	_, err := networkPolicyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if np, ok := obj.(*networkingv1.NetworkPolicy); ok {
				npi.sendNetworkPolicy(np, "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if np, ok := newObj.(*networkingv1.NetworkPolicy); ok {
				npi.sendNetworkPolicy(np, "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if np, ok := obj.(*networkingv1.NetworkPolicy); ok {
				npi.sendNetworkPolicy(np, "DELETE")
			}
		},
	})
	if err != nil {
		npi.logger.Error("Failed to add network policy event handler", zap.Error(err))
	}
}

// sendInitialNetworkPolicyInventory sends all existing network policies to the server
func (npi *NetworkPolicyIngester) sendInitialNetworkPolicyInventory(ctx context.Context) error {
	npi.logger.Info("Sending initial network policy inventory using direct API calls")

	policies, err := npi.clientset.NetworkingV1().NetworkPolicies("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list network policies: %w", err)
	}

	for _, policy := range policies.Items {
		npi.sendNetworkPolicy(&policy, "CREATE")
	}

	npi.logger.Info("Completed sending initial network policy inventory")
	return nil
}

// sendNetworkPolicy converts a Kubernetes NetworkPolicy to protobuf and sends it to the stream
func (npi *NetworkPolicyIngester) sendNetworkPolicy(np *networkingv1.NetworkPolicy, action string) {
	// Resolve target workloads for this policy
	targetWorkloads := npi.ResolveTargetWorkloads(context.Background(), *np)

	// Convert to protobuf format
	protoNP := &v1.NetworkPolicy{
		Metadata: &v1.ObjectMeta{
			Name:            np.Name,
			Namespace:       np.Namespace,
			Labels:          np.Labels,
			Annotations:     np.Annotations,
			ResourceVersion: np.ResourceVersion,
			Uid:             string(np.UID),
		},
		Spec:            convertNetworkPolicySpecToProto(np.Spec),
		TargetWorkloads: targetWorkloads,
		Action:          stringToAction(action),
	}

	select {
	case npi.networkPolicyChan <- protoNP:
		npi.logger.Debug("Sent network policy event",
			zap.String("name", protoNP.Metadata.Name),
			zap.String("namespace", protoNP.Metadata.Namespace),
			zap.String("action", protoNP.Action.String()),
			zap.Int("target_workloads", len(targetWorkloads)))
	default:
		npi.logger.Warn("Network policy channel full, dropping event",
			zap.String("name", protoNP.Metadata.Name),
			zap.String("namespace", protoNP.Metadata.Namespace),
			zap.String("action", protoNP.Action.String()))
	}
}

// convertNetworkPolicySpecToProto converts a K8s NetworkPolicySpec to proto format
func convertNetworkPolicySpecToProto(spec networkingv1.NetworkPolicySpec) *v1.NetworkPolicySpec {
	protoSpec := &v1.NetworkPolicySpec{
		PodSelector: convertLabelSelectorToProto(&spec.PodSelector),
		PolicyTypes: convertPolicyTypesToStrings(spec.PolicyTypes),
	}

	// Convert ingress rules
	for _, ingress := range spec.Ingress {
		protoIngress := &v1.NetworkPolicyIngressRule{}
		for _, from := range ingress.From {
			protoPeer := &v1.NetworkPolicyPeer{}
			if from.PodSelector != nil {
				protoPeer.PodSelector = convertLabelSelectorToProto(from.PodSelector)
			}
			if from.NamespaceSelector != nil {
				protoPeer.NamespaceSelector = convertLabelSelectorToProto(from.NamespaceSelector)
			}
			if from.IPBlock != nil {
				protoPeer.IpBlock = &v1.IPBlock{
					Cidr:   from.IPBlock.CIDR,
					Except: from.IPBlock.Except,
				}
			}
			protoIngress.From = append(protoIngress.From, protoPeer)
		}
		for _, port := range ingress.Ports {
			protoIngress.Ports = append(protoIngress.Ports, convertNetworkPolicyPortToProto(&port))
		}
		protoSpec.Ingress = append(protoSpec.Ingress, protoIngress)
	}

	// Convert egress rules
	for _, egress := range spec.Egress {
		protoEgress := &v1.NetworkPolicyEgressRule{}
		for _, to := range egress.To {
			protoPeer := &v1.NetworkPolicyPeer{}
			if to.PodSelector != nil {
				protoPeer.PodSelector = convertLabelSelectorToProto(to.PodSelector)
			}
			if to.NamespaceSelector != nil {
				protoPeer.NamespaceSelector = convertLabelSelectorToProto(to.NamespaceSelector)
			}
			if to.IPBlock != nil {
				protoPeer.IpBlock = &v1.IPBlock{
					Cidr:   to.IPBlock.CIDR,
					Except: to.IPBlock.Except,
				}
			}
			protoEgress.To = append(protoEgress.To, protoPeer)
		}
		for _, port := range egress.Ports {
			protoEgress.Ports = append(protoEgress.Ports, convertNetworkPolicyPortToProto(&port))
		}
		protoSpec.Egress = append(protoSpec.Egress, protoEgress)
	}

	return protoSpec
}

// Helper functions for converting K8s to proto
func convertLabelSelectorToProto(ls *metav1.LabelSelector) *v1.LabelSelector {
	if ls == nil {
		return nil
	}
	protoLS := &v1.LabelSelector{
		MatchLabels: ls.MatchLabels,
	}
	for _, req := range ls.MatchExpressions {
		protoReq := &v1.LabelSelectorRequirement{
			Key:      req.Key,
			Operator: string(req.Operator),
			Values:   req.Values,
		}
		protoLS.MatchExpressions = append(protoLS.MatchExpressions, protoReq)
	}
	return protoLS
}

func convertPolicyTypesToStrings(types []networkingv1.PolicyType) []string {
	var result []string
	for _, t := range types {
		result = append(result, string(t))
	}
	return result
}

func convertNetworkPolicyPortToProto(port *networkingv1.NetworkPolicyPort) *v1.NetworkPolicyPort {
	if port == nil {
		return nil
	}
	protoPort := &v1.NetworkPolicyPort{}
	if port.Protocol != nil {
		protoPort.Protocol = string(*port.Protocol)
	}
	if port.Port != nil {
		if port.Port.Type == intstr.Int {
			protoPort.PortValue = &v1.NetworkPolicyPort_Port{
				Port: port.Port.IntVal,
			}
		} else {
			protoPort.PortValue = &v1.NetworkPolicyPort_PortName{
				PortName: port.Port.StrVal,
			}
		}
	}
	if port.EndPort != nil {
		protoPort.EndPort = *port.EndPort
	}
	return protoPort
}

// ResolveTargetWorkloads resolves a network policy to the target workloads to which it applies
func (npi *NetworkPolicyIngester) ResolveTargetWorkloads(ctx context.Context, np networkingv1.NetworkPolicy) []string {
	podList, err := npi.clientset.CoreV1().Pods(np.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(np.Spec.PodSelector.MatchLabels).String(),
	})
	if err != nil {
		return nil
	}

	targetWorkloads := sets.New[string]()
	for _, p := range podList.Items {
		targetWorkloads.Insert(fmt.Sprintf("%s.Pod.%s", np.Namespace, p.Name)) // Bare Pod key

		// Every owner of the Pod in the chain
		for _, ref := range p.OwnerReferences {
			key := fmt.Sprintf("%s.%s.%s", np.Namespace, ref.Kind, ref.Name)
			targetWorkloads.Insert(key)

			// Special handling for ReplicaSet->Deployment and Job->CronJob relationships;
			// this is necessary because ReplicaSets and Jobs are not the root controllers – the root controllers
			// are Deployments and CronJobs, respectively, so we need to also include these "grandparent"
			// controllers of the Pod in the targetWorkloads set. This ensures that, no matter what controller
			// Cilium chooses for the Pod traffic, we will always be able to do this stitching.
			switch ref.Kind {
			case "ReplicaSet":
				if d := deploymentName(ref.Name); d != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", np.Namespace, "Deployment", d))
				}
			case "Job":
				if cj := cronJobName(ref.Name); cj != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", np.Namespace, "CronJob", cj))
				}
			}
		}
	}
	return targetWorkloads.UnsortedList()
}

// ApplyNetworkPolicy applies a network policy received from the server to the cluster
func ApplyNetworkPolicy(ctx context.Context, k8sClient *kubernetes.Clientset, policy *networkingv1.NetworkPolicy) error {
	// Check for nil policy or client
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	if k8sClient == nil {
		return fmt.Errorf("k8s client cannot be nil")
	}

	// Apply or update the policy
	_, err := k8sClient.NetworkingV1().NetworkPolicies(policy.Namespace).Create(ctx, policy, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			current, _ := k8sClient.NetworkingV1().
				NetworkPolicies(policy.Namespace).
				Get(ctx, policy.Name, metav1.GetOptions{})

			policy.ResourceVersion = current.ResourceVersion

			_, err = k8sClient.NetworkingV1().
				NetworkPolicies(policy.Namespace).
				Update(ctx, policy, metav1.UpdateOptions{})

			return err
		}
		return err
	}

	return nil
}

func CheckNetworkPolicy(logger *zap.Logger, policy string) error {
	// Check if the policy is empty
	if policy == "" {
		return fmt.Errorf("policy is empty")
	}

	// Parse the YAML into a NetworkPolicy object
	networkPolicy, err := ParseNetworkPolicyYAML(logger, policy)
	if err != nil {
		return fmt.Errorf("failed to parse policy: %v", err)
	}

	// Get a clientset for the dry run
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return fmt.Errorf("failed to create k8s clientset: %v", err)
	}

	// Perform a dry run create
	_, err = clientset.NetworkingV1().
		NetworkPolicies(networkPolicy.Namespace).
		Create(context.Background(), networkPolicy, metav1.CreateOptions{
			DryRun: []string{"All"},
		})

	if err != nil {
		return fmt.Errorf("dry run failed: %v", err)
	}

	return nil
}

// ConvertToK8sNetworkPolicy converts our proto NetworkPolicy back to the
// Kubernetes API object (networking.k8s.io/v1).
func ConvertToK8sNetworkPolicy(p *v1.NetworkPolicy) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.GetMetadata().GetName(),
			Namespace:   p.GetMetadata().GetNamespace(),
			Labels:      p.GetMetadata().GetLabels(),
			Annotations: p.GetMetadata().GetAnnotations(),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: ConvertToK8sLabelSelector(p.GetSpec().GetPodSelector()),
			PolicyTypes: convertToK8sPolicyTypes(p.GetSpec().GetPolicyTypes()),
			Ingress:     convertToK8sIngressRules(p.GetSpec().GetIngress()),
			Egress:      convertToK8sEgressRules(p.GetSpec().GetEgress()),
		},
	}
}

/* ---------- helpers ---------- */

func convertToK8sPolicyTypes(types []string) []networkingv1.PolicyType {
	out := make([]networkingv1.PolicyType, 0, len(types))
	for _, t := range types {
		out = append(out, networkingv1.PolicyType(t)) // "Ingress"/"Egress"
	}
	return out
}

func convertToK8sIngressRules(in []*v1.NetworkPolicyIngressRule) []networkingv1.NetworkPolicyIngressRule {
	out := make([]networkingv1.NetworkPolicyIngressRule, 0, len(in))
	for _, r := range in {
		out = append(out, networkingv1.NetworkPolicyIngressRule{
			From:  convertToK8sPeers(r.From),
			Ports: convertToK8sPorts(r.Ports),
		})
	}
	return out
}

func convertToK8sEgressRules(eg []*v1.NetworkPolicyEgressRule) []networkingv1.NetworkPolicyEgressRule {
	out := make([]networkingv1.NetworkPolicyEgressRule, 0, len(eg))
	for _, r := range eg {
		out = append(out, networkingv1.NetworkPolicyEgressRule{
			To:    convertToK8sPeers(r.To),
			Ports: convertToK8sPorts(r.Ports),
		})
	}
	return out
}

func convertToK8sPeers(peers []*v1.NetworkPolicyPeer) []networkingv1.NetworkPolicyPeer {
	out := make([]networkingv1.NetworkPolicyPeer, 0, len(peers))
	for _, p := range peers {
		out = append(out, networkingv1.NetworkPolicyPeer{
			PodSelector:       ConvertToK8sLabelSelectorPtr(p.PodSelector),
			NamespaceSelector: ConvertToK8sLabelSelectorPtr(p.NamespaceSelector),
			IPBlock:           convertToK8sIPBlockPtr(p.IpBlock),
		})
	}
	return out
}

func convertToK8sPorts(ports []*v1.NetworkPolicyPort) []networkingv1.NetworkPolicyPort {
	out := make([]networkingv1.NetworkPolicyPort, 0, len(ports))
	for _, pp := range ports {
		var protoPtr *corev1.Protocol
		if pp.Protocol != "" {
			p := corev1.Protocol(pp.Protocol)
			protoPtr = &p
		}

		k8sPort := networkingv1.NetworkPolicyPort{
			Protocol: protoPtr,
		}

		switch v := pp.PortValue.(type) {
		case *v1.NetworkPolicyPort_Port:
			port := intstr.FromInt(int(v.Port))
			k8sPort.Port = &port
		case *v1.NetworkPolicyPort_PortName:
			port := intstr.FromString(v.PortName)
			k8sPort.Port = &port
		}

		if pp.EndPort != 0 {
			end := int32(pp.EndPort)
			k8sPort.EndPort = &end
		}
		out = append(out, k8sPort)
	}
	return out
}

/* ----- label / selector helpers ----- */

// ConvertToK8sLabelSelector converts a proto LabelSelector to a K8s LabelSelector
func ConvertToK8sLabelSelector(sel *v1.LabelSelector) metav1.LabelSelector {
	if sel == nil {
		return metav1.LabelSelector{}
	}
	return metav1.LabelSelector{
		MatchLabels:      sel.MatchLabels,
		MatchExpressions: convertToK8sLabelExprs(sel.MatchExpressions),
	}
}

// ConvertToK8sLabelSelectorPtr converts a proto LabelSelector to a K8s LabelSelector pointer
func ConvertToK8sLabelSelectorPtr(sel *v1.LabelSelector) *metav1.LabelSelector {
	if sel == nil {
		return nil
	}
	tmp := ConvertToK8sLabelSelector(sel)
	return &tmp
}

func convertToK8sLabelExprs(exprs []*v1.LabelSelectorRequirement) []metav1.LabelSelectorRequirement {
	out := make([]metav1.LabelSelectorRequirement, 0, len(exprs))
	for _, e := range exprs {
		out = append(out, metav1.LabelSelectorRequirement{
			Key:      e.Key,
			Operator: metav1.LabelSelectorOperator(e.Operator),
			Values:   e.Values,
		})
	}
	return out
}

/* ---------- IPBlock helper ---------- */

func convertToK8sIPBlockPtr(b *v1.IPBlock) *networkingv1.IPBlock {
	if b == nil {
		return nil
	}
	return &networkingv1.IPBlock{
		CIDR:   b.Cidr,
		Except: b.Except,
	}
}

// deploymentName strips the hash suffix from a ReplicaSet name.
func deploymentName(rs string) string {
	if i := lastDash(rs); i > 0 {
		return rs[:i]
	}
	return ""
}

// cronJobName strips the timestamp suffix from a Job name.
func cronJobName(job string) string {
	if i := lastDash(job); i > 0 {
		return job[:i]
	}
	return ""
}

// lastDash returns the index of the last '-' in s, or -1.
func lastDash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '-' {
			return i
		}
	}
	return -1
}

// ParseNetworkPolicyYAML parses a YAML string into a Kubernetes NetworkPolicy object
func ParseNetworkPolicyYAML(logger *zap.Logger, yamlStr string) (*networkingv1.NetworkPolicy, error) {
	// Check for empty input
	if yamlStr == "" {
		return nil, fmt.Errorf("empty network policy YAML")
	}

	// Create a new scheme and codec factory
	scheme := runtime.NewScheme()
	codecFactory := serializer.NewCodecFactory(scheme)

	// Add the networking API group to the scheme
	if err := networkingv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add networking scheme: %v", err)
	}

	// Convert YAML to JSON (Kubernetes API machinery works with JSON)
	jsonBytes, err := yaml.ToJSON([]byte(yamlStr))
	if err != nil {
		return nil, fmt.Errorf("failed to convert YAML to JSON: %v", err)
	}

	// Create a decoder
	decoder := codecFactory.UniversalDeserializer()

	// Decode the JSON into a runtime.Object
	obj, _, err := decoder.Decode(jsonBytes, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decode network policy: %v", err)
	}

	// Type assert to NetworkPolicy
	networkPolicy, ok := obj.(*networkingv1.NetworkPolicy)
	if !ok {
		return nil, fmt.Errorf("decoded object is not a NetworkPolicy")
	}

	// Validate the network policy
	if networkPolicy.Spec.PodSelector.MatchLabels == nil && len(networkPolicy.Spec.PodSelector.MatchExpressions) == 0 {
		// Pod selector is empty, which is valid but worth checking
		logger.Warn("Pod selector is empty")
	}

	// Validate policy types
	for _, policyType := range networkPolicy.Spec.PolicyTypes {
		if policyType != networkingv1.PolicyTypeIngress && policyType != networkingv1.PolicyTypeEgress {
			return nil, fmt.Errorf("invalid policy type: %s", policyType)
		}
	}

	return networkPolicy, nil
}
