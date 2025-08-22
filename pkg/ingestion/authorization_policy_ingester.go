package ingestion

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// AuthorizationPolicyIngester handles the ingestion of Istio Authorization Policies from a Kubernetes cluster
type AuthorizationPolicyIngester struct {
	clientset               *kubernetes.Clientset
	dynamicClient           dynamic.Interface
	logger                  *zap.Logger
	authorizationPolicyChan chan *v1.AuthorizationPolicy
	informerFactory         dynamicinformer.DynamicSharedInformerFactory
	stopCh                  chan struct{}
	stopped                 bool
	mu                      sync.Mutex
	istioEnabled            bool
}

// AuthorizationPolicyResource defines the Istio AuthorizationPolicy resource
var AuthorizationPolicyResource = schema.GroupVersionResource{
	Group:    "security.istio.io",
	Version:  "v1beta1",
	Resource: "authorizationpolicies",
}

// NewAuthorizationPolicyIngester creates a new AuthorizationPolicyIngester instance with channel support
func NewAuthorizationPolicyIngester(logger *zap.Logger, authorizationPolicyChan chan *v1.AuthorizationPolicy) (*AuthorizationPolicyIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	restConfig, err := k8s_helper.NewRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Check if Istio is enabled by checking if the AuthorizationPolicy CRD exists
	istioEnabled := checkIstioEnabled(clientset, logger)

	// Create dynamic informer factory for Istio resources
	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 30*time.Second)

	return &AuthorizationPolicyIngester{
		clientset:               clientset,
		dynamicClient:           dynamicClient,
		logger:                  logger,
		authorizationPolicyChan: authorizationPolicyChan,
		informerFactory:         informerFactory,
		stopCh:                  make(chan struct{}),
		istioEnabled:            istioEnabled,
	}, nil
}

// checkIstioEnabled checks if Istio is available by looking for the AuthorizationPolicy CRD
func checkIstioEnabled(clientset *kubernetes.Clientset, logger *zap.Logger) bool {
	// Try to check if the AuthorizationPolicy CRD exists
	_, err := clientset.Discovery().ServerResourcesForGroupVersion("security.istio.io/v1beta1")
	if err != nil {
		logger.Info("Istio AuthorizationPolicy CRD not found, Istio authorization policy ingestion disabled", zap.Error(err))
		return false
	}
	logger.Info("Istio AuthorizationPolicy CRD found, enabling authorization policy ingestion")
	return true
}

// StartSync starts the authorization policy ingester and signals when initial sync is complete
func (api *AuthorizationPolicyIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	if !api.istioEnabled {
		api.logger.Info("Istio not enabled, skipping authorization policy ingestion")
		if syncDone != nil {
			syncDone <- nil
		}
		return nil
	}

	api.logger.Info("Starting Istio authorization policy ingester")

	// Set up authorization policy informer
	api.setupAuthorizationPolicyInformer()

	// Send initial inventory before starting informers
	if err := api.sendInitialAuthorizationPolicyInventory(ctx); err != nil {
		api.logger.Error("Failed to send initial authorization policy inventory", zap.Error(err))
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
	api.informerFactory.Start(api.stopCh)

	// Wait for all caches to sync before processing events
	api.logger.Info("Waiting for authorization policy informer cache to sync...")
	if !cache.WaitForCacheSync(api.stopCh,
		api.informerFactory.ForResource(AuthorizationPolicyResource).Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for authorization policy informer cache to sync")
	}
	api.logger.Info("Authorization policy informer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	api.safeClose()
	api.logger.Info("Stopped authorization policy ingester")
	return nil
}

// Stop stops the authorization policy ingester
func (api *AuthorizationPolicyIngester) Stop() {
	api.safeClose()
}

// safeClose safely closes the stop channel only once
func (api *AuthorizationPolicyIngester) safeClose() {
	api.mu.Lock()
	defer api.mu.Unlock()
	if !api.stopped {
		close(api.stopCh)
		api.stopped = true
	}
}

// setupAuthorizationPolicyInformer sets up the authorization policy informer
func (api *AuthorizationPolicyIngester) setupAuthorizationPolicyInformer() {
	authzPolicyInformer := api.informerFactory.ForResource(AuthorizationPolicyResource).Informer()

	_, err := authzPolicyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			api.sendAuthorizationPolicy(obj, "CREATE")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			api.sendAuthorizationPolicy(newObj, "UPDATE")
		},
		DeleteFunc: func(obj interface{}) {
			api.sendAuthorizationPolicy(obj, "DELETE")
		},
	})
	if err != nil {
		api.logger.Error("Failed to add authorization policy event handler", zap.Error(err))
	}
}

// sendInitialAuthorizationPolicyInventory sends all existing authorization policies to the server
func (api *AuthorizationPolicyIngester) sendInitialAuthorizationPolicyInventory(ctx context.Context) error {
	api.logger.Info("Sending initial authorization policy inventory using direct API calls")

	// List all authorization policies using dynamic client
	authzPolicies, err := api.dynamicClient.Resource(AuthorizationPolicyResource).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list authorization policies: %w", err)
	}

	for _, policy := range authzPolicies.Items {
		api.sendAuthorizationPolicy(&policy, "CREATE")
	}

	api.logger.Info("Completed sending initial authorization policy inventory", zap.Int("count", len(authzPolicies.Items)))
	return nil
}

// sendAuthorizationPolicy converts a Kubernetes AuthorizationPolicy to protobuf and sends it to the stream
func (api *AuthorizationPolicyIngester) sendAuthorizationPolicy(obj interface{}, action string) {
	// Handle tombstone objects from cache deletions
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
		api.logger.Debug("Unwrapped tombstone object", zap.String("action", action))
	}

	// Log the object type for debugging
	api.logger.Debug("Processing authorization policy object", zap.String("type", fmt.Sprintf("%T", obj)), zap.String("action", action))

	// Convert unstructured object to our protobuf format
	protoAuthzPolicy, err := api.convertToProtoAuthorizationPolicy(obj, action)
	if err != nil {
		api.logger.Error("Failed to convert authorization policy to proto",
			zap.Error(err),
			zap.String("object_type", fmt.Sprintf("%T", obj)),
			zap.String("action", action))
		return
	}

	select {
	case api.authorizationPolicyChan <- protoAuthzPolicy:
		api.logger.Debug("Sent authorization policy event",
			zap.String("name", protoAuthzPolicy.Metadata.Name),
			zap.String("namespace", protoAuthzPolicy.Metadata.Namespace),
			zap.String("action", protoAuthzPolicy.Action.String()),
			zap.Int("target_workloads", len(protoAuthzPolicy.TargetWorkloads)))
	default:
		api.logger.Warn("Authorization policy channel full, dropping event",
			zap.String("name", protoAuthzPolicy.Metadata.Name),
			zap.String("namespace", protoAuthzPolicy.Metadata.Namespace),
			zap.String("action", protoAuthzPolicy.Action.String()))
	}
}

// convertToProtoAuthorizationPolicy converts an unstructured Kubernetes object to protobuf AuthorizationPolicy
func (api *AuthorizationPolicyIngester) convertToProtoAuthorizationPolicy(obj interface{}, action string) (*v1.AuthorizationPolicy, error) {
	// Check for nil object
	if obj == nil {
		return nil, fmt.Errorf("authorization policy object is nil")
	}

	// Handle different types of objects (unstructured.Unstructured or runtime.Object)
	var name, namespace, uid, apiVersion, kind string
	var labels, annotations map[string]string
	var spec map[string]interface{}

	switch o := obj.(type) {
	case *unstructured.Unstructured:
		// Handle unstructured.Unstructured object (most common from informers)
		name = o.GetName()
		namespace = o.GetNamespace()
		uid = string(o.GetUID())
		apiVersion = o.GetAPIVersion()
		kind = o.GetKind()
		labels = o.GetLabels()
		annotations = o.GetAnnotations()

		// Get spec from unstructured object
		spec, _ = o.Object["spec"].(map[string]interface{})

	case map[string]interface{}:
		// Handle raw map object
		metadata, ok := o["metadata"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("missing metadata in authorization policy object")
		}

		name, _ = metadata["name"].(string)
		namespace, _ = metadata["namespace"].(string)
		uid, _ = metadata["uid"].(string)
		apiVersion, _ = o["apiVersion"].(string)
		kind, _ = o["kind"].(string)

		if labelsInterface, ok := metadata["labels"].(map[string]interface{}); ok {
			labels = make(map[string]string)
			for k, v := range labelsInterface {
				if strVal, ok := v.(string); ok {
					labels[k] = strVal
				}
			}
		}

		if annotationsInterface, ok := metadata["annotations"].(map[string]interface{}); ok {
			annotations = make(map[string]string)
			for k, v := range annotationsInterface {
				if strVal, ok := v.(string); ok {
					annotations[k] = strVal
				}
			}
		}

		spec, _ = o["spec"].(map[string]interface{})

	default:
		// Log the object details for debugging
		api.logger.Error("Unsupported object type in authorization policy conversion",
			zap.String("object_type", fmt.Sprintf("%T", obj)),
			zap.String("object_value", fmt.Sprintf("%+v", obj)),
			zap.String("action", action))
		return nil, fmt.Errorf("unsupported object type for authorization policy conversion: %T", obj)
	}

	// Set defaults if missing
	if apiVersion == "" {
		apiVersion = "security.istio.io/v1beta1"
	}
	if kind == "" {
		kind = "AuthorizationPolicy"
	}

	// Resolve target workloads for this policy
	targetWorkloads := api.resolveTargetWorkloads(context.Background(), namespace, spec)

	// Convert spec to protobuf format
	protoSpec := api.convertAuthorizationPolicySpecToProto(spec)

	// Convert to protobuf format
	protoAuthzPolicy := &v1.AuthorizationPolicy{
		ApiVersion: apiVersion,
		Kind:       kind,
		Metadata: &v1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
			Uid:         uid,
		},
		Spec:            protoSpec,
		TargetWorkloads: targetWorkloads,
		Action:          stringToAction(action),
	}

	// Note: CreatedAt field is not available in this ObjectMeta, unlike Workload/Namespace
	// This is consistent with how NetworkPolicy ingestion works

	return protoAuthzPolicy, nil
}

// convertAuthorizationPolicySpecToProto converts an authorization policy spec to protobuf format
func (api *AuthorizationPolicyIngester) convertAuthorizationPolicySpecToProto(spec map[string]interface{}) *v1.AuthorizationPolicySpec {
	if spec == nil {
		return &v1.AuthorizationPolicySpec{}
	}

	protoSpec := &v1.AuthorizationPolicySpec{}

	// Convert selector
	if selectorInterface, ok := spec["selector"].(map[string]interface{}); ok {
		protoSpec.Selector = api.convertSelectorToProto(selectorInterface)
	}

	// Convert action
	if actionStr, ok := spec["action"].(string); ok {
		protoSpec.Action = api.convertActionToProto(actionStr)
	}

	// Convert rules
	if rulesInterface, ok := spec["rules"].([]interface{}); ok {
		protoSpec.Rules = api.convertRulesToProto(rulesInterface)
	}

	return protoSpec
}

// convertSelectorToProto converts a selector map to protobuf LabelSelector
func (api *AuthorizationPolicyIngester) convertSelectorToProto(selector map[string]interface{}) *v1.LabelSelector {
	protoSelector := &v1.LabelSelector{}

	if matchLabelsInterface, ok := selector["matchLabels"].(map[string]interface{}); ok {
		matchLabels := make(map[string]string)
		for k, v := range matchLabelsInterface {
			if strVal, ok := v.(string); ok {
				matchLabels[k] = strVal
			}
		}
		protoSelector.MatchLabels = matchLabels
	}

	// Note: matchExpressions conversion omitted for brevity but can be added if needed
	return protoSelector
}

// convertActionToProto converts action string to protobuf enum
func (api *AuthorizationPolicyIngester) convertActionToProto(action string) v1.AuthorizationPolicyAction {
	switch strings.ToUpper(action) {
	case "ALLOW":
		return v1.AuthorizationPolicyAction_AUTHORIZATION_POLICY_ACTION_ALLOW
	case "DENY":
		return v1.AuthorizationPolicyAction_AUTHORIZATION_POLICY_ACTION_DENY
	case "AUDIT":
		return v1.AuthorizationPolicyAction_AUTHORIZATION_POLICY_ACTION_AUDIT
	case "CUSTOM":
		return v1.AuthorizationPolicyAction_AUTHORIZATION_POLICY_ACTION_CUSTOM
	default:
		return v1.AuthorizationPolicyAction_AUTHORIZATION_POLICY_ACTION_UNSPECIFIED
	}
}

// convertRulesToProto converts rules array to protobuf format
func (api *AuthorizationPolicyIngester) convertRulesToProto(rules []interface{}) []*v1.AuthorizationPolicyRule {
	protoRules := make([]*v1.AuthorizationPolicyRule, 0, len(rules))

	for _, ruleInterface := range rules {
		if rule, ok := ruleInterface.(map[string]interface{}); ok {
			protoRule := &v1.AuthorizationPolicyRule{}

			// Convert 'from' field
			if fromInterface, ok := rule["from"].([]interface{}); ok {
				protoRule.From = api.convertFromToProto(fromInterface)
			}

			// Convert 'to' field
			if toInterface, ok := rule["to"].([]interface{}); ok {
				protoRule.To = api.convertToToProto(toInterface)
			}

			// Convert 'when' field
			if whenInterface, ok := rule["when"].([]interface{}); ok {
				protoRule.When = api.convertWhenToProto(whenInterface)
			}

			protoRules = append(protoRules, protoRule)
		}
	}

	return protoRules
}

// convertFromToProto converts 'from' rules to protobuf format
func (api *AuthorizationPolicyIngester) convertFromToProto(from []interface{}) []*v1.AuthorizationPolicyRuleFrom {
	protoFrom := make([]*v1.AuthorizationPolicyRuleFrom, 0, len(from))

	for _, fromInterface := range from {
		if fromRule, ok := fromInterface.(map[string]interface{}); ok {
			protoFromRule := &v1.AuthorizationPolicyRuleFrom{}

			if sourceInterface, ok := fromRule["source"].(map[string]interface{}); ok {
				protoFromRule.Source = api.convertSourceToProto(sourceInterface)
			}

			protoFrom = append(protoFrom, protoFromRule)
		}
	}

	return protoFrom
}

// convertToToProto converts 'to' rules to protobuf format
func (api *AuthorizationPolicyIngester) convertToToProto(to []interface{}) []*v1.AuthorizationPolicyRuleTo {
	protoTo := make([]*v1.AuthorizationPolicyRuleTo, 0, len(to))

	for _, toInterface := range to {
		if toRule, ok := toInterface.(map[string]interface{}); ok {
			protoToRule := &v1.AuthorizationPolicyRuleTo{}

			if operationInterface, ok := toRule["operation"].(map[string]interface{}); ok {
				protoToRule.Operation = api.convertOperationToProto(operationInterface)
			}

			protoTo = append(protoTo, protoToRule)
		}
	}

	return protoTo
}

// convertSourceToProto converts source to protobuf format
func (api *AuthorizationPolicyIngester) convertSourceToProto(source map[string]interface{}) *v1.AuthorizationPolicySource {
	protoSource := &v1.AuthorizationPolicySource{}

	if principals, ok := source["principals"].([]interface{}); ok {
		protoSource.Principals = api.convertStringArray(principals)
	}

	if requestPrincipals, ok := source["requestPrincipals"].([]interface{}); ok {
		protoSource.RequestPrincipals = api.convertStringArray(requestPrincipals)
	}

	if namespaces, ok := source["namespaces"].([]interface{}); ok {
		protoSource.Namespaces = api.convertStringArray(namespaces)
	}

	if ipBlocks, ok := source["ipBlocks"].([]interface{}); ok {
		protoSource.IpBlocks = api.convertStringArray(ipBlocks)
	}

	if remoteIpBlocks, ok := source["remoteIpBlocks"].([]interface{}); ok {
		protoSource.RemoteIpBlocks = api.convertStringArray(remoteIpBlocks)
	}

	return protoSource
}

// convertOperationToProto converts operation to protobuf format
func (api *AuthorizationPolicyIngester) convertOperationToProto(operation map[string]interface{}) *v1.AuthorizationPolicyOperation {
	protoOperation := &v1.AuthorizationPolicyOperation{}

	if methods, ok := operation["methods"].([]interface{}); ok {
		protoOperation.Methods = api.convertStringArray(methods)
	}

	if paths, ok := operation["paths"].([]interface{}); ok {
		protoOperation.Paths = api.convertStringArray(paths)
	}

	if ports, ok := operation["ports"].([]interface{}); ok {
		protoOperation.Ports = api.convertStringArray(ports)
	}

	if hosts, ok := operation["hosts"].([]interface{}); ok {
		protoOperation.Hosts = api.convertStringArray(hosts)
	}

	return protoOperation
}

// convertWhenToProto converts 'when' conditions to protobuf format
func (api *AuthorizationPolicyIngester) convertWhenToProto(when []interface{}) []*v1.AuthorizationPolicyCondition {
	protoConditions := make([]*v1.AuthorizationPolicyCondition, 0, len(when))

	for _, whenInterface := range when {
		if condition, ok := whenInterface.(map[string]interface{}); ok {
			protoCondition := &v1.AuthorizationPolicyCondition{}

			if key, ok := condition["key"].(string); ok {
				protoCondition.Key = key
			}

			if values, ok := condition["values"].([]interface{}); ok {
				protoCondition.Values = api.convertStringArray(values)
			}

			if notValues, ok := condition["notValues"].([]interface{}); ok {
				protoCondition.NotValues = api.convertStringArray(notValues)
			}

			protoConditions = append(protoConditions, protoCondition)
		}
	}

	return protoConditions
}

// convertStringArray converts []interface{} to []string
func (api *AuthorizationPolicyIngester) convertStringArray(arr []interface{}) []string {
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if str, ok := item.(string); ok {
			result = append(result, str)
		}
	}
	return result
}

// resolveTargetWorkloads resolves an authorization policy to the target workloads to which it applies
func (api *AuthorizationPolicyIngester) resolveTargetWorkloads(ctx context.Context, namespace string, spec map[string]interface{}) []string {
	if spec == nil {
		return []string{}
	}

	// Get selector from spec
	selectorInterface, ok := spec["selector"].(map[string]interface{})
	if !ok {
		// If no selector, policy applies to all workloads in the namespace
		return api.getAllWorkloadsInNamespace(ctx, namespace)
	}

	matchLabelsInterface, ok := selectorInterface["matchLabels"].(map[string]interface{})
	if !ok {
		return []string{}
	}

	// Convert to label selector
	matchLabels := make(map[string]string)
	for k, v := range matchLabelsInterface {
		if strVal, ok := v.(string); ok {
			matchLabels[k] = strVal
		}
	}

	// Find pods matching the selector
	podList, err := api.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(matchLabels).String(),
	})
	if err != nil {
		api.logger.Error("Failed to list pods for authorization policy target resolution", zap.Error(err))
		return []string{}
	}

	targetWorkloads := sets.New[string]()
	for _, p := range podList.Items {
		for _, ref := range p.OwnerReferences {
			switch ref.Kind {
			case "Deployment", "StatefulSet", "DaemonSet", "CronJob":
				targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
			case "ReplicaSet":
				if d := deploymentName(ref.Name); d != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, "Deployment", d))
				} else {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
				}
			case "Job":
				if cj := cronJobName(ref.Name); cj != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, "CronJob", cj))
				} else {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
				}
			}
		}
	}

	return targetWorkloads.UnsortedList()
}

// getAllWorkloadsInNamespace gets all workloads in a namespace (when no selector is specified)
func (api *AuthorizationPolicyIngester) getAllWorkloadsInNamespace(ctx context.Context, namespace string) []string {
	targetWorkloads := sets.New[string]()

	// Get all pods in the namespace
	podList, err := api.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		api.logger.Error("Failed to list all pods in namespace for authorization policy", zap.String("namespace", namespace), zap.Error(err))
		return []string{}
	}

	for _, p := range podList.Items {
		for _, ref := range p.OwnerReferences {
			switch ref.Kind {
			case "Deployment", "StatefulSet", "DaemonSet", "CronJob":
				targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
			case "ReplicaSet":
				if d := deploymentName(ref.Name); d != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, "Deployment", d))
				} else {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
				}
			case "Job":
				if cj := cronJobName(ref.Name); cj != "" {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, "CronJob", cj))
				} else {
					targetWorkloads.Insert(fmt.Sprintf("%s.%s.%s", namespace, ref.Kind, ref.Name))
				}
			}
		}
	}

	return targetWorkloads.UnsortedList()
}

// CheckAuthorizationPolicy validates an authorization policy YAML string
func CheckAuthorizationPolicy(logger *zap.Logger, policyYAML string) error {
	// Check if the policy is empty
	if policyYAML == "" {
		return fmt.Errorf("authorization policy is empty")
	}

	// Parse the YAML into an AuthorizationPolicy object
	authzPolicy, err := ParseAuthorizationPolicyYAML(logger, policyYAML)
	if err != nil {
		return fmt.Errorf("failed to parse authorization policy: %v", err)
	}

	// Get a dynamic client for the dry run
	restConfig, err := k8s_helper.NewRestConfig()
	if err != nil {
		return fmt.Errorf("failed to create rest config: %v", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	// Perform a dry run create
	_, err = dynamicClient.Resource(AuthorizationPolicyResource).
		Namespace(authzPolicy.GetNamespace()).
		Create(context.Background(), authzPolicy, metav1.CreateOptions{
			DryRun: []string{"All"},
		})

	if err != nil {
		return fmt.Errorf("dry run failed: %v", err)
	}

	return nil
}

// ParseAuthorizationPolicyYAML parses a YAML string into an unstructured Kubernetes object
func ParseAuthorizationPolicyYAML(logger *zap.Logger, yamlStr string) (*unstructured.Unstructured, error) {
	// Check for empty input
	if yamlStr == "" {
		return nil, fmt.Errorf("empty authorization policy YAML")
	}

	// Convert YAML to JSON (Kubernetes API machinery works with JSON)
	jsonBytes, err := yaml.ToJSON([]byte(yamlStr))
	if err != nil {
		return nil, fmt.Errorf("failed to convert YAML to JSON: %v", err)
	}

	// Create unstructured object
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(jsonBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal authorization policy: %v", err)
	}

	return obj, nil
}

// ApplyAuthorizationPolicy applies an authorization policy received from the server to the cluster
func ApplyAuthorizationPolicy(ctx context.Context, dynamicClient dynamic.Interface, policyYAML string) error {
	// Check for nil client
	if dynamicClient == nil {
		return fmt.Errorf("dynamic client cannot be nil")
	}

	// Parse the YAML
	jsonBytes, err := yaml.ToJSON([]byte(policyYAML))
	if err != nil {
		return fmt.Errorf("failed to convert YAML to JSON: %v", err)
	}

	// Create unstructured object
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(jsonBytes); err != nil {
		return fmt.Errorf("failed to unmarshal authorization policy: %v", err)
	}

	namespace := obj.GetNamespace()
	name := obj.GetName()

	// Apply or update the policy
	_, err = dynamicClient.Resource(AuthorizationPolicyResource).
		Namespace(namespace).
		Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			current, getErr := dynamicClient.Resource(AuthorizationPolicyResource).
				Namespace(namespace).
				Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("failed to get existing authorization policy: %v", getErr)
			}

			// Set resource version for update
			obj.SetResourceVersion(current.GetResourceVersion())

			_, err = dynamicClient.Resource(AuthorizationPolicyResource).
				Namespace(namespace).
				Update(ctx, obj, metav1.UpdateOptions{})

			return err
		}
		return err
	}

	return nil
}
