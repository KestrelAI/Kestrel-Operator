package ingestion

import (
	"context"
	"fmt"
	"strings"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// YamlValidator handles dry-run validation of Kubernetes YAML manifests
type YamlValidator struct {
	clientset     *kubernetes.Clientset
	dynamicClient dynamic.Interface
	logger        *zap.Logger
}

// NewYamlValidator creates a new YAML validator
func NewYamlValidator(logger *zap.Logger) (*YamlValidator, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	dynamicClient, err := k8s_helper.NewDynamicClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &YamlValidator{
		clientset:     clientset,
		dynamicClient: dynamicClient,
		logger:        logger,
	}, nil
}

// ValidateYamlManifests validates multiple YAML manifests using dry-run
func (yv *YamlValidator) ValidateYamlManifests(ctx context.Context, request *v1.YamlDryRunRequest) *v1.YamlDryRunResponse {
	yv.logger.Info("Validating YAML manifests",
		zap.String("request_id", request.RequestId),
		zap.Int("manifests_count", len(request.YamlManifests)))

	response := &v1.YamlDryRunResponse{
		RequestId: request.RequestId,
		Results:   make([]*v1.YamlValidationResult, 0, len(request.YamlManifests)),
	}

	// Validate each manifest
	for _, manifest := range request.YamlManifests {
		result := yv.validateSingleManifest(ctx, manifest)
		response.Results = append(response.Results, result)
	}

	return response
}

// validateSingleManifest validates a single YAML manifest
func (yv *YamlValidator) validateSingleManifest(ctx context.Context, manifest *v1.YamlManifest) *v1.YamlValidationResult {
	result := &v1.YamlValidationResult{
		ManifestId: manifest.ManifestId,
	}

	yv.logger.Info("Validating YAML manifest",
		zap.String("manifest_id", manifest.ManifestId),
		zap.String("resource_type", manifest.ResourceType),
		zap.String("namespace", manifest.Namespace))

	// Parse the YAML into an unstructured object
	obj, err := yv.parseYamlToUnstructured(manifest.YamlContent)
	if err != nil {
		result.Valid = false
		result.ErrorMessage = fmt.Sprintf("Failed to parse YAML: %v", err)
		result.StatusCode = 400
		return result
	}

	// Set namespace if provided and not already set
	if manifest.Namespace != "" && obj.GetNamespace() == "" {
		obj.SetNamespace(manifest.Namespace)
	}

	// Perform dry-run validation using dynamic client
	err = yv.dryRunValidate(ctx, obj)
	if err != nil {
		result.Valid = false
		result.ErrorMessage = err.Error()

		// Extract status code from Kubernetes API error
		if statusErr, ok := err.(*apierrors.StatusError); ok {
			result.StatusCode = int32(statusErr.ErrStatus.Code)
		} else {
			result.StatusCode = 500
		}

		// Generate suggested fixes based on common errors
		result.SuggestedFix = yv.generateSuggestedFix(err, obj)
		return result
	}

	result.Valid = true
	result.StatusCode = 200
	yv.logger.Info("YAML manifest validation succeeded",
		zap.String("manifest_id", manifest.ManifestId),
		zap.String("kind", obj.GetKind()),
		zap.String("name", obj.GetName()),
		zap.String("namespace", obj.GetNamespace()))

	return result
}

// parseYamlToUnstructured parses YAML content into an unstructured object
func (yv *YamlValidator) parseYamlToUnstructured(yamlContent string) (*unstructured.Unstructured, error) {
	// Convert YAML to JSON
	jsonBytes, err := yaml.ToJSON([]byte(yamlContent))
	if err != nil {
		return nil, fmt.Errorf("failed to convert YAML to JSON: %v", err)
	}

	// Parse into unstructured object
	obj := &unstructured.Unstructured{}
	err = obj.UnmarshalJSON(jsonBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return obj, nil
}

// dryRunValidate performs dry-run validation using the dynamic client
func (yv *YamlValidator) dryRunValidate(ctx context.Context, obj *unstructured.Unstructured) error {
	// Get the GVR (Group, Version, Resource) for this object
	gvk := obj.GroupVersionKind()

	// Map common kinds to their resource names
	resourceName := strings.ToLower(gvk.Kind) + "s"

	// Handle special cases
	switch gvk.Kind {
	case "NetworkPolicy":
		resourceName = "networkpolicies"
	case "Endpoints":
		resourceName = "endpoints"
	case "Ingress":
		resourceName = "ingresses"
	}

	// Create GVR
	gvr := gvk.GroupVersion().WithResource(resourceName)

	// Get the appropriate client
	var resourceClient dynamic.ResourceInterface
	if obj.GetNamespace() != "" {
		// Namespaced resource
		resourceClient = yv.dynamicClient.Resource(gvr).Namespace(obj.GetNamespace())
	} else {
		// Cluster-scoped resource
		resourceClient = yv.dynamicClient.Resource(gvr)
	}

	// Perform dry-run create
	_, err := resourceClient.Create(ctx, obj, metav1.CreateOptions{
		DryRun: []string{"All"},
	})

	return err
}

// generateSuggestedFix generates suggested fixes based on common validation errors
func (yv *YamlValidator) generateSuggestedFix(err error, obj *unstructured.Unstructured) string {
	errorMsg := err.Error()

	// Common error patterns and their fixes
	if strings.Contains(errorMsg, "required value") {
		return "Add missing required fields to the resource specification"
	}

	if strings.Contains(errorMsg, "invalid value") {
		return "Check field values for correct format and allowed values"
	}

	if strings.Contains(errorMsg, "already exists") {
		return "Resource already exists - consider using 'update' file_type instead of 'create'"
	}

	if strings.Contains(errorMsg, "not found") {
		return "Referenced resource or namespace does not exist"
	}

	if strings.Contains(errorMsg, "forbidden") {
		return "Insufficient permissions to create this resource type"
	}

	if strings.Contains(errorMsg, "invalid") && obj.GetKind() == "NetworkPolicy" {
		return "Check NetworkPolicy selector syntax and rule format"
	}

	if strings.Contains(errorMsg, "invalid") && obj.GetKind() == "Role" {
		return "Check RBAC rules format - verify apiGroups, resources, and verbs"
	}

	if strings.Contains(errorMsg, "invalid") && obj.GetKind() == "Deployment" {
		return "Check container specifications, image names, and resource requirements"
	}

	// Default suggestion
	return "Review the YAML syntax and ensure all required fields are present with valid values"
}
