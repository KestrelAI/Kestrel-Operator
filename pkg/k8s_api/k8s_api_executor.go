package k8s_api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// APIExecutor handles execution of Kubernetes API requests
type APIExecutor struct {
	Logger    *zap.Logger
	ClientSet *kubernetes.Clientset
	Config    *rest.Config
}

// NewAPIExecutor creates a new Kubernetes API executor
func NewAPIExecutor(logger *zap.Logger, clientset *kubernetes.Clientset, config *rest.Config) *APIExecutor {
	return &APIExecutor{
		Logger:    logger,
		ClientSet: clientset,
		Config:    config,
	}
}

// ExecuteAPIRequests processes a batch of Kubernetes API requests
func (e *APIExecutor) ExecuteAPIRequests(ctx context.Context, request *v1.KubernetesAPIRequest) *v1.KubernetesAPIResponse {
	e.Logger.Info("Executing Kubernetes API requests",
		zap.String("request_id", request.RequestId),
		zap.Int("api_paths_count", len(request.ApiPaths)),
		zap.Int32("timeout_seconds", request.TimeoutSeconds))

	// Set up timeout context
	timeout := 30 * time.Second // default timeout
	if request.TimeoutSeconds > 0 {
		timeout = time.Duration(request.TimeoutSeconds) * time.Second
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	response := &v1.KubernetesAPIResponse{
		RequestId: request.RequestId,
		Results:   make([]*v1.KubernetesAPIResult, 0, len(request.ApiPaths)),
	}

	// Execute each API request
	for _, apiPath := range request.ApiPaths {
		result := e.executeAPICall(ctxWithTimeout, apiPath)
		response.Results = append(response.Results, result)
	}

	return response
}

// executeAPICall performs a single Kubernetes API call
func (e *APIExecutor) executeAPICall(ctx context.Context, apiPath string) *v1.KubernetesAPIResult {
	e.Logger.Debug("Executing API call", zap.String("api_path", apiPath))

	result := &v1.KubernetesAPIResult{
		ApiPath: apiPath,
	}

	// Validate API path format
	if !e.isValidAPIPath(apiPath) {
		result.Success = false
		result.ErrorMessage = "Invalid API path format"
		result.StatusCode = http.StatusBadRequest
		return result
	}

	// Split path from query parameters (e.g., "api/v1/.../log?tailLines=50")
	cleanPath := apiPath
	var queryString string
	if qIdx := strings.Index(apiPath, "?"); qIdx >= 0 {
		cleanPath = apiPath[:qIdx]
		queryString = apiPath[qIdx+1:]
	}

	// Check for metadata-only flag (custom param, not forwarded to K8s API)
	metadataOnly := false
	if queryString != "" {
		var filteredPairs []string
		for _, pair := range strings.Split(queryString, "&") {
			if pair == "metadata=true" {
				metadataOnly = true
			} else {
				filteredPairs = append(filteredPairs, pair)
			}
		}
		queryString = strings.Join(filteredPairs, "&")
	}

	// Create REST client for the API call
	restClient := e.ClientSet.RESTClient()

	// Execute the GET request
	req := restClient.Get().AbsPath("/" + strings.TrimPrefix(cleanPath, "/"))

	// When metadata-only is requested, use PartialObjectMetadataList to return
	// only resource metadata (name, namespace, labels, etc.) without spec/status.
	if metadataOnly {
		req = req.SetHeader("Accept", "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1")
	}

	// Apply query parameters (e.g., tailLines, fieldSelector, labelSelector)
	if queryString != "" {
		for _, pair := range strings.Split(queryString, "&") {
			if k, v, ok := strings.Cut(pair, "="); ok && k != "" {
				req = req.Param(k, v)
			}
		}
	}

	// Execute with context
	resultBytes, err := req.DoRaw(ctx)
	if err != nil {
		result.Success = false
		result.ErrorMessage = fmt.Sprintf("API call failed: %v", err)
		result.StatusCode = http.StatusInternalServerError

		e.Logger.Error("Kubernetes API call failed",
			zap.String("api_path", apiPath),
			zap.Error(err))
		return result
	}

	// Validate that the result is valid JSON.
	// Log sub-resources return plain text, so skip JSON validation for those.
	isLogSubresource := strings.HasSuffix(cleanPath, "/log")
	if !isLogSubresource {
		var jsonValidation interface{}
		if err := json.Unmarshal(resultBytes, &jsonValidation); err != nil {
			result.Success = false
			result.ErrorMessage = fmt.Sprintf("Invalid JSON response: %v", err)
			result.StatusCode = http.StatusInternalServerError

			e.Logger.Error("Invalid JSON response from Kubernetes API",
				zap.String("api_path", apiPath),
				zap.Error(err))
			return result
		}
	}

	// Success
	result.Success = true
	result.ResponseData = string(resultBytes)
	result.StatusCode = http.StatusOK

	e.Logger.Debug("API call completed successfully",
		zap.String("api_path", apiPath),
		zap.Int("response_size", len(resultBytes)))

	return result
}

// isValidAPIPath performs basic validation on the API path
func (e *APIExecutor) isValidAPIPath(apiPath string) bool {
	// Strip query parameters before validating the path
	path := apiPath
	if qIdx := strings.Index(path, "?"); qIdx >= 0 {
		path = path[:qIdx]
	}
	// Remove leading slash if present
	path = strings.TrimPrefix(path, "/")

	// Must not be empty
	if path == "" {
		return false
	}

	// Must start with valid API prefixes
	validPrefixes := []string{
		"api/v1/",
		"apis/",
		"api/v1beta1/",
		"openapi/v2",
		"version",
	}

	for _, prefix := range validPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	// Allow some common single-word endpoints
	singleWordEndpoints := []string{
		"api",
		"apis",
		"version",
		"healthz",
		"livez",
		"readyz",
	}

	for _, endpoint := range singleWordEndpoints {
		if path == endpoint {
			return true
		}
	}

	return false
}
