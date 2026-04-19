package datadog_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// DatadogExecutor discovers in-cluster Datadog installations and proxies
// queries to the Datadog cloud API using the customer's own API key.
//
// RBAC requirements for the operator's ServiceAccount:
//   - secrets: get (in the Datadog namespace, e.g. "datadog")
//   - daemonsets: list (cluster-wide or in the Datadog namespace)
//   - deployments: list (in the Datadog namespace)
type DatadogExecutor struct {
	Logger    *zap.Logger
	ClientSet *kubernetes.Clientset

	OverrideNamespace  string // DD_NAMESPACE
	OverrideSecretName string // DD_SECRET_NAME
	OverrideAPIKey     string // DD_API_KEY (direct override)
	OverrideAppKey     string // DD_APP_KEY (direct override)
	OverrideSite       string // DD_SITE (e.g. "datadoghq.eu")

	mu         sync.RWMutex
	discovered bool
	apiKey     string
	appKey     string
	site       string
	namespace  string // resolved Datadog namespace (for logging)
	httpClient *http.Client
}

func NewDatadogExecutor(
	logger *zap.Logger,
	clientSet *kubernetes.Clientset,
	overrideNamespace string,
	overrideSecretName string,
	overrideAPIKey string,
	overrideAppKey string,
	overrideSite string,
) *DatadogExecutor {
	return &DatadogExecutor{
		Logger:             logger,
		ClientSet:          clientSet,
		OverrideNamespace:  overrideNamespace,
		OverrideSecretName: overrideSecretName,
		OverrideAPIKey:     overrideAPIKey,
		OverrideAppKey:     overrideAppKey,
		OverrideSite:       overrideSite,
		httpClient: &http.Client{},
	}
}

// All Datadog API query endpoints require both DD-API-KEY and DD-APPLICATION-KEY.

func (e *DatadogExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Datadog query",
		zap.String("request_id", req.RequestId),
		zap.String("query_type", req.QueryType.String()),
		zap.String("query", truncate(req.Query, 200)))

	resp := &v1.DatadogQueryResponse{RequestId: req.RequestId}

	if err := e.ensureDiscovered(ctx); err != nil {
		e.Logger.Error("Datadog discovery failed for query execution",
			zap.String("request_id", req.RequestId),
			zap.Error(err))
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("Datadog discovery failed: %v", err)
		resp.StatusCode = http.StatusServiceUnavailable
		return resp
	}

	e.mu.RLock()
	hasAppKey := e.appKey != ""
	e.mu.RUnlock()
	if !hasAppKey {
		e.Logger.Warn("Datadog query requires app key but none available",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
		resp.Success = false
		resp.ErrorMessage = "All Datadog API queries require an Application Key (DD-APPLICATION-KEY), " +
			"but no app-key was found in the Datadog secret. " +
			"Add an app-key to the Datadog Helm values or set DD_APP_KEY on the operator."
		resp.StatusCode = http.StatusForbidden
		return resp
	}

	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var result *v1.DatadogQueryResponse
	switch req.QueryType {
	case v1.DatadogQueryType_DATADOG_QUERY_METRICS:
		result = e.queryMetrics(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_QUERY_EVENTS:
		result = e.queryEvents(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_QUERY_HOSTS:
		result = e.queryHosts(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_QUERY_LOGS:
		result = e.queryLogs(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_QUERY_LIST_METRICS:
		result = e.queryListMetrics(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_CREATE_MONITOR:
		result = e.createMonitor(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_SEND_EVENT:
		result = e.sendEvent(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_MUTE_MONITOR:
		result = e.muteMonitor(ctxTimeout, req)
	case v1.DatadogQueryType_DATADOG_LIST_MONITORS:
		result = e.listMonitors(ctxTimeout, req)
	default:
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("unsupported query type: %s", req.QueryType.String())
		resp.StatusCode = http.StatusBadRequest
		return resp
	}

	if result.Success {
		e.Logger.Info("Datadog query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.Int("response_bytes", len(result.ResponseData)))
	} else {
		e.Logger.Error("Datadog query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.Int32("status_code", result.StatusCode),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

func (e *DatadogExecutor) ensureDiscovered(ctx context.Context) error {
	e.mu.RLock()
	if e.discovered {
		e.mu.RUnlock()
		return nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.discovered {
		return nil
	}

	if e.OverrideAPIKey != "" {
		e.apiKey = e.OverrideAPIKey
		e.appKey = e.OverrideAppKey
		e.site = e.OverrideSite
		if e.site == "" {
			e.site = "datadoghq.com"
		}
		e.namespace = "(override)"
		e.discovered = true
		e.Logger.Info("[Datadog] Using config overrides (DD_API_KEY set)",
			zap.String("site", e.site),
			zap.Bool("has_app_key", e.appKey != ""))
		return nil
	}

	e.Logger.Info("[Datadog] Starting auto-discovery of Datadog installation in the cluster")
	if err := e.discover(ctx); err != nil {
		return err
	}
	e.discovered = true
	return nil
}

func (e *DatadogExecutor) discover(ctx context.Context) error {
	namespace := e.OverrideNamespace
	secretName := e.OverrideSecretName

	if namespace == "" {
		e.Logger.Info("[Datadog] No namespace override set, scanning cluster for Datadog workloads")
		ns, err := e.findDatadogNamespace(ctx)
		if err != nil {
			return fmt.Errorf("failed to find Datadog namespace: %w", err)
		}
		namespace = ns
	} else {
		e.Logger.Info("[Datadog] Using namespace override", zap.String("namespace", namespace))
	}

	// --- API key ---
	// Try multiple known secret names: Helm chart uses "datadog", Datadog
	// Operator uses "datadog-secret". The override takes priority.
	secretCandidates := []string{"datadog", "datadog-secret"}
	if secretName != "" {
		secretCandidates = []string{secretName}
	}

	var apiKeyBytes []byte
	for _, candidate := range secretCandidates {
		e.Logger.Info("[Datadog] Trying secret for API key",
			zap.String("namespace", namespace),
			zap.String("secret", candidate))

		secret, err := e.ClientSet.CoreV1().Secrets(namespace).Get(ctx, candidate, metav1.GetOptions{})
		if err != nil {
			e.Logger.Debug("[Datadog] Secret not found or inaccessible",
				zap.String("secret", candidate),
				zap.Error(err))
			continue
		}

		if keyBytes, ok := secret.Data["api-key"]; ok && len(strings.TrimSpace(string(keyBytes))) > 0 {
			apiKeyBytes = keyBytes
			secretName = candidate
			e.Logger.Info("[Datadog] API key found in secret",
				zap.String("secret", candidate),
				zap.Int("key_length", len(strings.TrimSpace(string(keyBytes)))))

			// Also check for app-key in this same secret
			if appKeyBytes, ok := secret.Data["app-key"]; ok && len(strings.TrimSpace(string(appKeyBytes))) > 0 {
				e.appKey = strings.TrimSpace(string(appKeyBytes))
				e.Logger.Info("[Datadog] Application key also found in same secret",
					zap.String("secret", candidate))
			}
			break
		} else {
			availableKeys := make([]string, 0, len(secret.Data))
			for k := range secret.Data {
				availableKeys = append(availableKeys, k)
			}
			e.Logger.Debug("[Datadog] Secret exists but has no 'api-key' field",
				zap.String("secret", candidate),
				zap.Strings("available_keys", availableKeys))
		}
	}

	if apiKeyBytes == nil {
		return fmt.Errorf("no Datadog API key found in namespace %s (tried secrets: %v)", namespace, secretCandidates)
	}
	e.apiKey = strings.TrimSpace(string(apiKeyBytes))

	// If app-key wasn't found in the primary secret (handled above in the loop),
	// try the cluster-agent secret as a fallback.
	if e.appKey == "" {
		e.Logger.Info("[Datadog] No app-key in primary secret, checking cluster-agent secret")
		clusterAgentSecret, err := e.ClientSet.CoreV1().Secrets(namespace).Get(ctx, "datadog-cluster-agent", metav1.GetOptions{})
		if err != nil {
			e.Logger.Debug("[Datadog] Could not read datadog-cluster-agent secret (may not exist)",
				zap.Error(err))
		} else if appKeyBytes, ok := clusterAgentSecret.Data["app-key"]; ok && len(strings.TrimSpace(string(appKeyBytes))) > 0 {
			e.appKey = strings.TrimSpace(string(appKeyBytes))
			e.Logger.Info("[Datadog] Application key found in cluster-agent secret")
		} else {
			e.Logger.Info("[Datadog] No application key found in any secret — events/hosts/logs queries will not be available, only metrics")
		}
	}

	if e.appKey == "" && e.OverrideAppKey != "" {
		e.appKey = e.OverrideAppKey
		e.Logger.Info("[Datadog] Using application key from DD_APP_KEY override")
	}

	// --- DD_SITE ---
	site := e.OverrideSite
	if site == "" {
		e.Logger.Info("[Datadog] Discovering DD_SITE from cluster-agent deployment env vars",
			zap.String("namespace", namespace))
		site = e.discoverSite(ctx, namespace)
	}
	if site == "" {
		site = "datadoghq.com"
		e.Logger.Info("[Datadog] DD_SITE not found, using default", zap.String("site", site))
	} else {
		e.Logger.Info("[Datadog] DD_SITE resolved", zap.String("site", site))
	}
	e.site = site
	e.namespace = namespace

	e.Logger.Info("[Datadog] Discovery complete",
		zap.String("namespace", namespace),
		zap.String("secret", secretName),
		zap.String("site", e.site),
		zap.Bool("has_api_key", e.apiKey != ""),
		zap.Bool("has_app_key", e.appKey != ""),
		zap.String("api_url", e.apiURL("")))
	return nil
}

// datadogDaemonSetSelectors are label selectors that identify a Datadog Agent
// DaemonSet across both deployment patterns:
//   - Helm chart: app.kubernetes.io/name=datadog
//   - Datadog Operator: agent.datadoghq.com/component=agent
var datadogDaemonSetSelectors = []string{
	"app.kubernetes.io/name=datadog",
	"agent.datadoghq.com/component=agent",
}

// datadogClusterAgentSelectors identify the Cluster Agent Deployment:
//   - Helm chart: app.kubernetes.io/component=cluster-agent,app.kubernetes.io/name=datadog
//   - Datadog Operator: agent.datadoghq.com/component=cluster-agent
var datadogClusterAgentSelectors = []string{
	"app.kubernetes.io/component=cluster-agent,app.kubernetes.io/name=datadog",
	"agent.datadoghq.com/component=cluster-agent",
}

// findDatadogNamespace finds the namespace where Datadog is deployed by
// searching for the Datadog Agent DaemonSet, Cluster Agent Deployment,
// or Datadog Operator Deployment across the cluster.
//
// Supports both Helm chart and Datadog Operator deployment patterns.
//
// Strategy: try common namespaces first (cheap targeted calls) then fall
// back to cluster-wide searches if none match.
func (e *DatadogExecutor) findDatadogNamespace(ctx context.Context) (string, error) {
	commonNamespaces := []string{"datadog", "datadog-agent", "monitoring", "observability", "default"}

	// Fast path: check common namespaces for any Datadog DaemonSet label variant
	for _, ns := range commonNamespaces {
		for _, selector := range datadogDaemonSetSelectors {
			dsList, err := e.ClientSet.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{
				LabelSelector: selector,
			})
			if err != nil {
				continue
			}
			if len(dsList.Items) > 0 {
				e.Logger.Info("[Datadog] Found Datadog Agent DaemonSet in common namespace",
					zap.String("namespace", ns),
					zap.String("daemonset", dsList.Items[0].Name),
					zap.String("matched_selector", selector))
				return ns, nil
			}
		}
	}

	e.Logger.Info("[Datadog] Datadog not found in common namespaces, searching all namespaces")

	// Cluster-wide search: DaemonSet (both Helm and Operator label patterns)
	for _, selector := range datadogDaemonSetSelectors {
		dsList, err := e.ClientSet.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil {
			e.Logger.Warn("[Datadog] Cluster-wide DaemonSet search failed",
				zap.String("selector", selector), zap.Error(err))
			continue
		}
		if len(dsList.Items) > 0 {
			ns := dsList.Items[0].Namespace
			e.Logger.Info("[Datadog] Found Datadog Agent DaemonSet via cluster-wide search",
				zap.String("namespace", ns),
				zap.String("daemonset", dsList.Items[0].Name),
				zap.String("matched_selector", selector))
			return ns, nil
		}
	}

	// Cluster-wide search: Cluster Agent Deployment (both label patterns)
	for _, selector := range datadogClusterAgentSelectors {
		deployList, err := e.ClientSet.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil {
			e.Logger.Warn("[Datadog] Cluster-wide Cluster Agent search failed",
				zap.String("selector", selector), zap.Error(err))
			continue
		}
		if len(deployList.Items) > 0 {
			ns := deployList.Items[0].Namespace
			e.Logger.Info("[Datadog] Found Datadog Cluster Agent via cluster-wide search",
				zap.String("namespace", ns),
				zap.String("deployment", deployList.Items[0].Name),
				zap.String("matched_selector", selector))
			return ns, nil
		}
	}

	// Cluster-wide search: Datadog Operator Deployment
	operatorList, err := e.ClientSet.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=datadog-operator",
	})
	if err == nil && len(operatorList.Items) > 0 {
		ns := operatorList.Items[0].Namespace
		e.Logger.Info("[Datadog] Found Datadog Operator Deployment via cluster-wide search",
			zap.String("namespace", ns),
			zap.String("deployment", operatorList.Items[0].Name))
		return ns, nil
	}
	if err != nil {
		e.Logger.Warn("[Datadog] Cluster-wide Datadog Operator search failed", zap.Error(err))
	}

	e.Logger.Error("[Datadog] No Datadog workloads found in the cluster after exhaustive search")
	return "", fmt.Errorf("no Datadog workloads found in the cluster (searched DaemonSets with selectors %v, "+
		"Cluster Agent with selectors %v, and Datadog Operator)", datadogDaemonSetSelectors, datadogClusterAgentSelectors)
}

func (e *DatadogExecutor) discoverSite(ctx context.Context, namespace string) string {
	// Try both Helm and Operator label patterns for the Cluster Agent
	for _, selector := range datadogClusterAgentSelectors {
		deployments, err := e.ClientSet.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil || len(deployments.Items) == 0 {
			continue
		}

		for _, container := range deployments.Items[0].Spec.Template.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "DD_SITE" {
					e.Logger.Debug("[Datadog] Found DD_SITE in cluster-agent container env",
						zap.String("deployment", deployments.Items[0].Name),
						zap.String("container", container.Name),
						zap.String("DD_SITE", env.Value))
					return env.Value
				}
			}
		}
	}

	// Also check the DaemonSet agent pods (some setups put DD_SITE there)
	for _, selector := range datadogDaemonSetSelectors {
		dsList, err := e.ClientSet.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil || len(dsList.Items) == 0 {
			continue
		}
		for _, container := range dsList.Items[0].Spec.Template.Spec.Containers {
			for _, env := range container.Env {
				if env.Name == "DD_SITE" {
					e.Logger.Debug("[Datadog] Found DD_SITE in agent DaemonSet container env",
						zap.String("daemonset", dsList.Items[0].Name),
						zap.String("container", container.Name),
						zap.String("DD_SITE", env.Value))
					return env.Value
				}
			}
		}
	}

	e.Logger.Debug("[Datadog] DD_SITE not found in any Datadog workload env vars",
		zap.String("namespace", namespace))
	return ""
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// credentials returns a consistent snapshot of the discovered credentials
// under the read lock, safe from concurrent ResetDiscovery calls.
func (e *DatadogExecutor) credentials() (site, apiKey, appKey string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.site, e.apiKey, e.appKey
}

func (e *DatadogExecutor) apiURL(path string) string {
	return fmt.Sprintf("https://api.%s%s", e.site, path)
}

func (e *DatadogExecutor) doGet(ctx context.Context, apiPath string, params url.Values, needsAppKey bool) ([]byte, int, error) {
	site, apiKey, appKey := e.credentials()
	fullURL := fmt.Sprintf("https://api.%s%s", site, apiPath)
	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}

	e.Logger.Debug("[Datadog] HTTP GET request",
		zap.String("url", truncate(fullURL, 300)),
		zap.Bool("with_app_key", needsAppKey && appKey != ""))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("DD-API-KEY", apiKey)
	if needsAppKey && appKey != "" {
		req.Header.Set("DD-APPLICATION-KEY", appKey)
	}
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := e.httpClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		e.Logger.Error("[Datadog] HTTP GET failed",
			zap.String("url", truncate(fullURL, 300)),
			zap.Duration("elapsed", elapsed),
			zap.Error(err))
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	e.Logger.Debug("[Datadog] HTTP GET response",
		zap.String("url", truncate(fullURL, 200)),
		zap.Int("status", resp.StatusCode),
		zap.Int("body_bytes", len(body)),
		zap.Duration("elapsed", elapsed))

	return body, resp.StatusCode, nil
}

func (e *DatadogExecutor) doPost(ctx context.Context, apiPath string, jsonBody []byte, needsAppKey bool) ([]byte, int, error) {
	site, apiKey, appKey := e.credentials()
	fullURL := fmt.Sprintf("https://api.%s%s", site, apiPath)

	e.Logger.Debug("[Datadog] HTTP POST request",
		zap.String("url", fullURL),
		zap.Int("body_bytes", len(jsonBody)),
		zap.Bool("with_app_key", needsAppKey && appKey != ""))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("DD-API-KEY", apiKey)
	if needsAppKey && appKey != "" {
		req.Header.Set("DD-APPLICATION-KEY", appKey)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := e.httpClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		e.Logger.Error("[Datadog] HTTP POST failed",
			zap.String("url", fullURL),
			zap.Duration("elapsed", elapsed),
			zap.Error(err))
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	e.Logger.Debug("[Datadog] HTTP POST response",
		zap.String("url", fullURL),
		zap.Int("status", resp.StatusCode),
		zap.Int("body_bytes", len(body)),
		zap.Duration("elapsed", elapsed))

	return body, resp.StatusCode, nil
}

func (e *DatadogExecutor) makeResponse(requestID string, body []byte, statusCode int, err error) *v1.DatadogQueryResponse {
	resp := &v1.DatadogQueryResponse{RequestId: requestID}
	if err != nil {
		resp.Success = false
		resp.ErrorMessage = err.Error()
		resp.StatusCode = int32(statusCode)
		return resp
	}
	if statusCode < 200 || statusCode >= 300 {
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("Datadog API returned HTTP %d: %s", statusCode, truncate(string(body), 500))
		resp.StatusCode = int32(statusCode)
		return resp
	}
	resp.Success = true
	resp.ResponseData = string(body)
	resp.StatusCode = int32(statusCode)
	return resp
}

// ---------------------------------------------------------------------------
// Query methods
// ---------------------------------------------------------------------------

func (e *DatadogExecutor) queryMetrics(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	if req.Query == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "query string is required for metrics queries",
			StatusCode:   http.StatusBadRequest,
		}
	}

	params := url.Values{}
	params.Set("query", req.Query)
	params.Set("from", strconv.FormatInt(req.FromTs, 10))
	params.Set("to", strconv.FormatInt(req.ToTs, 10))

	e.Logger.Info("[Datadog] Querying metrics",
		zap.String("request_id", req.RequestId),
		zap.String("query", truncate(req.Query, 200)),
		zap.Int64("from", req.FromTs),
		zap.Int64("to", req.ToTs))

	body, code, err := e.doGet(ctx, "/api/v1/query", params, true)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) queryEvents(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	params := url.Values{}
	params.Set("start", strconv.FormatInt(req.FromTs, 10))
	params.Set("end", strconv.FormatInt(req.ToTs, 10))
	if req.Filter != "" {
		params.Set("sources", req.Filter)
	}
	if req.MaxResults > 0 {
		params.Set("count", strconv.FormatInt(int64(req.MaxResults), 10))
	}

	e.Logger.Info("[Datadog] Querying events",
		zap.String("request_id", req.RequestId),
		zap.Int64("from", req.FromTs),
		zap.Int64("to", req.ToTs),
		zap.String("filter", req.Filter))

	body, code, err := e.doGet(ctx, "/api/v1/events", params, true)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) queryHosts(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	params := url.Values{}
	if req.Filter != "" {
		params.Set("filter", req.Filter)
	}
	if req.Sort != "" {
		params.Set("sort_field", req.Sort)
	}
	if req.MaxResults > 0 {
		params.Set("count", strconv.FormatInt(int64(req.MaxResults), 10))
	}

	e.Logger.Info("[Datadog] Querying hosts",
		zap.String("request_id", req.RequestId),
		zap.String("filter", req.Filter))

	body, code, err := e.doGet(ctx, "/api/v1/hosts", params, true)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) queryLogs(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	payload := map[string]interface{}{
		"filter": map[string]interface{}{
			"from":  time.Unix(req.FromTs, 0).UTC().Format(time.RFC3339),
			"to":    time.Unix(req.ToTs, 0).UTC().Format(time.RFC3339),
			"query": req.Query,
		},
	}
	if req.Sort != "" {
		payload["sort"] = req.Sort
	}
	if req.MaxResults > 0 {
		payload["page"] = map[string]interface{}{"limit": req.MaxResults}
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to marshal logs request: %v", err),
			StatusCode:   http.StatusInternalServerError,
		}
	}

	e.Logger.Info("[Datadog] Querying logs",
		zap.String("request_id", req.RequestId),
		zap.String("query", truncate(req.Query, 200)),
		zap.Int64("from", req.FromTs),
		zap.Int64("to", req.ToTs))

	body, code, errHTTP := e.doPost(ctx, "/api/v2/logs/events/search", jsonBody, true)
	return e.makeResponse(req.RequestId, body, code, errHTTP)
}

// queryListMetrics calls GET /api/v1/metrics to list available metric names.
func (e *DatadogExecutor) queryListMetrics(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	params := url.Values{}
	// from_ts is required — list metrics active since this timestamp
	from := req.FromTs
	if from == 0 {
		from = time.Now().Add(-1 * time.Hour).Unix()
	}
	params.Set("from", strconv.FormatInt(from, 10))
	if req.Filter != "" {
		params.Set("host", req.Filter)
	}

	e.Logger.Info("[Datadog] Listing available metrics",
		zap.String("request_id", req.RequestId),
		zap.Int64("from", from))

	body, code, err := e.doGet(ctx, "/api/v1/metrics", params, true)
	return e.makeResponse(req.RequestId, body, code, err)
}

// ---------------------------------------------------------------------------
// Write methods (create monitor, send event, mute monitor)
// ---------------------------------------------------------------------------

func (e *DatadogExecutor) createMonitor(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	if req.JsonBody == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "json_body is required for create-monitor",
			StatusCode:   http.StatusBadRequest,
		}
	}

	e.Logger.Info("[Datadog] Creating monitor",
		zap.String("request_id", req.RequestId),
		zap.Int("body_bytes", len(req.JsonBody)))

	body, code, err := e.doPost(ctx, "/api/v1/monitor", []byte(req.JsonBody), true)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) sendEvent(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	if req.JsonBody == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "json_body is required for send-event",
			StatusCode:   http.StatusBadRequest,
		}
	}

	e.Logger.Info("[Datadog] Sending event",
		zap.String("request_id", req.RequestId),
		zap.Int("body_bytes", len(req.JsonBody)))

	body, code, err := e.doPost(ctx, "/api/v1/events", []byte(req.JsonBody), false)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) muteMonitor(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	monitorID := req.Filter
	if monitorID == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "filter field must contain the monitor ID for mute-monitor",
			StatusCode:   http.StatusBadRequest,
		}
	}

	muteBody := req.JsonBody
	if muteBody == "" {
		muteBody = "{}"
	}

	apiPath := fmt.Sprintf("/api/v1/monitor/%s/mute", monitorID)
	e.Logger.Info("[Datadog] Muting monitor",
		zap.String("request_id", req.RequestId),
		zap.String("monitor_id", monitorID))

	body, code, err := e.doPost(ctx, apiPath, []byte(muteBody), true)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *DatadogExecutor) listMonitors(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	params := url.Values{}
	params.Set("page", "0")
	params.Set("per_page", "200")

	e.Logger.Info("[Datadog] Listing monitors",
		zap.String("request_id", req.RequestId))

	body, code, err := e.doGet(ctx, "/api/v1/monitor", params, true)
	return e.makeResponse(req.RequestId, body, code, err)
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

func (e *DatadogExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.discovered
}

func (e *DatadogExecutor) Probe(ctx context.Context) bool {
	if err := e.ensureDiscovered(ctx); err != nil {
		e.Logger.Info("[Datadog] Probe: Datadog not found in cluster (this is normal if Datadog is not deployed)",
			zap.Error(err))
		return false
	}
	e.mu.RLock()
	ns, site, appKey := e.namespace, e.site, e.appKey
	e.mu.RUnlock()
	e.Logger.Info("[Datadog] Probe: Datadog available",
		zap.String("namespace", ns),
		zap.String("site", site),
		zap.Bool("has_app_key", appKey != ""))
	return true
}

func (e *DatadogExecutor) ResetDiscovery() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.discovered = false
	e.apiKey = ""
	e.appKey = ""
	e.site = ""
	e.namespace = ""
	e.Logger.Info("[Datadog] Discovery state reset — will re-discover on next query")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
