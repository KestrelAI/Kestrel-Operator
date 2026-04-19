package argocd_executor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ArgoCDExecutor discovers in-cluster ArgoCD installations and proxies
// queries to the ArgoCD API using the admin password from the cluster secret.
//
// RBAC requirements for the operator's ServiceAccount:
//   - secrets: get (in the ArgoCD namespace, for argocd-initial-admin-secret)
//   - services: list (to discover argocd-server)
type ArgoCDExecutor struct {
	Logger    *zap.Logger
	ClientSet *kubernetes.Clientset

	OverrideNamespace string

	mu         sync.RWMutex
	discovered bool
	namespace  string
	baseURL    string
	authToken  string
	tokenExp   time.Time
	httpClient *http.Client
}

func NewArgoCDExecutor(
	logger *zap.Logger,
	clientSet *kubernetes.Clientset,
	overrideNamespace string,
) *ArgoCDExecutor {
	return &ArgoCDExecutor{
		Logger:            logger,
		ClientSet:         clientSet,
		OverrideNamespace: overrideNamespace,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

func (e *ArgoCDExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing ArgoCD query",
		zap.String("request_id", req.RequestId),
		zap.String("query_type", req.QueryType.String()),
		zap.String("filter", req.Filter))

	resp := &v1.DatadogQueryResponse{RequestId: req.RequestId}

	if err := e.ensureDiscovered(ctx); err != nil {
		e.Logger.Error("ArgoCD discovery failed",
			zap.String("request_id", req.RequestId),
			zap.Error(err))
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("ArgoCD discovery failed: %v", err)
		resp.StatusCode = http.StatusServiceUnavailable
		return resp
	}

	if err := e.ensureAuthenticated(ctx); err != nil {
		e.Logger.Error("ArgoCD authentication failed",
			zap.String("request_id", req.RequestId),
			zap.Error(err))
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("ArgoCD authentication failed: %v", err)
		resp.StatusCode = http.StatusUnauthorized
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
	case v1.DatadogQueryType_ARGOCD_LIST_APPS:
		result = e.listApps(ctxTimeout, req)
	case v1.DatadogQueryType_ARGOCD_GET_APP_STATUS:
		result = e.getAppStatus(ctxTimeout, req)
	case v1.DatadogQueryType_ARGOCD_SYNC_APP:
		result = e.syncApp(ctxTimeout, req)
	default:
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("unsupported ArgoCD query type: %s", req.QueryType.String())
		resp.StatusCode = http.StatusBadRequest
		return resp
	}

	if result.Success {
		e.Logger.Info("ArgoCD query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.Int("response_bytes", len(result.ResponseData)))
	} else {
		e.Logger.Error("ArgoCD query failed",
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

func (e *ArgoCDExecutor) ensureDiscovered(ctx context.Context) error {
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

	if err := e.discover(ctx); err != nil {
		return err
	}
	e.discovered = true
	return nil
}

var argoCDCommonNamespaces = []string{"argocd", "argo-cd", "argo", "gitops", "default"}

func (e *ArgoCDExecutor) discover(ctx context.Context) error {
	namespace := e.OverrideNamespace

	if namespace == "" {
		e.Logger.Info("[ArgoCD] No namespace override set, scanning for argocd-server service")
		ns, err := e.findArgoCDNamespace(ctx)
		if err != nil {
			return fmt.Errorf("failed to find ArgoCD namespace: %w", err)
		}
		namespace = ns
	} else {
		e.Logger.Info("[ArgoCD] Using namespace override", zap.String("namespace", namespace))
	}

	e.namespace = namespace
	e.baseURL = fmt.Sprintf("https://argocd-server.%s.svc.cluster.local", namespace)

	e.Logger.Info("[ArgoCD] Discovery complete",
		zap.String("namespace", namespace),
		zap.String("base_url", e.baseURL))
	return nil
}

func (e *ArgoCDExecutor) findArgoCDNamespace(ctx context.Context) (string, error) {
	for _, ns := range argoCDCommonNamespaces {
		svc, err := e.ClientSet.CoreV1().Services(ns).Get(ctx, "argocd-server", metav1.GetOptions{})
		if err != nil {
			continue
		}
		if svc != nil {
			e.Logger.Info("[ArgoCD] Found argocd-server service",
				zap.String("namespace", ns))
			return ns, nil
		}
	}

	e.Logger.Info("[ArgoCD] Not found in common namespaces, searching all namespaces")
	svcList, err := e.ClientSet.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list services cluster-wide: %w", err)
	}
	for _, svc := range svcList.Items {
		if svc.Name == "argocd-server" {
			e.Logger.Info("[ArgoCD] Found argocd-server service via cluster-wide search",
				zap.String("namespace", svc.Namespace))
			return svc.Namespace, nil
		}
	}

	return "", fmt.Errorf("argocd-server service not found in any namespace")
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

func (e *ArgoCDExecutor) ensureAuthenticated(ctx context.Context) error {
	e.mu.RLock()
	if e.authToken != "" && time.Now().Before(e.tokenExp) {
		e.mu.RUnlock()
		return nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.authToken != "" && time.Now().Before(e.tokenExp) {
		return nil
	}

	password, err := e.getAdminPassword(ctx)
	if err != nil {
		return fmt.Errorf("failed to get admin password: %w", err)
	}

	token, err := e.login(ctx, password)
	if err != nil {
		return fmt.Errorf("failed to login to ArgoCD: %w", err)
	}

	e.authToken = token
	e.tokenExp = time.Now().Add(20 * time.Minute)
	return nil
}

func (e *ArgoCDExecutor) getAdminPassword(ctx context.Context) (string, error) {
	secret, err := e.ClientSet.CoreV1().Secrets(e.namespace).Get(ctx, "argocd-initial-admin-secret", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get argocd-initial-admin-secret in namespace %s: %w", e.namespace, err)
	}

	password, ok := secret.Data["password"]
	if !ok || len(password) == 0 {
		return "", fmt.Errorf("argocd-initial-admin-secret has no 'password' field")
	}

	return strings.TrimSpace(string(password)), nil
}

func (e *ArgoCDExecutor) login(ctx context.Context, password string) (string, error) {
	payload := map[string]string{
		"username": "admin",
		"password": password,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	url := e.baseURL + "/api/v1/session"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST /api/v1/session failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read login response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ArgoCD login returned HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("failed to parse login response: %w", err)
	}
	if result.Token == "" {
		return "", fmt.Errorf("ArgoCD login response missing token")
	}

	e.Logger.Info("[ArgoCD] Successfully authenticated with ArgoCD")
	return result.Token, nil
}

// ---------------------------------------------------------------------------
// API operations
// ---------------------------------------------------------------------------

func (e *ArgoCDExecutor) listApps(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("[ArgoCD] Listing applications",
		zap.String("request_id", req.RequestId))

	body, code, err := e.doGet(ctx, "/api/v1/applications")
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *ArgoCDExecutor) getAppStatus(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	appName := req.Filter
	if appName == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "filter field must contain the application name for get-app-status",
			StatusCode:   http.StatusBadRequest,
		}
	}

	e.Logger.Info("[ArgoCD] Getting application status",
		zap.String("request_id", req.RequestId),
		zap.String("app_name", appName))

	path := fmt.Sprintf("/api/v1/applications/%s", appName)
	body, code, err := e.doGet(ctx, path)
	return e.makeResponse(req.RequestId, body, code, err)
}

func (e *ArgoCDExecutor) syncApp(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	appName := req.Filter
	if appName == "" {
		return &v1.DatadogQueryResponse{
			RequestId:    req.RequestId,
			Success:      false,
			ErrorMessage: "filter field must contain the application name for sync-app",
			StatusCode:   http.StatusBadRequest,
		}
	}

	syncBody := req.JsonBody
	if syncBody == "" {
		syncBody = "{}"
	}

	e.Logger.Info("[ArgoCD] Syncing application",
		zap.String("request_id", req.RequestId),
		zap.String("app_name", appName))

	path := fmt.Sprintf("/api/v1/applications/%s/sync", appName)
	body, code, err := e.doPost(ctx, path, []byte(syncBody))
	return e.makeResponse(req.RequestId, body, code, err)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func (e *ArgoCDExecutor) doGet(ctx context.Context, path string) ([]byte, int, error) {
	e.mu.RLock()
	token := e.authToken
	baseURL := e.baseURL
	e.mu.RUnlock()

	fullURL := baseURL + path

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := e.httpClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		e.Logger.Error("[ArgoCD] HTTP GET failed",
			zap.String("url", truncate(fullURL, 200)),
			zap.Duration("elapsed", elapsed),
			zap.Error(err))
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	e.Logger.Debug("[ArgoCD] HTTP GET response",
		zap.String("path", path),
		zap.Int("status", resp.StatusCode),
		zap.Int("body_bytes", len(body)),
		zap.Duration("elapsed", elapsed))

	return body, resp.StatusCode, nil
}

func (e *ArgoCDExecutor) doPost(ctx context.Context, path string, jsonBody []byte) ([]byte, int, error) {
	e.mu.RLock()
	token := e.authToken
	baseURL := e.baseURL
	e.mu.RUnlock()

	fullURL := baseURL + path

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := e.httpClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		e.Logger.Error("[ArgoCD] HTTP POST failed",
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

	e.Logger.Debug("[ArgoCD] HTTP POST response",
		zap.String("path", path),
		zap.Int("status", resp.StatusCode),
		zap.Int("body_bytes", len(body)),
		zap.Duration("elapsed", elapsed))

	return body, resp.StatusCode, nil
}

func (e *ArgoCDExecutor) makeResponse(requestID string, body []byte, statusCode int, err error) *v1.DatadogQueryResponse {
	resp := &v1.DatadogQueryResponse{RequestId: requestID}
	if err != nil {
		resp.Success = false
		resp.ErrorMessage = err.Error()
		resp.StatusCode = int32(statusCode)
		return resp
	}
	if statusCode < 200 || statusCode >= 300 {
		resp.Success = false
		resp.ErrorMessage = fmt.Sprintf("ArgoCD API returned HTTP %d: %s", statusCode, truncate(string(body), 500))
		resp.StatusCode = int32(statusCode)
		return resp
	}
	resp.Success = true
	resp.ResponseData = string(body)
	resp.StatusCode = int32(statusCode)
	return resp
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

func (e *ArgoCDExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.discovered
}

func (e *ArgoCDExecutor) Probe(ctx context.Context) bool {
	if err := e.ensureDiscovered(ctx); err != nil {
		e.Logger.Info("[ArgoCD] Probe: ArgoCD not found in cluster (this is normal if ArgoCD is not deployed)",
			zap.Error(err))
		return false
	}
	e.mu.RLock()
	ns := e.namespace
	e.mu.RUnlock()
	e.Logger.Info("[ArgoCD] Probe: ArgoCD available",
		zap.String("namespace", ns))
	return true
}

func (e *ArgoCDExecutor) ResetDiscovery() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.discovered = false
	e.namespace = ""
	e.baseURL = ""
	e.authToken = ""
	e.tokenExp = time.Time{}
	e.Logger.Info("[ArgoCD] Discovery state reset — will re-discover on next query")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
