package trivy_executor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// TrivyExecutor discovers Trivy DaemonSet pods and routes image scan
// commands to the Trivy server running on the node where the target image
// exists in the local container runtime.
//
// Routing flow:
//  1. Parse the image reference from the trivy command
//  2. List all cluster pods to find which node(s) have pulled that image
//  3. Find the Trivy DaemonSet pod on that node
//  4. Exec `trivy image` inside that pod (which has the runtime socket mounted)
type TrivyExecutor struct {
	Logger     *zap.Logger
	ClientSet  kubernetes.Interface
	RestConfig *rest.Config
	Namespace  string // operator namespace (where DaemonSet is deployed)

	mu             sync.RWMutex
	trivyPodCache  map[string]trivyPod // nodeName → trivy pod
	trivyCacheTime time.Time

	imageMu        sync.RWMutex
	imageNodeCache map[string]string // normalizedImage → nodeName
	imageCacheTime time.Time
}

type trivyPod struct {
	Name     string
	IP       string
	NodeName string
}

const (
	trivyPodCacheTTL  = 30 * time.Second
	imageNodeCacheTTL = 30 * time.Second
	trivyLabelKey     = "component"
	trivyLabelValue   = "trivy-scanner"
)

func NewTrivyExecutor(
	logger *zap.Logger,
	clientSet kubernetes.Interface,
	restConfig *rest.Config,
	namespace string,
) *TrivyExecutor {
	return &TrivyExecutor{
		Logger:     logger,
		ClientSet:  clientSet,
		RestConfig: restConfig,
		Namespace:  namespace,
	}
}

// IsTrivyCommand returns true if the command is a "trivy image" scan.
// Only image scans are routed to the DaemonSet; other subcommands
// (trivy fs, trivy repo, etc.) fall through to the shell executor.
func IsTrivyCommand(command string) bool {
	trimmed := strings.TrimSpace(command)
	return strings.HasPrefix(trimmed, "trivy image ") || trimmed == "trivy image"
}

// ExecuteTrivyCommand intercepts a trivy shell command and routes it to the
// Trivy DaemonSet pod on the node where the target image is present.
func (e *TrivyExecutor) ExecuteTrivyCommand(ctx context.Context, command string) *v1.ShellCommandResult {
	result := &v1.ShellCommandResult{
		Command: command,
	}

	// Parse the image reference from the command
	image := parseImageFromCommand(command)
	if image == "" {
		result.Success = false
		result.Stderr = fmt.Sprintf("could not parse image name from trivy command: %s", command)
		result.ExitCode = 1
		return result
	}

	// Find which node has this image by looking up running pods
	node, err := e.findNodeForImage(ctx, image)
	if err != nil {
		result.Success = false
		result.Stderr = fmt.Sprintf("image %q not found on any cluster node: %v", image, err)
		result.ExitCode = 1
		return result
	}

	// Find the Trivy DaemonSet pod on that node
	pod, err := e.getTrivyPodOnNode(ctx, node)
	if err != nil {
		result.Success = false
		result.Stderr = fmt.Sprintf("no ready Trivy pod on node %s: %v", node, err)
		result.ExitCode = 1
		return result
	}

	// Inject --cache-dir to use the server's pre-downloaded vulnerability DB.
	// The DaemonSet runs `trivy server --cache-dir /var/trivy-cache`, so the
	// exec'd CLI must use the same directory to avoid a separate DB download.
	execCommand := injectCacheDir(command)

	e.Logger.Info("Routing trivy scan to DaemonSet pod on target node",
		zap.String("image", image),
		zap.String("node", node),
		zap.String("trivy_pod", pod.Name))

	stdout, stderr, exitCode, err := e.execInPod(ctx, pod, execCommand)
	if err != nil {
		result.Success = false
		result.Stderr = fmt.Sprintf("exec into trivy pod %s failed: %v\nstderr: %s", pod.Name, err, stderr)
		result.Stdout = stdout
		result.ExitCode = 1
		return result
	}

	result.Success = exitCode == 0
	result.Stdout = stdout
	result.Stderr = stderr
	result.ExitCode = int32(exitCode)
	return result
}

// findNodeForImage looks up which node has the target image by consulting a
// cached image→node index. The index is built by listing all running pods
// cluster-wide and is refreshed every 30 seconds, so repeated scans within
// a short window don't each trigger a full pod list.
func (e *TrivyExecutor) findNodeForImage(ctx context.Context, image string) (string, error) {
	normalizedTarget := normalizeImageRef(image)

	index, err := e.refreshImageNodeCache(ctx)
	if err != nil {
		return "", err
	}

	node, ok := index[normalizedTarget]
	if !ok {
		return "", fmt.Errorf("no running pod uses image %q (normalized: %q)", image, normalizedTarget)
	}

	e.Logger.Debug("Resolved image to node via cache",
		zap.String("image", image),
		zap.String("normalized", normalizedTarget),
		zap.String("node", node))
	return node, nil
}

// refreshImageNodeCache rebuilds the normalizedImage→nodeName index if stale.
func (e *TrivyExecutor) refreshImageNodeCache(ctx context.Context) (map[string]string, error) {
	e.imageMu.RLock()
	if time.Since(e.imageCacheTime) < imageNodeCacheTTL && e.imageNodeCache != nil {
		cache := e.imageNodeCache
		e.imageMu.RUnlock()
		return cache, nil
	}
	e.imageMu.RUnlock()

	e.imageMu.Lock()
	defer e.imageMu.Unlock()

	// Double-check after acquiring write lock
	if time.Since(e.imageCacheTime) < imageNodeCacheTTL && e.imageNodeCache != nil {
		return e.imageNodeCache, nil
	}

	pods, err := e.ClientSet.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster pods: %w", err)
	}

	index := make(map[string]string)
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}
		for _, container := range pod.Spec.Containers {
			norm := normalizeImageRef(container.Image)
			if _, exists := index[norm]; !exists {
				index[norm] = pod.Spec.NodeName
			}
		}
		for _, container := range pod.Spec.InitContainers {
			norm := normalizeImageRef(container.Image)
			if _, exists := index[norm]; !exists {
				index[norm] = pod.Spec.NodeName
			}
		}
	}

	if len(index) == 0 {
		// Don't persist the empty result — allow retries on the next call
		// instead of serving a stale empty cache for the full TTL.
		return nil, fmt.Errorf("no running pods found in cluster to build image-node index")
	}

	e.imageNodeCache = index
	e.imageCacheTime = time.Now()

	e.Logger.Debug("Refreshed image-to-node cache",
		zap.Int("unique_images", len(index)),
		zap.Int("pods_scanned", len(pods.Items)))

	return index, nil
}

// getTrivyPodOnNode returns the Trivy DaemonSet pod running on the specified node.
func (e *TrivyExecutor) getTrivyPodOnNode(ctx context.Context, nodeName string) (trivyPod, error) {
	cache, err := e.refreshTrivyPodCache(ctx)
	if err != nil {
		return trivyPod{}, err
	}

	pod, ok := cache[nodeName]
	if !ok {
		return trivyPod{}, fmt.Errorf("no trivy-scanner pod found on node %s (trivy pods exist on %d nodes)", nodeName, len(cache))
	}
	return pod, nil
}

// refreshTrivyPodCache refreshes the node→trivyPod cache if stale.
func (e *TrivyExecutor) refreshTrivyPodCache(ctx context.Context) (map[string]trivyPod, error) {
	e.mu.RLock()
	if time.Since(e.trivyCacheTime) < trivyPodCacheTTL && e.trivyPodCache != nil {
		cache := e.trivyPodCache
		e.mu.RUnlock()
		return cache, nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Double-check after acquiring write lock
	if time.Since(e.trivyCacheTime) < trivyPodCacheTTL && e.trivyPodCache != nil {
		return e.trivyPodCache, nil
	}

	pods, err := e.ClientSet.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", trivyLabelKey, trivyLabelValue),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list trivy pods: %w", err)
	}

	cache := make(map[string]trivyPod)
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || p.Spec.NodeName == "" {
			continue
		}
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if ready {
			cache[p.Spec.NodeName] = trivyPod{
				Name:     p.Name,
				IP:       p.Status.PodIP,
				NodeName: p.Spec.NodeName,
			}
		}
	}

	if len(cache) == 0 {
		// Don't persist the empty result — allow retries on the next call
		// instead of serving a stale empty cache for the full TTL.
		return nil, fmt.Errorf("no ready trivy-scanner pods found in namespace %s", e.Namespace)
	}

	e.trivyPodCache = cache
	e.trivyCacheTime = time.Now()

	e.Logger.Debug("Refreshed trivy pod cache",
		zap.Int("ready_pods", len(cache)))

	return cache, nil
}

// execInPod uses the Kubernetes remotecommand API to exec a command inside
// the Trivy DaemonSet pod's trivy-server container.
// The command is split into argv tokens and exec'd directly (no shell) to
// prevent injection via the runtime-socket-mounted container.
func (e *TrivyExecutor) execInPod(ctx context.Context, pod trivyPod, command string) (stdout, stderr string, exitCode int, err error) {
	argv := strings.Fields(command)
	if len(argv) < 2 || argv[0] != "trivy" || argv[1] != "image" {
		return "", "", 1, fmt.Errorf("refusing to exec non-trivy-image command: %q", command)
	}

	req := e.ClientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(e.Namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Container: "trivy-server",
		Command:   argv,
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(e.RestConfig, "POST", req.URL())
	if err != nil {
		return "", "", 1, fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	var stdoutBuf, stderrBuf strings.Builder
	streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdoutBuf,
		Stderr: &stderrBuf,
	})

	if streamErr != nil {
		if exitErr, ok := streamErr.(interface{ ExitStatus() int }); ok {
			return stdoutBuf.String(), stderrBuf.String(), exitErr.ExitStatus(), nil
		}
		return stdoutBuf.String(), stderrBuf.String(), 1, streamErr
	}

	return stdoutBuf.String(), stderrBuf.String(), 0, nil
}

// parseImageFromCommand extracts the image reference from a trivy CLI command.
// Expected formats:
//
//	"trivy image nginx:latest"
//	"trivy image --severity HIGH nginx:latest"
//	"trivy image --scanners vuln nginx:latest"
//	"trivy image nginx:latest --exit-code 1"
//
// Instead of maintaining a fragile allowlist of trivy flags, we identify the
// image by its shape: image references contain ":" (tag), "/" (registry/repo),
// "." (FQDN), or "@" (digest). Flag values like "vuln", "json", "1", "HIGH"
// never match this pattern. Single-word images like "nginx" (no tag, no
// registry) are matched as any non-flag token that contains only valid image
// name chars and at least one letter.
func parseImageFromCommand(command string) string {
	parts := strings.Fields(command)

	// Skip "trivy" prefix if present
	if len(parts) > 0 && parts[0] == "trivy" {
		parts = parts[1:]
	}
	// Skip "image" subcommand if present
	if len(parts) > 0 && parts[0] == "image" {
		parts = parts[1:]
	}

	// Collect positional arguments (non-flag tokens that aren't flag values).
	// A non-flag token that immediately follows a flag is assumed to be that
	// flag's value — UNLESS it contains /:@ (strong image ref indicators),
	// since flag values like "HIGH", "json", "1", "result.json" never have those.
	// Consecutive flags (--skip-db-update --offline-scan) are handled naturally:
	// each flag token resets prevWasFlag without consuming a value.
	var positional []string
	prevWasFlag := false
	for _, p := range parts {
		if strings.HasPrefix(p, "-") {
			prevWasFlag = true
			continue
		}
		// Non-flag token
		if prevWasFlag {
			prevWasFlag = false
			if !strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "./") && !strings.HasPrefix(p, "../") &&
				!strings.HasPrefix(p, "@") && !strings.Contains(p, "://") &&
				strings.ContainsAny(p, "/:@") {
				// Strong image ref chars AND not a file path, template ref, or URI — not a flag value
				positional = append(positional, p)
			}
			// else: likely a flag value (HIGH, json, 1, result.json, /tmp/out.json, @contrib/html.tpl) — skip
			continue
		}
		positional = append(positional, p)
	}

	// First pass: find a positional arg that looks like an image reference
	// (contains /, :, @, or . which are image-specific characters)
	for _, p := range positional {
		if looksLikeImageRef(p) {
			return p
		}
	}

	// Second pass: return the last positional arg (handles bare names like "nginx").
	if len(positional) > 0 {
		return positional[len(positional)-1]
	}

	return ""
}

// looksLikeImageRef returns true if the token looks like a container image
// reference rather than a flag value. Image refs contain structural characters
// that flag values (like "vuln", "json", "HIGH", "1") don't.
func looksLikeImageRef(token string) bool {
	return strings.ContainsAny(token, "/:@.")
}

// trivyCacheDir is the cache directory used by the trivy server in the DaemonSet.
// The exec'd CLI must use the same directory to share the vulnerability DB.
const trivyCacheDir = "/var/trivy-cache"

// injectCacheDir adds --cache-dir and --skip-db-update flags to a trivy command
// so the exec'd CLI uses the DaemonSet server's pre-downloaded vulnerability DB
// instead of attempting a fresh download.
func injectCacheDir(command string) string {
	if strings.Contains(command, "--cache-dir") {
		return command
	}
	// Insert after "trivy image" so flags are in a valid position
	return strings.Replace(command, "trivy image", "trivy image --cache-dir "+trivyCacheDir+" --skip-db-update", 1)
}

// normalizeImageRef normalizes a container image reference for comparison.
// Handles Docker Hub defaults:
//
//	"nginx"           → "docker.io/library/nginx:latest"
//	"nginx:1.25"      → "docker.io/library/nginx:1.25"
//	"myrepo/app"      → "docker.io/myrepo/app:latest"
//	"gcr.io/proj/app" → "gcr.io/proj/app:latest"
func normalizeImageRef(image string) string {
	// Split off the tag/digest
	ref := image
	tag := "latest"
	hasDigest := false
	if idx := strings.LastIndex(ref, "@"); idx != -1 {
		// Has a digest — preserve it directly (no colon prefix)
		tag = ref[idx:]
		ref = ref[:idx]
		hasDigest = true
	} else if idx := strings.LastIndex(ref, ":"); idx != -1 {
		// Ensure this colon is a tag separator, not a port separator.
		// A port colon would appear before a slash (e.g., "localhost:5000/app").
		afterColon := ref[idx+1:]
		if !strings.Contains(afterColon, "/") {
			tag = afterColon
			ref = ref[:idx]
		}
	}

	// Normalize the registry/repository
	parts := strings.Split(ref, "/")

	// Canonicalize index.docker.io → docker.io for all segment counts
	if len(parts) >= 2 && parts[0] == "index.docker.io" {
		parts[0] = "docker.io"
		ref = strings.Join(parts, "/")
	}

	switch len(parts) {
	case 1:
		// "nginx" → "docker.io/library/nginx"
		ref = "docker.io/library/" + parts[0]
	case 2:
		// Could be "myrepo/app" (Docker Hub) or "gcr.io/app" (unlikely but possible).
		// If the first part has a dot or colon, treat as registry.
		if strings.Contains(parts[0], ".") || strings.Contains(parts[0], ":") {
			// "docker.io/nginx" → "docker.io/library/nginx" (official image shorthand)
			if parts[0] == "docker.io" {
				ref = "docker.io/library/" + parts[1]
			}
		} else {
			// "myrepo/app" → "docker.io/myrepo/app"
			ref = "docker.io/" + ref
		}
	}

	if hasDigest {
		return ref + tag
	}
	return ref + ":" + tag
}
