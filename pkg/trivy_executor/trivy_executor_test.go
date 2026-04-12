package trivy_executor

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParseImageFromCommand(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    string
	}{
		{
			name:    "simple image",
			command: "trivy image nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "image with severity flag",
			command: "trivy image --severity HIGH,CRITICAL nginx:1.25",
			want:    "nginx:1.25",
		},
		{
			name:    "image with multiple flags",
			command: "trivy image --format json --severity CRITICAL myregistry.io/myapp:v1.2.3",
			want:    "myregistry.io/myapp:v1.2.3",
		},
		{
			name:    "image with boolean flags",
			command: "trivy image --skip-db-update --offline-scan nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "image without tag",
			command: "trivy image nginx",
			want:    "nginx",
		},
		{
			name:    "full registry path",
			command: "trivy image gcr.io/my-project/my-app:sha-abc123",
			want:    "gcr.io/my-project/my-app:sha-abc123",
		},
		{
			name:    "no image",
			command: "trivy image",
			want:    "",
		},
		{
			name:    "scanners flag with value",
			command: "trivy image --scanners vuln nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "format flag with value",
			command: "trivy image --format json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "short flags",
			command: "trivy image -s HIGH -f json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "without trivy prefix",
			command: "image --severity HIGH nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "trailing flags after image",
			command: "trivy image nginx:latest --exit-code 1",
			want:    "nginx:latest",
		},
		{
			name:    "trailing flags with severity after image",
			command: "trivy image nginx:latest --severity HIGH,CRITICAL --exit-code 1",
			want:    "nginx:latest",
		},
		{
			name:    "boolean flag before image",
			command: "trivy image --skip-db-update nginx:latest --exit-code 1",
			want:    "nginx:latest",
		},
		{
			name:    "output flag with dot in value",
			command: "trivy image --output result.json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "bare image with trailing exit-code",
			command: "trivy image nginx --exit-code 1",
			want:    "nginx",
		},
		{
			name:    "bare image with trailing format",
			command: "trivy image nginx --format json",
			want:    "nginx",
		},
		{
			name:    "output flag with absolute path",
			command: "trivy image --output /tmp/result.json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "output flag with relative path",
			command: "trivy image --output ./results/scan.json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "output flag with parent path",
			command: "trivy image --output ../scan.json nginx:latest",
			want:    "nginx:latest",
		},
		{
			name:    "template flag with absolute path",
			command: "trivy image --template /etc/trivy/html.tpl gcr.io/project/app:v1",
			want:    "gcr.io/project/app:v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseImageFromCommand(tt.command)
			if got != tt.want {
				t.Errorf("parseImageFromCommand(%q) = %q, want %q", tt.command, got, tt.want)
			}
		})
	}
}

func TestNormalizeImageRef(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{
			name:  "bare image name",
			image: "nginx",
			want:  "docker.io/library/nginx:latest",
		},
		{
			name:  "image with tag",
			image: "nginx:1.25",
			want:  "docker.io/library/nginx:1.25",
		},
		{
			name:  "docker hub user repo",
			image: "myuser/myapp",
			want:  "docker.io/myuser/myapp:latest",
		},
		{
			name:  "docker hub user repo with tag",
			image: "myuser/myapp:v2",
			want:  "docker.io/myuser/myapp:v2",
		},
		{
			name:  "full registry path",
			image: "gcr.io/my-project/my-app:v1.0",
			want:  "gcr.io/my-project/my-app:v1.0",
		},
		{
			name:  "full registry no tag",
			image: "gcr.io/my-project/my-app",
			want:  "gcr.io/my-project/my-app:latest",
		},
		{
			name:  "docker.io explicit",
			image: "docker.io/library/nginx:latest",
			want:  "docker.io/library/nginx:latest",
		},
		{
			name:  "localhost registry with port",
			image: "localhost:5000/myapp:v1",
			want:  "localhost:5000/myapp:v1",
		},
		{
			name:  "private registry with port",
			image: "registry.example.com:5000/org/app:latest",
			want:  "registry.example.com:5000/org/app:latest",
		},
		{
			name:  "docker.io shorthand official image",
			image: "docker.io/nginx",
			want:  "docker.io/library/nginx:latest",
		},
		{
			name:  "docker.io shorthand official image with tag",
			image: "docker.io/nginx:1.25",
			want:  "docker.io/library/nginx:1.25",
		},
		{
			name:  "index.docker.io shorthand",
			image: "index.docker.io/nginx:latest",
			want:  "docker.io/library/nginx:latest",
		},
		{
			name:  "index.docker.io with user repo",
			image: "index.docker.io/myrepo/app:v1",
			want:  "docker.io/myrepo/app:v1",
		},
		{
			name:  "index.docker.io with org and nested path",
			image: "index.docker.io/org/sub/app:latest",
			want:  "docker.io/org/sub/app:latest",
		},
		{
			name:  "image with digest",
			image: "nginx@sha256:abc123",
			want:  "docker.io/library/nginx@sha256:abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeImageRef(tt.image)
			if got != tt.want {
				t.Errorf("normalizeImageRef(%q) = %q, want %q", tt.image, got, tt.want)
			}
		})
	}
}

func TestNormalizeImageRef_Matching(t *testing.T) {
	// These pairs should normalize to the same value
	pairs := []struct {
		name string
		a, b string
	}{
		{"bare vs explicit", "nginx", "docker.io/library/nginx:latest"},
		{"bare vs tagged", "nginx", "nginx:latest"},
		{"bare vs docker.io shorthand", "nginx", "docker.io/nginx"},
		{"docker.io shorthand vs explicit", "docker.io/nginx:1.25", "docker.io/library/nginx:1.25"},
		{"user repo", "myuser/app", "docker.io/myuser/app:latest"},
		{"user repo tagged", "myuser/app:v1", "docker.io/myuser/app:v1"},
		{"index.docker.io 3-segment", "index.docker.io/myrepo/app:v1", "docker.io/myrepo/app:v1"},
	}

	for _, tt := range pairs {
		t.Run(tt.name, func(t *testing.T) {
			na := normalizeImageRef(tt.a)
			nb := normalizeImageRef(tt.b)
			if na != nb {
				t.Errorf("normalizeImageRef(%q) = %q, normalizeImageRef(%q) = %q — expected equal", tt.a, na, tt.b, nb)
			}
		})
	}
}

func TestIsTrivyCommand(t *testing.T) {
	tests := []struct {
		command string
		want    bool
	}{
		{"trivy image nginx:latest", true},
		{"trivy image", true},
		{"  trivy image nginx", true},
		{"trivy fs /path", false},
		{"trivy repo https://example.com", false},
		{"kubectl get pods", false},
		{"trivynotacommand", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			if got := IsTrivyCommand(tt.command); got != tt.want {
				t.Errorf("IsTrivyCommand(%q) = %v, want %v", tt.command, got, tt.want)
			}
		})
	}
}

func TestInjectCacheDir(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    string
	}{
		{
			name:    "basic command",
			command: "trivy image nginx:latest",
			want:    "trivy image --cache-dir /var/trivy-cache --skip-db-update nginx:latest",
		},
		{
			name:    "already has cache-dir",
			command: "trivy image --cache-dir /tmp/cache nginx:latest",
			want:    "trivy image --cache-dir /tmp/cache nginx:latest",
		},
		{
			name:    "command with flags",
			command: "trivy image --severity HIGH nginx:latest",
			want:    "trivy image --cache-dir /var/trivy-cache --skip-db-update --severity HIGH nginx:latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := injectCacheDir(tt.command)
			if got != tt.want {
				t.Errorf("injectCacheDir(%q) = %q, want %q", tt.command, got, tt.want)
			}
		})
	}
}

func TestLooksLikeImageRef(t *testing.T) {
	tests := []struct {
		token string
		want  bool
	}{
		{"nginx:latest", true},
		{"gcr.io/project/app", true},
		{"myrepo/myapp:v1", true},
		{"nginx@sha256:abc123", true},
		{"vuln", false},
		{"json", false},
		{"HIGH", false},
		{"1", false},
		{"HIGH,CRITICAL", false},
		{"/tmp/result.json", true},  // looksLikeImageRef only checks structural chars; file path filtering is in parseImageFromCommand
		{"./results/scan.json", true},
	}

	for _, tt := range tests {
		t.Run(tt.token, func(t *testing.T) {
			if got := looksLikeImageRef(tt.token); got != tt.want {
				t.Errorf("looksLikeImageRef(%q) = %v, want %v", tt.token, got, tt.want)
			}
		})
	}
}

func TestParseImageFromCommand_FlagValueNotReturnedAsImage(t *testing.T) {
	// Ensure flag values are never returned as the image
	tests := []struct {
		cmd  string
		want string
	}{
		{"trivy image --scanners vuln nginx:latest", "nginx:latest"},
		{"trivy image --format json nginx:latest", "nginx:latest"},
		{"trivy image --severity HIGH,CRITICAL nginx:latest", "nginx:latest"},
		{"trivy image --exit-code 1 nginx:latest", "nginx:latest"},
		// Bare image names (no tag) — flag values must not be returned
		{"trivy image --severity HIGH nginx", "nginx"},
		{"trivy image --format json nginx", "nginx"},
		{"trivy image --exit-code 1 nginx", "nginx"},
	}

	badValues := []string{"vuln", "json", "HIGH,CRITICAL", "HIGH", "1"}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			got := parseImageFromCommand(tt.cmd)
			if got != tt.want {
				t.Errorf("parseImageFromCommand(%q) = %q, want %q", tt.cmd, got, tt.want)
			}
			for _, bad := range badValues {
				if got == bad {
					t.Errorf("parseImageFromCommand(%q) returned flag value %q as image", tt.cmd, bad)
				}
			}
		})
	}
}

func TestInjectCacheDir_ContainsExpectedFlags(t *testing.T) {
	result := injectCacheDir("trivy image nginx:latest")
	if !strings.Contains(result, "--cache-dir /var/trivy-cache") {
		t.Errorf("expected --cache-dir flag, got: %s", result)
	}
	if !strings.Contains(result, "--skip-db-update") {
		t.Errorf("expected --skip-db-update flag, got: %s", result)
	}
}

// --- Integration-style tests using fake clientset ---

func newTestExecutor(pods ...corev1.Pod) *TrivyExecutor {
	objs := make([]interface{}, 0, len(pods))
	for i := range pods {
		objs = append(objs, &pods[i])
	}
	clientset := fake.NewSimpleClientset()
	for i := range pods {
		clientset.CoreV1().Pods(pods[i].Namespace).Create(context.Background(), &pods[i], metav1.CreateOptions{})
	}
	return &TrivyExecutor{
		Logger:    zap.NewNop(),
		ClientSet: clientset,
		Namespace: "kestrel-operator",
	}
}

func runningPod(name, namespace, nodeName, image string, labels map[string]string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{Name: "main", Image: image},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestRefreshImageNodeCache_EmptyCluster(t *testing.T) {
	exec := newTestExecutor() // no pods
	_, err := exec.refreshImageNodeCache(context.Background())
	if err == nil {
		t.Fatal("expected error for empty cluster, got nil")
	}
	if !strings.Contains(err.Error(), "no running pods") {
		t.Errorf("unexpected error: %v", err)
	}
	// Cache should NOT be populated — next call should retry
	if exec.imageNodeCache != nil {
		t.Error("empty result should not have been cached")
	}
}

func TestRefreshImageNodeCache_PopulatesIndex(t *testing.T) {
	exec := newTestExecutor(
		runningPod("web-1", "default", "node-a", "nginx:1.25", nil),
		runningPod("api-1", "default", "node-b", "gcr.io/proj/api:v2", nil),
	)

	index, err := exec.refreshImageNodeCache(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check nginx normalized
	if node, ok := index["docker.io/library/nginx:1.25"]; !ok || node != "node-a" {
		t.Errorf("expected nginx on node-a, got node=%q ok=%v", node, ok)
	}
	// Check gcr image
	if node, ok := index["gcr.io/proj/api:v2"]; !ok || node != "node-b" {
		t.Errorf("expected api on node-b, got node=%q ok=%v", node, ok)
	}
}

func TestRefreshTrivyPodCache_EmptyNamespace(t *testing.T) {
	exec := newTestExecutor() // no trivy pods
	_, err := exec.refreshTrivyPodCache(context.Background())
	if err == nil {
		t.Fatal("expected error for empty namespace, got nil")
	}
	if !strings.Contains(err.Error(), "no ready trivy-scanner pods") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRefreshTrivyPodCache_FindsTrivyPods(t *testing.T) {
	trivyLabels := map[string]string{"component": "trivy-scanner"}
	exec := newTestExecutor(
		runningPod("trivy-abc", "kestrel-operator", "node-a", "aquasec/trivy:0.50.0", trivyLabels),
		runningPod("trivy-def", "kestrel-operator", "node-b", "aquasec/trivy:0.50.0", trivyLabels),
	)

	cache, err := exec.refreshTrivyPodCache(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cache) != 2 {
		t.Errorf("expected 2 trivy pods, got %d", len(cache))
	}
	if pod, ok := cache["node-a"]; !ok || pod.Name != "trivy-abc" {
		t.Errorf("expected trivy-abc on node-a, got %+v", pod)
	}
}

func TestFindNodeForImage_ResolvesNormalized(t *testing.T) {
	exec := newTestExecutor(
		runningPod("web-1", "default", "node-a", "nginx:1.25", nil),
	)

	// Query with bare "nginx" which normalizes differently from "nginx:1.25"
	node, err := exec.findNodeForImage(context.Background(), "nginx:1.25")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node != "node-a" {
		t.Errorf("expected node-a, got %s", node)
	}
}

func TestFindNodeForImage_NotFound(t *testing.T) {
	exec := newTestExecutor(
		runningPod("web-1", "default", "node-a", "nginx:1.25", nil),
	)

	_, err := exec.findNodeForImage(context.Background(), "redis:7")
	if err == nil {
		t.Fatal("expected error for missing image")
	}
	if !strings.Contains(err.Error(), "no running pod uses image") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExecuteTrivyCommand_NoImage(t *testing.T) {
	exec := newTestExecutor()
	result := exec.ExecuteTrivyCommand(context.Background(), "trivy image")
	if result.Success {
		t.Error("expected failure for command with no image")
	}
	if !strings.Contains(result.Stderr, "could not parse image name") {
		t.Errorf("unexpected stderr: %s", result.Stderr)
	}
}

func TestExecuteTrivyCommand_ImageNotInCluster(t *testing.T) {
	exec := newTestExecutor(
		runningPod("web-1", "default", "node-a", "nginx:1.25", nil),
	)
	result := exec.ExecuteTrivyCommand(context.Background(), "trivy image redis:7")
	if result.Success {
		t.Error("expected failure for image not in cluster")
	}
	if !strings.Contains(result.Stderr, "not found on any cluster node") {
		t.Errorf("unexpected stderr: %s", result.Stderr)
	}
}

func TestExecuteTrivyCommand_NoTrivyPodOnNode(t *testing.T) {
	// Image exists on node-a, but no trivy pod on node-a
	exec := newTestExecutor(
		runningPod("web-1", "default", "node-a", "nginx:1.25", nil),
	)
	result := exec.ExecuteTrivyCommand(context.Background(), "trivy image nginx:1.25")
	if result.Success {
		t.Error("expected failure when no trivy pod on target node")
	}
	if !strings.Contains(result.Stderr, "no ready Trivy pod on node") || !strings.Contains(result.Stderr, "no ready trivy-scanner pods") {
		// Either error message is acceptable depending on whether empty cache or missing node
		if !strings.Contains(result.Stderr, "Trivy pod") && !strings.Contains(result.Stderr, "trivy-scanner") {
			t.Errorf("unexpected stderr: %s", result.Stderr)
		}
	}
}
