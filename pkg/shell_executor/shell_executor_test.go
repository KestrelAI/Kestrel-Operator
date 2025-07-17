package shell_executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseShellArgs(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected []string
		hasError bool
	}{
		{
			name:     "simple command",
			command:  "ls -la",
			expected: []string{"ls", "-la"},
			hasError: false,
		},
		{
			name:     "command with double quoted argument",
			command:  `echo "hello world"`,
			expected: []string{"echo", "hello world"},
			hasError: false,
		},
		{
			name:     "command with single quoted argument",
			command:  `echo 'hello world'`,
			expected: []string{"echo", "hello world"},
			hasError: false,
		},
		{
			name:     "kubectl command with quoted pod name",
			command:  `kubectl get pods "test pod"`,
			expected: []string{"kubectl", "get", "pods", "test pod"},
			hasError: false,
		},
		{
			name:     "command with escaped characters",
			command:  `echo "hello \"world\""`,
			expected: []string{"echo", `hello "world"`},
			hasError: false,
		},
		{
			name:     "command with mixed quotes",
			command:  `echo "hello" 'world'`,
			expected: []string{"echo", "hello", "world"},
			hasError: false,
		},
		{
			name:     "command with multiple spaces",
			command:  "ls    -la     /tmp",
			expected: []string{"ls", "-la", "/tmp"},
			hasError: false,
		},
		{
			name:     "empty command",
			command:  "",
			expected: []string{},
			hasError: false,
		},
		{
			name:     "whitespace only command",
			command:  "   ",
			expected: []string{},
			hasError: false,
		},
		{
			name:     "unclosed double quote",
			command:  `echo "hello world`,
			expected: nil,
			hasError: true,
		},
		{
			name:     "unclosed single quote",
			command:  `echo 'hello world`,
			expected: nil,
			hasError: true,
		},
		{
			name:     "complex kubectl command",
			command:  `kubectl patch deployment myapp -p '{"spec":{"replicas":3}}'`,
			expected: []string{"kubectl", "patch", "deployment", "myapp", "-p", `{"spec":{"replicas":3}}`},
			hasError: false,
		},
		{
			name:     "command with nested quotes",
			command:  `bash -c "echo 'nested quote'"`,
			expected: []string{"bash", "-c", "echo 'nested quote'"},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseShellArgs(tt.command)

			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseShellArgs_ComplexNested(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected []string
		hasError bool
	}{
		{
			name:    "kubectl + jq filter (escaped double quotes)",
			command: `bash -c "kubectl get pods -o json | jq '.items[] | select(.status.phase==\"Running\") | .metadata.name'"`,
			expected: []string{
				"bash", "-c",
				`kubectl get pods -o json | jq '.items[] | select(.status.phase=="Running") | .metadata.name'`,
			},
		},
		{
			name:    "for‑loop over namespaces with command substitution",
			command: `bash -c 'for ns in $(kubectl get ns -o jsonpath="{.items[*].metadata.name}"); do kubectl get pods -n "$ns"; done'`,
			expected: []string{
				"bash", "-c",
				`for ns in $(kubectl get ns -o jsonpath="{.items[*].metadata.name}"); do kubectl get pods -n "$ns"; done`,
			},
		},
		{
			name:    "service list piped through awk and column",
			command: `bash -c "kubectl get svc --all-namespaces | awk '{print $1,$2}' | column -t"`,
			expected: []string{
				"bash", "-c",
				`kubectl get svc --all-namespaces | awk '{print $1,$2}' | column -t`,
			},
		},
		{
			name:    "command substitution echo of current‑context",
			command: `bash -c "echo $(kubectl config current-context)"`,
			expected: []string{
				"bash", "-c",
				`echo $(kubectl config current-context)`,
			},
		},
		{
			name:    "jsonpath, tr, sort, uniq pipeline with nested quotes",
			command: `bash -c "kubectl get pods -o=jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort | uniq"`,
			expected: []string{
				"bash", "-c",
				`kubectl get pods -o=jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort | uniq`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseShellArgs(tt.command)

			if tt.hasError {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}
