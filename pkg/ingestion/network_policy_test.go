package ingestion

import (
	"testing"

	v1 "operator/api/cloud/v1"
	testhelper "operator/pkg/test_helper"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestNetworkPolicySuite runs the NetworkPolicyTestSuite
func TestNetworkPolicySuite(t *testing.T) {
	suite.Run(t, new(NetworkPolicyTestSuite))
}

type NetworkPolicyTestSuite struct {
	testhelper.ControllerTestSuite
}

// Network Policy Tests
func (suite *NetworkPolicyTestSuite) TestParseNetworkPolicyYAML() {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{
			name: "valid network policy",
			yaml: `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: test-policy
  namespace: default
spec:
  podSelector:
    matchLabels:
      role: db
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          project: myproject
    ports:
    - protocol: TCP
      port: 6379
`,
			wantErr: false,
		},
		{
			name:    "empty yaml",
			yaml:    "",
			wantErr: true,
		},
		{
			name: "invalid yaml",
			yaml: `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: test-policy-invalid
  namespace: default
spec:
  podSelector:
    matchLabels:
      role: db
  policyTypes:
  - InvalidType
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := ParseNetworkPolicyYAML(zap.NewNop(), tt.yaml)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("ParseNetworkPolicyYAML() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				suite.T().Error("ParseNetworkPolicyYAML() returned nil policy when no error expected")
			}
		})
	}
}

func (suite *NetworkPolicyTestSuite) TestCheckNetworkPolicy() {
	tests := []struct {
		name    string
		policy  string
		wantErr bool
	}{
		{
			name: "valid network policy",
			policy: `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: test-policy-evan
  namespace: default
spec:
  podSelector:
    matchLabels:
      role: db
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          project: myproject
    ports:
    - protocol: TCP
      port: 6379
`,
			wantErr: false,
		},
		{
			name:    "empty policy",
			policy:  "",
			wantErr: true,
		},
		{
			name: "invalid policy type",
			policy: `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: test-policy-2
  namespace: default
spec:
  podSelector:
    matchLabels:
      role: db
  policyTypes:
  - InvalidType
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			err := CheckNetworkPolicy(zap.NewNop(), tt.policy)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("CheckNetworkPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *NetworkPolicyTestSuite) TestApplyNetworkPolicy() {
	// Create a test network policy
	testPolicy := &v1.NetworkPolicy{
		Metadata: &v1.ObjectMeta{
			Name:      "test-policy-raman",
			Namespace: "default",
		},
		Spec: &v1.NetworkPolicySpec{
			PodSelector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"role": "db",
				},
			},
			PolicyTypes: []string{"Ingress"},
			Ingress: []*v1.NetworkPolicyIngressRule{
				{
					From: []*v1.NetworkPolicyPeer{
						{
							NamespaceSelector: &v1.LabelSelector{
								MatchLabels: map[string]string{
									"project": "myproject",
								},
							},
						},
					},
					Ports: []*v1.NetworkPolicyPort{
						{
							Protocol: "TCP",
							PortValue: &v1.NetworkPolicyPort_Port{
								Port: 6379,
							},
						},
					},
				},
			},
		},
	}

	// Convert to k8s NetworkPolicy
	k8sPolicy := ConvertToK8sNetworkPolicy(testPolicy)

	tests := []struct {
		name    string
		policy  *networkingv1.NetworkPolicy
		wantErr bool
	}{
		{
			name:    "valid policy",
			policy:  k8sPolicy,
			wantErr: false,
		},
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: true,
		},
		{
			name: "policy with invalid namespace",
			policy: &networkingv1.NetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-policy-random",
					Namespace: "invalid-namespace",
				},
				Spec: networkingv1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			err := ApplyNetworkPolicy(suite.GetContext(), suite.GetClientset(), tt.policy)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("ApplyNetworkPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *NetworkPolicyTestSuite) TestConvertToK8sNetworkPolicy() {
	testPolicy := &v1.NetworkPolicy{
		Metadata: &v1.ObjectMeta{
			Name:      "test-policy-1",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test",
			},
		},
		Spec: &v1.NetworkPolicySpec{
			PodSelector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"role": "db",
				},
			},
			PolicyTypes: []string{"Ingress", "Egress"},
			Ingress: []*v1.NetworkPolicyIngressRule{
				{
					From: []*v1.NetworkPolicyPeer{
						{
							PodSelector: &v1.LabelSelector{
								MatchLabels: map[string]string{
									"app": "web",
								},
							},
						},
					},
					Ports: []*v1.NetworkPolicyPort{
						{
							Protocol: "TCP",
							PortValue: &v1.NetworkPolicyPort_Port{
								Port: 80,
							},
						},
					},
				},
			},
			Egress: []*v1.NetworkPolicyEgressRule{
				{
					To: []*v1.NetworkPolicyPeer{
						{
							IpBlock: &v1.IPBlock{
								Cidr:   "10.0.0.0/24",
								Except: []string{"10.0.0.1"},
							},
						},
					},
				},
			},
		},
	}

	got := ConvertToK8sNetworkPolicy(testPolicy)

	// Verify basic fields
	suite.Equal(testPolicy.Metadata.Name, got.Name, "name should match")
	suite.Equal(testPolicy.Metadata.Namespace, got.Namespace, "namespace should match")

	// Verify policy types
	suite.Equal(2, len(got.Spec.PolicyTypes), "policy types length should be 2")

	// Verify ingress rules
	suite.Equal(1, len(got.Spec.Ingress), "ingress rules length should be 1")

	// Verify egress rules
	suite.Equal(1, len(got.Spec.Egress), "egress rules length should be 1")
}

func (suite *NetworkPolicyTestSuite) TestConvertToK8sLabelSelector() {
	tests := []struct {
		name     string
		selector *v1.LabelSelector
		want     metav1.LabelSelector
	}{
		{
			name: "with match labels",
			selector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "test",
				},
			},
			want: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "test",
				},
			},
		},
		{
			name: "with match expressions",
			selector: &v1.LabelSelector{
				MatchExpressions: []*v1.LabelSelectorRequirement{
					{
						Key:      "environment",
						Operator: "In",
						Values:   []string{"prod", "staging"},
					},
				},
			},
			want: metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "environment",
						Operator: "In",
						Values:   []string{"prod", "staging"},
					},
				},
			},
		},
		{
			name:     "nil selector",
			selector: nil,
			want:     metav1.LabelSelector{},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got := ConvertToK8sLabelSelector(tt.selector)
			suite.True(labelSelectorsEqual(got, tt.want), "label selectors should be equal")
		})
	}
}

// Helper function to compare label selectors
func labelSelectorsEqual(a, b metav1.LabelSelector) bool {
	if len(a.MatchLabels) != len(b.MatchLabels) {
		return false
	}
	for k, v := range a.MatchLabels {
		if b.MatchLabels[k] != v {
			return false
		}
	}
	if len(a.MatchExpressions) != len(b.MatchExpressions) {
		return false
	}
	for i, expr := range a.MatchExpressions {
		if expr.Key != b.MatchExpressions[i].Key ||
			expr.Operator != b.MatchExpressions[i].Operator ||
			!stringSlicesEqual(expr.Values, b.MatchExpressions[i].Values) {
			return false
		}
	}
	return true
}

// Helper function to compare string slices
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (suite *NetworkPolicyTestSuite) TestErrorNetworkPolicyWithID() {
	// Test policy with a specific ID
	errorPolicy := &v1.NetworkPolicyWithError{
		NetworkPolicy: `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: test-policy-with-id
  namespace: default
spec:
  podSelector:
    matchLabels:
      app: web
  policyTypes:
  - Ingress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          role: frontend
    ports:
    - protocol: TCP
      port: 80
`,
		PolicyId: "test-policy-id-12345",
	}

	// First, check that the policy is valid
	err := CheckNetworkPolicy(zap.NewNop(), errorPolicy.NetworkPolicy)
	suite.NoError(err, "Policy should be valid")

	// Make sure the ID is preserved
	suite.Equal("test-policy-id-12345", errorPolicy.PolicyId, "Policy ID should be preserved")

	// Parse the policy and verify its contents
	k8sPolicy, err := ParseNetworkPolicyYAML(zap.NewNop(), errorPolicy.NetworkPolicy)
	suite.NoError(err, "Should parse without error")
	suite.Equal("test-policy-with-id", k8sPolicy.Name, "Policy name should match")
	suite.Equal("default", k8sPolicy.Namespace, "Namespace should match")

	// Verify the policy specifies the expected labels
	podSelector := k8sPolicy.Spec.PodSelector
	suite.Contains(podSelector.MatchLabels, "app", "Pod selector should contain 'app' label")
	suite.Equal("web", podSelector.MatchLabels["app"], "App label should be 'web'")
}
