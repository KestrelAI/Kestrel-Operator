package ingestion

import (
	"context"
	"fmt"

	v1 "operator/api/cloud/v1"
	"operator/pkg/k8s_helper"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// NetworkPolicyIngester handles the ingestion of network policies from a Kubernetes cluster
type NetworkPolicyIngester struct {
	clientset *kubernetes.Clientset
}

// NewNetworkPolicyIngester creates a new NetworkPolicyIngester instance
func NewNetworkPolicyIngester() (*NetworkPolicyIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s clientset: %v", err)
	}
	return &NetworkPolicyIngester{
		clientset: clientset,
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

// applyNetworkPolicy applies a network policy received from the server to the cluster
func ApplyNetworkPolicy(ctx context.Context, k8sClient *kubernetes.Clientset, policy *v1.NetworkPolicy) error {
	// Convert proto policy to K8s policy
	k8sPolicy := convertToK8sNetworkPolicy(policy)

	// Apply or update the policy
	_, err := k8sClient.NetworkingV1().NetworkPolicies(k8sPolicy.Namespace).Create(ctx, k8sPolicy, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			current, _ := k8sClient.NetworkingV1().
				NetworkPolicies(k8sPolicy.Namespace).
				Get(ctx, k8sPolicy.Name, metav1.GetOptions{})

			k8sPolicy.ResourceVersion = current.ResourceVersion

			_, err = k8sClient.NetworkingV1().
				NetworkPolicies(k8sPolicy.Namespace).
				Update(ctx, k8sPolicy, metav1.UpdateOptions{})

			return err
		}
	}

	return nil
}

// convertToK8sNetworkPolicy converts our proto NetworkPolicy to a K8s NetworkPolicy
func convertToK8sNetworkPolicy(policy *v1.NetworkPolicy) *networkingv1.NetworkPolicy {
	// Convert proto NetworkPolicy to K8s NetworkPolicy
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      policy.Metadata.Name,
			Namespace: policy.Metadata.Namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: convertToK8sLabelSelector(policy.Spec.PodSelector),
			// Will add more fields here
		},
	}
}

// Helper function to convert proto label selector to K8s label selector
func convertToK8sLabelSelector(selector *v1.LabelSelector) metav1.LabelSelector {
	return metav1.LabelSelector{
		MatchLabels: selector.MatchLabels,
	}
}
