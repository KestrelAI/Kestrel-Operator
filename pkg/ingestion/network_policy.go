package ingestion

import (
	"context"
	"fmt"
	"log"

	"operator/pkg/k8s_helper"

	networkingv1 "k8s.io/api/networking/v1"
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
	// Get all namespaces
	namespaces, err := npi.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %v", err)
	}

	var allPolicies []networkingv1.NetworkPolicy

	// Iterate through each namespace and get network policies
	for _, namespace := range namespaces.Items {
		policies, err := npi.clientset.NetworkingV1().NetworkPolicies(namespace.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("Warning: failed to list network policies in namespace %s: %v", namespace.Name, err)
			continue
		}

		allPolicies = append(allPolicies, policies.Items...)
	}

	return allPolicies, nil
}

// GetNetworkPolicy fetches a specific network policy by name and namespace
func (npi *NetworkPolicyIngester) GetNetworkPolicy(ctx context.Context, namespace, name string) (*networkingv1.NetworkPolicy, error) {
	return npi.clientset.NetworkingV1().NetworkPolicies(namespace).Get(ctx, name, metav1.GetOptions{})
}
