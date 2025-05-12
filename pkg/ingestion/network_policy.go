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

// convertToK8sNetworkPolicy converts our proto NetworkPolicy back to the
// Kubernetes API object (networking.k8s.io/v1).
func convertToK8sNetworkPolicy(p *v1.NetworkPolicy) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.Metadata.Name,
			Namespace:   p.Metadata.Namespace,
			Labels:      p.Metadata.Labels,
			Annotations: p.Metadata.Annotations,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: convertToK8sLabelSelector(p.Spec.PodSelector),
			PolicyTypes: convertToK8sPolicyTypes(p.Spec.PolicyTypes),
			Ingress:     convertToK8sIngressRules(p.Spec.Ingress),
			Egress:      convertToK8sEgressRules(p.Spec.Egress),
		},
	}
}

/* ---------- helpers ---------- */

func convertToK8sPolicyTypes(types []string) []networkingv1.PolicyType {
	out := make([]networkingv1.PolicyType, 0, len(types))
	for _, t := range types {
		out = append(out, networkingv1.PolicyType(t)) // "Ingress"/"Egress"
	}
	return out
}

func convertToK8sIngressRules(in []*v1.NetworkPolicyIngressRule) []networkingv1.NetworkPolicyIngressRule {
	out := make([]networkingv1.NetworkPolicyIngressRule, 0, len(in))
	for _, r := range in {
		out = append(out, networkingv1.NetworkPolicyIngressRule{
			From:  convertToK8sPeers(r.From),
			Ports: convertToK8sPorts(r.Ports),
		})
	}
	return out
}

func convertToK8sEgressRules(eg []*v1.NetworkPolicyEgressRule) []networkingv1.NetworkPolicyEgressRule {
	out := make([]networkingv1.NetworkPolicyEgressRule, 0, len(eg))
	for _, r := range eg {
		out = append(out, networkingv1.NetworkPolicyEgressRule{
			To:    convertToK8sPeers(r.To),
			Ports: convertToK8sPorts(r.Ports),
		})
	}
	return out
}

func convertToK8sPeers(peers []*v1.NetworkPolicyPeer) []networkingv1.NetworkPolicyPeer {
	out := make([]networkingv1.NetworkPolicyPeer, 0, len(peers))
	for _, p := range peers {
		out = append(out, networkingv1.NetworkPolicyPeer{
			PodSelector:       convertToK8sLabelSelectorPtr(p.PodSelector),
			NamespaceSelector: convertToK8sLabelSelectorPtr(p.NamespaceSelector),
			IPBlock:           convertToK8sIPBlockPtr(p.IpBlock),
		})
	}
	return out
}

func convertToK8sPorts(ports []*v1.NetworkPolicyPort) []networkingv1.NetworkPolicyPort {
	out := make([]networkingv1.NetworkPolicyPort, 0, len(ports))
	for _, pp := range ports {
		var protoPtr *v1.Protocol
		if pp.Protocol != "" {
			p := v1.Protocol(pp.Protocol)
			protoPtr = &p
		}

		k8sPort := networkingv1.NetworkPolicyPort{
			Protocol: protoPtr,
		}

		switch v := pp.Port.(type) {
		case *v1.NetworkPolicyPort_Port:
			port := intstr.FromInt(int(v.Port))
			k8sPort.Port = &port
		case *v1.NetworkPolicyPort_PortName:
			port := intstr.FromString(v.PortName)
			k8sPort.Port = &port
		}

		if pp.EndPort != 0 {
			end := int32(pp.EndPort)
			k8sPort.EndPort = &end
		}
		out = append(out, k8sPort)
	}
	return out
}

/* ----- label / selector helpers ----- */

func convertToK8sLabelSelector(sel *v1.LabelSelector) metav1.LabelSelector {
	if sel == nil {
		return metav1.LabelSelector{}
	}
	return metav1.LabelSelector{
		MatchLabels:      sel.MatchLabels,
		MatchExpressions: convertToK8sLabelExprs(sel.MatchExpressions),
	}
}

func convertToK8sLabelSelectorPtr(sel *v1.LabelSelector) *metav1.LabelSelector {
	if sel == nil {
		return nil
	}
	tmp := convertToK8sLabelSelector(sel)
	return &tmp
}

func convertToK8sLabelExprs(exprs []*v1.LabelSelectorRequirement) []metav1.LabelSelectorRequirement {
	out := make([]metav1.LabelSelectorRequirement, 0, len(exprs))
	for _, e := range exprs {
		out = append(out, metav1.LabelSelectorRequirement{
			Key:      e.Key,
			Operator: metav1.LabelSelectorOperator(e.Operator),
			Values:   e.Values,
		})
	}
	return out
}

/* ---------- IPBlock helper ---------- */

func convertToK8sIPBlockPtr(b *v1.IPBlock) *networkingv1.IPBlock {
	if b == nil {
		return nil
	}
	return &networkingv1.IPBlock{
		CIDR:   b.Cidr,
		Except: b.Except,
	}
