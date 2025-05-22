package cilium

import (
	"context"
	"errors"
	"fmt"

	"operator/pkg/k8s_helper"

	smartcache "operator/pkg/smart_cache"

	flow "github.com/cilium/cilium/api/v1/flow"
	observer "github.com/cilium/cilium/api/v1/observer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	ciliumHubbleRelayMaxFlowCount uint64 = 100
	ciliumHubbleRelayServiceName  string = "hubble-relay"
)

type FlowCollector struct {
	logger *zap.Logger
	client observer.ObserverClient
	cache  *smartcache.SmartCache
}

// discoverCiliumHubbleRelayAddress uses a kubernetes clientset in order to discover the address of the hubble-relay service within kube-system.
func discoverCiliumHubbleRelayAddress(ctx context.Context, ciliumNamespace string, clientset kubernetes.Interface) (string, error) {
	service, err := clientset.CoreV1().Services(ciliumNamespace).Get(ctx, ciliumHubbleRelayServiceName, metav1.GetOptions{})
	if err != nil {
		return "", errors.New("failed to get hubble-relay service")
	}

	if len(service.Spec.Ports) == 0 {
		return "", errors.New("no ports found in hubble-relay service")
	}

	address := fmt.Sprintf("%s:%d", service.Spec.ClusterIP, service.Spec.Ports[0].Port)
	return address, nil
}

// newCiliumCollector connects to Ciilium Hubble Relay, sets up an Observer client, and returns a new Collector using it.
func NewFlowCollector(ctx context.Context, logger *zap.Logger, ciliumNamespace string, cache *smartcache.SmartCache) (*FlowCollector, error) {
	config, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create new client set: %w", err)
	}
	hubbleAddress, err := discoverCiliumHubbleRelayAddress(ctx, ciliumNamespace, config)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(hubbleAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cilium Hubble Relay: %w", err)
	}
	hubbleClient := observer.NewObserverClient(conn)
	return &FlowCollector{logger: logger, client: hubbleClient, cache: cache}, nil
}

// exportCiliumFlows makes one stream gRPC call to hubble-relay to collect, convert, and export flows into the given stream.
func (fm *FlowCollector) ExportCiliumFlows(ctx context.Context) error {
	req := &observer.GetFlowsRequest{
		Number: ciliumHubbleRelayMaxFlowCount,
		Follow: true,
	}
	observerClient := fm.client
	stream, err := observerClient.GetFlows(ctx, req)
	if err != nil {
		fm.logger.Error("Error getting network flows", zap.Error(err))
		return err
	}
	defer func() {
		err = stream.CloseSend()
		if err != nil {
			fm.logger.Error("Error closing observerClient stream", zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		flow, err := stream.Recv()
		if err != nil {
			fm.logger.Warn("Failed to get flow log from stream", zap.Error(err))
			return err
		}

		// Skip reply-direction packets; they never need a NetworkPolicy rule.
		// K8s NetworkPolicy is state-aware: once the policy allows the initiating packet,
		// all packets in the reverse / reply direction of that connection are automatically accepted.
		// We only write policies for the request direction (reply traffic matches conntrack and is allowed implicitly).
		if ir := flow.GetFlow().GetIsReply(); ir != nil && ir.GetValue() {
			continue
		}

		flowKey := createFlowKey(flow.GetFlow())

		// Convert string slice labels to map[string]struct{}
		srcLabels := make(map[string]struct{})
		dstLabels := make(map[string]struct{})
		for _, label := range flow.GetFlow().GetSource().GetLabels() {
			srcLabels[label] = struct{}{}
		}
		for _, label := range flow.GetFlow().GetDestination().GetLabels() {
			dstLabels[label] = struct{}{}
		}

		flowMetadata := &smartcache.FlowMetadata{
			FirstSeen:    &timestamppb.Timestamp{},
			LastSeen:     &timestamppb.Timestamp{},
			SourceLabels: srcLabels,
			DestLabels:   dstLabels,
		}
		fm.cache.AddFlowKey(*flowKey, flow.GetFlow(), flowMetadata)
	}
}

// getEndpointNameAndKind safely extracts the name and kind from an endpoint
func getEndpointNameAndKind(endpoint *flow.Endpoint) (name, kind string) {
	if len(endpoint.GetWorkloads()) > 0 {
		return endpoint.GetWorkloads()[0].GetName(), endpoint.GetWorkloads()[0].GetKind()
	} else if endpoint.GetPodName() != "" {
		return endpoint.GetPodName(), "Pod"
	} else if len(endpoint.GetLabels()) > 0 {
		return endpoint.GetLabels()[0], "Unmanaged"
	} else if endpoint.GetIdentity() != 0 {
		return fmt.Sprintf("identity-%d", endpoint.GetIdentity()), "Unknown"
	}
	return "unknown", "Unknown"
}

// createFlowKey creates a FlowKey from a Cilium flow
func createFlowKey(networkFlow *flow.Flow) *smartcache.FlowKey {
	var protocol string
	var srcport, dstport uint32

	switch l4 := networkFlow.GetL4().GetProtocol().(type) {
	case *flow.Layer4_TCP:
		protocol = "TCP"
		srcport = l4.TCP.GetSourcePort()
		dstport = l4.TCP.GetDestinationPort()
	case *flow.Layer4_UDP:
		protocol = "UDP"
		srcport = l4.UDP.GetSourcePort()
		dstport = l4.UDP.GetDestinationPort()
	case *flow.Layer4_SCTP:
		protocol = "SCTP"
		srcport = l4.SCTP.GetSourcePort()
		dstport = l4.SCTP.GetDestinationPort()
	case *flow.Layer4_ICMPv4:
		protocol = "ICMPv4"
		// ICMPv4 doesn't have ports
	case *flow.Layer4_ICMPv6:
		protocol = "ICMPv6"
		// ICMPv6 doesn't have ports
	}

	// Use the helper to extract name and kind
	srcName, srcKind := getEndpointNameAndKind(networkFlow.GetSource())
	dstName, dstKind := getEndpointNameAndKind(networkFlow.GetDestination())

	return &smartcache.FlowKey{
		SourceIPAddress:      networkFlow.GetIP().GetSource(),
		SourceNamespace:      networkFlow.GetSource().GetNamespace(),
		SourceKind:           srcKind,
		SourceName:           srcName,
		DestinationIPAddress: networkFlow.GetIP().GetDestination(),
		DestinationNamespace: networkFlow.GetDestination().GetNamespace(),
		DestinationKind:      dstKind,
		DestinationName:      dstName,
		SourcePort:           srcport,
		DestinationPort:      dstport,
		Protocol:             protocol,
		Direction:            networkFlow.GetTrafficDirection().String(),
		Verdict:              networkFlow.GetVerdict().String(),
	}
}
