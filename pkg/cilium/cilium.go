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
		flowKey := createFlowKey(flow.GetFlow())
		flowMetadata := &smartcache.FlowMetadata{FirstSeen: nil, LastSeen: nil, SourceLabels: flow.GetFlow().GetSource().Labels, DestLabels: flow.GetFlow().GetDestination().Labels}
		fm.cache.AddFlowKey(*flowKey, flow.GetFlow(), flowMetadata)
	}
}

// getSafeWorkloadInfo safely extracts kind and name from a Cilium endpoint
// Falls back to pod_name if available when workloads slice is empty
func getSafeWorkloadInfo(endpoint *flow.Endpoint) (kind, name string) {
	if endpoint == nil {
		return "Unknown", "unknown"
	}

	workloads := endpoint.GetWorkloads()
	if len(workloads) > 0 {
		return workloads[0].GetKind(), workloads[0].GetName()
	}

	if podName := endpoint.GetPodName(); podName != "" {
		// Fall back to pod_name if workloads is empty
		return "Pod", podName
	}

	// Unidentified endpoint
	return "Unknown", "unknown"
}

// createFlowKey creates a FlowKey struct from a Cilium flow
func createFlowKey(networkFlow *flow.Flow) *smartcache.FlowKey {
	var protocol string
	var srcport, dstport uint32

	switch networkFlow.GetL4().GetProtocol().(type) {
	case *flow.Layer4_TCP:
		protocol = "TCP"
		srcport = networkFlow.GetL4().GetTCP().GetSourcePort()
		dstport = networkFlow.GetL4().GetTCP().GetDestinationPort()
	case *flow.Layer4_UDP:
		protocol = "UDP"
		srcport = networkFlow.GetL4().GetUDP().GetSourcePort()
		dstport = networkFlow.GetL4().GetUDP().GetDestinationPort()
	case *flow.Layer4_ICMPv4:
		protocol = "ICMP"
		// ICMPv4 does not have ports
	case *flow.Layer4_SCTP:
		protocol = "SCTP"
		srcport = networkFlow.GetL4().GetSCTP().GetSourcePort()
		dstport = networkFlow.GetL4().GetSCTP().GetDestinationPort()
	default:
		return &smartcache.FlowKey{}
	}
	// Get source and destination workload info safely
	sourceKind, sourceName := getSafeWorkloadInfo(networkFlow.GetSource())
	destKind, destName := getSafeWorkloadInfo(networkFlow.GetDestination())

	return &smartcache.FlowKey{
		SourceIPAddress:      networkFlow.GetIP().GetSource(),
		SourceNamespace:      networkFlow.GetSource().GetNamespace(),
		SourceKind:           sourceKind,
		SourceName:           sourceName,
		DestinationIPAddress: networkFlow.GetIP().GetDestination(),
		DestinationNamespace: networkFlow.GetDestination().GetNamespace(),
		DestinationKind:      destKind,
		DestinationName:      destName,
		SourcePort:           srcport,
		DestinationPort:      dstport,
		Protocol:             protocol,
		Direction:            networkFlow.GetTrafficDirection().String(),
		Verdict:              networkFlow.GetVerdict().String(),
	}
}
