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
		count, exists := fm.cache.GetFlowKey(*flowKey)
		if !exists {
			fm.cache.AddFlowKey(*flowKey, 1, flow.GetFlow())
		} else {
			fm.cache.AddFlowKey(*flowKey, count.Count+1, flow.GetFlow())
		}
	}
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
	}
	return &smartcache.FlowKey{
		SourceIPAddress:      networkFlow.GetIP().GetSource(),
		SourceNamespace:      networkFlow.GetSource().GetNamespace(),
		SourceKind:           networkFlow.GetSource().GetWorkloads()[0].GetKind(),
		SourceName:           networkFlow.GetSource().GetWorkloads()[0].GetName(),
		DestinationIPAddress: networkFlow.GetIP().GetDestination(),
		DestinationNamespace: networkFlow.GetDestination().GetNamespace(),
		DestinationKind:      networkFlow.GetDestination().GetWorkloads()[0].GetKind(),
		DestinationName:      networkFlow.GetDestination().GetWorkloads()[0].GetName(),
		SourcePort:           srcport,
		DestinationPort:      dstport,
		Protocol:             protocol,
		Direction:            networkFlow.GetTrafficDirection().String(),
		Verdict:              networkFlow.GetVerdict().String(),
	}
}
