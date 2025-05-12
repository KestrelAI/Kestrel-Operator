package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"

	v1 "operator/api/cloud/v1"
	"operator/pkg/cilium"
	"operator/pkg/ingestion"
	"operator/pkg/k8s_helper"
	smartcache "operator/pkg/smart_cache"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServerConfig holds the configuration for connecting to the server
type ServerConfig struct {
	Host   string
	Port   int
	UseTLS bool
	Token  string
}

// StreamClient is the client for streaming data to and from the server
type StreamClient struct {
	Logger *zap.Logger
	Client *grpc.ClientConn
	Config ServerConfig
}

// NewStreamClient creates a new StreamClient with the given configuration
func NewStreamClient(logger *zap.Logger, config ServerConfig) (*StreamClient, error) {
	var opts []grpc.DialOption
	var creds credentials.TransportCredentials

	// Set up credentials based on TLS configuration
	if config.UseTLS {
		// Use TLS credentials with skip verification since we're in a cluster
		// For production, you'd want to use proper CA certificates
		creds = credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false,
		})
		logger.Info("Using TLS with InsecureSkipVerify=true")
	} else {
		// Use insecure credentials
		creds = credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		logger.Info("Using insecure credentials (no TLS)")
	}
	opts = append(opts, grpc.WithTransportCredentials(creds))

	// TODO: Add JWT token-based authentication if needed
	// if config.Token != "" {
	//     opts = append(opts, grpc.WithPerRPCCredentials(...))
	// }

	// Create the connection to the server

	serverAddr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	logger.Info("Connecting to server at", zap.String("serverAddr", serverAddr))
	conn, err := grpc.NewClient(serverAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s: %w", serverAddr, err)
	}

	return &StreamClient{
		Logger: logger,
		Client: conn,
		Config: config,
	}, nil
}

// LoadConfigFromEnv loads server configuration from environment variables
func LoadConfigFromEnv() ServerConfig {
	// Get server configuration from environment variables (set by Helm)
	host := getEnvOrDefault("SERVER_HOST", "auto-np-server")
	portStr := getEnvOrDefault("SERVER_PORT", "50051")
	useTLSStr := getEnvOrDefault("SERVER_USE_TLS", "true")
	token := getEnvOrDefault("AUTH_TOKEN", "")

	// Parse port as integer
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 50051 // Default port if parsing fails
	}

	// Parse useTLS as boolean
	useTLS := true
	if useTLSStr == "false" {
		useTLS = false
	}

	return ServerConfig{
		Host:   host,
		Port:   port,
		UseTLS: useTLS,
		Token:  token,
	}
}

// Helper function to get environment variable with default value
func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// startOperator starts the operator and begins to stream data to the server and listens to cilium flows.
func (s *StreamClient) StartOperator(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel for flow data
	flowChan := make(chan smartcache.FlowCount)

	// Initialize flow cache
	cache := smartcache.InitFlowCache(ctx, flowChan)

	// Set up the flow collector to monitor Cilium flows
	flowCollector, err := cilium.NewFlowCollector(ctx, s.Logger, "kube-system", cache)
	if err != nil {
		s.Logger.Error("Failed to create flow collector", zap.Error(err))
		return err
	}

	// Set up network policy ingestion
	networkPolicyIngester, err := ingestion.NewNetworkPolicyIngester()
	if err != nil {
		s.Logger.Error("Failed to create network policy ingester", zap.Error(err))
		return err
	}

	// Create a new stream service client
	streamClient := v1.NewStreamServiceClient(s.Client)

	// Define the stream function that will be retried
	streamFunc := func(ctx context.Context) error {
		// Add tenant ID to the context metadata
		// Temp until we implement multi-tenancy
		md := metadata.New(map[string]string{
			"autonp-tenantid": "default",
		})
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Start the bidirectional stream
		stream, err := streamClient.StreamData(ctx)
		if err != nil {
			s.Logger.Error("Failed to establish stream", zap.Error(err))
			return err
		}

		// First, fetch and send current network policies
		s.Logger.Info("Fetching current network policies from the cluster")
		policies, err := networkPolicyIngester.IngestNetworkPolicies(ctx)
		if err != nil {
			s.Logger.Error("Failed to ingest network policies", zap.Error(err))
			return err
		}

		// Send initial network policies to the server
		for _, policy := range policies {
			// Check context before each send
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Convert to proto message and send
			policyMsg := &v1.StreamDataRequest{
				Request: &v1.StreamDataRequest_NetworkPolicy{
					NetworkPolicy: convertToProtoNetworkPolicy(policy),
				},
			}
			if err := stream.Send(policyMsg); err != nil {
				s.Logger.Error("Failed to send network policy", zap.Error(err))
				return err
			}
		}
		s.Logger.Info("Sent initial network policies to server", zap.Int("count", len(policies)))

		// Start exporting Cilium flows with context
		flowCtx, flowCancel := context.WithCancel(ctx)
		defer flowCancel() // Ensure flowCollector stops if this function exits

		go func() {
			// If ExportCiliumFlows returns, consider it an error and cancel the parent context
			err := flowCollector.ExportCiliumFlows(flowCtx)
			if err != nil && flowCtx.Err() == nil {
				s.Logger.Error("Flow collector failed unexpectedly", zap.Error(err))
				flowCancel() // Cancel this context to signal other components
			}
		}()
		s.Logger.Info("Started exporting Cilium flows")

		// Create a channel to handle stream closure
		done := make(chan error, 1)

		// Start goroutine to handle incoming messages (network policies from server)
		go func() {
			k8sClient, err := k8s_helper.NewClientSet()
			if err != nil {
				s.Logger.Error("Failed to create k8s client", zap.Error(err))
				done <- err
				return
			}
			for {
				select {
				case <-ctx.Done():
					done <- ctx.Err()
					return
				default:
					// Attempt to receive with a timeout to allow checking context
					// This is a non-blocking receive attempt
					response, err := stream.Recv()
					if err != nil {
						done <- err
						return
					}

					// Handle the response based on its type
					switch resp := response.Response.(type) {
					case *v1.StreamDataResponse_Ack:
						s.Logger.Debug("Received acknowledgment from server")
					case *v1.StreamDataResponse_NetworkPolicy:
						s.Logger.Info("Received network policy from server", zap.String("name", resp.NetworkPolicy.String()))

						// Apply the network policy with context
						err := ingestion.ApplyNetworkPolicy(ctx, k8sClient, resp.NetworkPolicy)
						if err != nil {
							s.Logger.Error("Failed to apply network policy", zap.Error(err))
							// Continue processing other messages, don't fail the whole stream
						} else {
							s.Logger.Info("Successfully applied network policy from server")
						}
					}
				}
			}
		}()

		// Start goroutine to collect flow data and send to server
		go func() {
			for {
				select {
				case <-ctx.Done():
					// Context is done, exit the goroutine
					return
				case flowData, ok := <-flowChan:
					// Check if channel was closed
					if !ok {
						s.Logger.Warn("Flow channel closed unexpectedly")
						done <- fmt.Errorf("flow channel closed")
						return
					}

					// Check context again before sending
					select {
					case <-ctx.Done():
						return
					default:
						fmt.Println("Sending flow data to server")
						// Convert flow data to proto message and send
						flowMsg := &v1.StreamDataRequest{
							Request: &v1.StreamDataRequest_Flow{
								Flow: convertToProtoFlow(flowData),
							},
						}
						if err := stream.Send(flowMsg); err != nil {
							s.Logger.Error("Failed to send flow data", zap.Error(err))
							done <- err
							return
						}
					}
				}
			}
		}()

		// Wait for stream to end or context cancellation
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Use the retry logic to handle stream reconnection
	return WithReconnect(ctx, streamFunc)
}

// convertToProtoNetworkPolicy converts a native K8s NetworkPolicy
// (k8s.io/api/networking/v1) to our cloud.v1 proto representation.
func convertToProtoNetworkPolicy(np networkingv1.NetworkPolicy) *v1.NetworkPolicy {
	return &v1.NetworkPolicy{
		Metadata: &v1.ObjectMeta{
			Name:        np.Name,
			Namespace:   np.Namespace,
			Labels:      np.Labels,      // maybe unnecessary
			Annotations: np.Annotations, // maybe unnecessary
		},
		Spec: &v1.NetworkPolicySpec{
			PodSelector: convertLabelSelector(np.Spec.PodSelector),
			Ingress:     convertIngressRules(np.Spec.Ingress),
			Egress:      convertEgressRules(np.Spec.Egress),
			PolicyTypes: convertPolicyTypes(np.Spec.PolicyTypes),
		},
	}
}

// ---------- helpers ----------

func convertPolicyTypes(t []networkingv1.PolicyType) []string {
	out := make([]string, 0, len(t))
	for _, pt := range t {
		out = append(out, string(pt)) // "Ingress" / "Egress"
	}
	return out
}

func convertIngressRules(rules []networkingv1.NetworkPolicyIngressRule) []*v1.NetworkPolicyIngressRule {
	out := make([]*v1.NetworkPolicyIngressRule, 0, len(rules))
	for _, r := range rules {
		out = append(out, &v1.NetworkPolicyIngressRule{
			From:  convertPeers(r.From),
			Ports: convertPorts(r.Ports),
		})
	}
	return out
}

func convertEgressRules(rules []networkingv1.NetworkPolicyEgressRule) []*v1.NetworkPolicyEgressRule {
	out := make([]*v1.NetworkPolicyEgressRule, 0, len(rules))
	for _, r := range rules {
		out = append(out, &v1.NetworkPolicyEgressRule{
			To:    convertPeers(r.To),
			Ports: convertPorts(r.Ports),
		})
	}
	return out
}

func convertPeers(peers []networkingv1.NetworkPolicyPeer) []*v1.NetworkPolicyPeer {
	out := make([]*v1.NetworkPolicyPeer, 0, len(peers))
	for _, p := range peers {
		out = append(out, &v1.NetworkPolicyPeer{
			PodSelector:       convertLabelSelectorPtr(p.PodSelector),
			NamespaceSelector: convertLabelSelectorPtr(p.NamespaceSelector),
			IpBlock:           convertIPBlockPtr(p.IPBlock),
		})
	}
	return out
}

func convertPorts(ports []networkingv1.NetworkPolicyPort) []*v1.NetworkPolicyPort {
	out := make([]*v1.NetworkPolicyPort, 0, len(ports))
	for _, p := range ports {
		pp := &v1.NetworkPolicyPort{}
		if p.Protocol != nil {
			pp.Protocol = string(*p.Protocol)
		}
		if p.Port != nil {
			if intPort := p.Port.IntVal; intPort != 0 {
				pp.PortValue = &v1.NetworkPolicyPort_Port{Port: int32(intPort)}
			} else {
				pp.PortValue = &v1.NetworkPolicyPort_PortName{PortName: p.Port.StrVal}
			}
		}
		if p.EndPort != nil {
			pp.EndPort = *p.EndPort
		}
		out = append(out, pp)
	}
	return out
}

func convertLabelSelector(ls metav1.LabelSelector) *v1.LabelSelector {
	return &v1.LabelSelector{
		MatchLabels:      ls.MatchLabels,
		MatchExpressions: convertLabelExprs(ls.MatchExpressions),
	}
}

func convertLabelSelectorPtr(ls *metav1.LabelSelector) *v1.LabelSelector {
	if ls == nil {
		return nil
	}
	return convertLabelSelector(*ls)
}

func convertLabelExprs(exprs []metav1.LabelSelectorRequirement) []*v1.LabelSelectorRequirement {
	out := make([]*v1.LabelSelectorRequirement, 0, len(exprs))
	for _, e := range exprs {
		out = append(out, &v1.LabelSelectorRequirement{
			Key:      e.Key,
			Operator: string(e.Operator),
			Values:   e.Values,
		})
	}
	return out
}

func convertIPBlockPtr(block *networkingv1.IPBlock) *v1.IPBlock {
	if block == nil {
		return nil
	}
	return &v1.IPBlock{
		Cidr:   block.CIDR,
		Except: block.Except,
	}
}

// convertToProtoFlow converts a FlowCount to our proto Flow
func convertToProtoFlow(flowData smartcache.FlowCount) *v1.Flow {
	// Extract data from FlowKey
	flowKey := flowData.FlowKey

	return &v1.Flow{
		Src: &v1.Endpoint{
			Ns:     flowKey.SourceNamespace,
			Kind:   flowKey.SourceKind,
			Name:   flowKey.SourceName,
			Labels: flowData.FlowMetadata.SourceLabels,
		},
		Dst: &v1.Endpoint{
			Ns:     flowKey.DestinationNamespace,
			Kind:   flowKey.DestinationKind,
			Name:   flowKey.DestinationName,
			Labels: flowData.FlowMetadata.DestLabels,
		},
		Direction: flowKey.Direction,
		Port:      flowKey.DestinationPort,
		Protocol:  flowKey.Protocol,
		Allowed:   flowKey.Verdict == "FORWARDED", // Assuming "FORWARDED" means allowed
		Count:     flowData.Count,
		FirstSeen: flowData.FlowMetadata.FirstSeen,
		LastSeen:  flowData.FlowMetadata.LastSeen,
	}
}
