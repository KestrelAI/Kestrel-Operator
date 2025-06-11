package client

import (
	"context"
	"crypto/tls"
	"fmt"
	v1 "operator/api/cloud/v1"
	"operator/pkg/auth"
	"operator/pkg/cilium"
	"operator/pkg/ingestion"
	"operator/pkg/k8s_helper"
	smartcache "operator/pkg/smart_cache"
	"os"
	"strconv"
	"strings"

	serverv1 "server/api/gen/server/v1"

	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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
func NewStreamClient(ctx context.Context, logger *zap.Logger, config ServerConfig) (*StreamClient, error) {
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

	// Create the connection to the server
	serverAddr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	logger.Info("Connecting to server at", zap.String("serverAddr", serverAddr))
	conn, err := grpc.NewClient(serverAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s: %w", serverAddr, err)
	}

	if config.Token != "" {
		// Build TokenSource backed by RenewClusterToken RPC (implements OAuth2.0 access token renewal flow)
		logger.Info("Onboarding/initial token is not empty, creating per-RPC OAuth2.0 token source")

		authzClient := serverv1.NewAutonpServerServiceClient(conn)
		ts, err := auth.NewTokenSource(ctx, authzClient, config.Token)
		if err != nil {
			_ = conn.Close()
			logger.Error("Failed to create new token source", zap.Error(err))
			return nil, fmt.Errorf("init TokenSource: %w", err)
		}
		perRPC := oauth.TokenSource{TokenSource: oauth2.ReuseTokenSource(nil, ts)}

		conn, err = grpc.NewClient(serverAddr, append(opts, grpc.WithPerRPCCredentials(perRPC))...)
		if err != nil {
			_ = conn.Close()
			logger.Error("Failed to create a new connection to the server with the per-RPC token source", zap.Error(err))
			return nil, fmt.Errorf("dial secured: %w", err)
		}
	}

	return &StreamClient{
		Logger: logger,
		Client: conn,
		Config: config,
	}, nil
}

// LoadConfigFromEnv loads server configuration from environment variables
func LoadConfigFromEnv() (*ServerConfig, error) {
	// Get server configuration from environment variables (set by Helm)
	host := getEnvOrDefault("SERVER_HOST", "auto-np-server")
	portStr := getEnvOrDefault("SERVER_PORT", "50051")
	useTLSStr := getEnvOrDefault("SERVER_USE_TLS", "true")
	token := getEnvOrDefault("AUTH_TOKEN", "")

	if token == "" {
		return nil, fmt.Errorf("onboarding jwt token secret does not exist")
	}

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

	return &ServerConfig{
		Host:   host,
		Port:   port,
		UseTLS: useTLS,
		Token:  token,
	}, nil
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

	// Create flow components
	flowChan, _, flowCollector, err := s.setupFlowComponents(ctx)
	if err != nil {
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
		// Create stream with tenant context
		stream, ctx, err := s.createStreamWithTenantContext(ctx, streamClient)
		if err != nil {
			return err
		}

		// Send initial network policies
		if err := s.sendInitialNetworkPolicies(ctx, stream, networkPolicyIngester); err != nil {
			return err
		}

		// Start exporting Cilium flows with context
		flowCtx, flowCancel := context.WithCancel(ctx)
		defer flowCancel() // Ensure flowCollector stops if this function exits

		go s.exportCiliumFlows(flowCtx, flowCollector, flowCancel)
		s.Logger.Info("Started exporting Cilium flows")

		// Handle bidirectional streaming
		return s.handleBidirectionalStream(ctx, stream, flowChan)
	}

	// Use the retry logic to handle stream reconnection
	return WithReconnect(ctx, streamFunc)
}

// setupFlowComponents creates and initializes the flow cache and collector
func (s *StreamClient) setupFlowComponents(ctx context.Context) (chan smartcache.FlowCount, *smartcache.SmartCache, *cilium.FlowCollector, error) {
	// Create a channel for flow data - buffer large enough for one purge-cycle (~60 s) worth of flows
	flowChan := make(chan smartcache.FlowCount, 10_000)

	// Initialize flow cache
	cache := smartcache.InitFlowCache(ctx, flowChan)

	// Set up the flow collector to monitor Cilium flows
	flowCollector, err := cilium.NewFlowCollector(ctx, s.Logger, "kube-system", cache)
	if err != nil {
		s.Logger.Error("Failed to create flow collector", zap.Error(err))
		return nil, nil, nil, err
	}

	return flowChan, cache, flowCollector, nil
}

// extractTenantFromToken extracts the tenant ID from a JWT token
func extractTenantFromToken(tokenString string) (string, error) {
	// Parse the token without verification to extract claims
	claims := jwt.MapClaims{}
	_, _, err := new(jwt.Parser).ParseUnverified(tokenString, claims)
	if err != nil {
		return "", fmt.Errorf("failed to parse JWT token: %w", err)
	}

	// Extract tenant from claims
	tenant, ok := claims["tenant"].(string)
	if !ok || tenant == "" {
		return "", fmt.Errorf("tenant claim not found or empty in JWT token")
	}

	return tenant, nil
}

// createStreamWithTenantContext creates a new stream with tenant context
func (s *StreamClient) createStreamWithTenantContext(ctx context.Context, streamClient v1.StreamServiceClient) (v1.StreamService_StreamDataClient, context.Context, error) {
	tenantID, err := extractTenantFromToken(s.Config.Token)
	if err != nil {
		s.Logger.Error("Failed to extract tenant ID from JWT token", zap.Error(err))
		return nil, nil, fmt.Errorf("failed to extract tenant from token: %w", err)
	}

	s.Logger.Info("Using tenant ID from JWT token", zap.String("tenantID", tenantID))

	// Add tenant ID to the context metadata
	md := metadata.New(map[string]string{
		"autonp-tenantid": tenantID,
	})
	ctxWithMetadata := metadata.NewOutgoingContext(ctx, md)

	// Start the bidirectional stream
	stream, err := streamClient.StreamData(ctxWithMetadata)
	if err != nil {
		s.Logger.Error("Failed to establish stream", zap.Error(err))
		return nil, nil, err
	}

	return stream, ctxWithMetadata, nil
}

// sendInitialNetworkPolicies fetches and sends existing network policies to the server
func (s *StreamClient) sendInitialNetworkPolicies(ctx context.Context, stream v1.StreamService_StreamDataClient, networkPolicyIngester *ingestion.NetworkPolicyIngester) error {
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

		targetWorkloads := networkPolicyIngester.ResolveTargetWorkloads(ctx, policy)

		// Convert to proto message and send
		policyMsg := &v1.StreamDataRequest{
			Request: &v1.StreamDataRequest_NetworkPolicy{
				NetworkPolicy: convertToProtoNetworkPolicy(policy, targetWorkloads),
			},
		}
		if err := stream.Send(policyMsg); err != nil {
			s.Logger.Error("Failed to send network policy", zap.Error(err))
			return err
		}
	}
	s.Logger.Info("Sent initial network policies to server", zap.Int("count", len(policies)))
	return nil
}

// exportCiliumFlows starts the flow collector in a background goroutine
func (s *StreamClient) exportCiliumFlows(ctx context.Context, flowCollector *cilium.FlowCollector, cancel context.CancelFunc) {
	// If ExportCiliumFlows returns, consider it an error and cancel the parent context
	err := flowCollector.ExportCiliumFlows(ctx)
	if err != nil && ctx.Err() == nil {
		s.Logger.Error("Flow collector failed unexpectedly", zap.Error(err))
		cancel() // Cancel this context to signal other components
	}
}

// handleBidirectionalStream manages the bidirectional streaming between client and server
func (s *StreamClient) handleBidirectionalStream(ctx context.Context, stream v1.StreamService_StreamDataClient, flowChan chan smartcache.FlowCount) error {
	// Create a channel to handle stream closure
	done := make(chan error, 1)

	// Start goroutine to handle incoming network policies from server
	go s.handleServerMessages(ctx, stream, done)

	// Start goroutine to collect flow data and send to server
	go s.sendFlowData(ctx, stream, flowChan, done)

	// Wait for stream to end or context cancellation
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleServerMessages processes incoming messages from the server
func (s *StreamClient) handleServerMessages(ctx context.Context, stream v1.StreamService_StreamDataClient, done chan<- error) {
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
			response, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}

			// Handle the response based on its type
			switch resp := response.Response.(type) {
			case *v1.StreamDataResponse_Ack:
				// Server ACK is no longer needed for policy application
				s.Logger.Debug("Received acknowledgment from server")
			case *v1.StreamDataResponse_NetworkPolicy:
				s.handleNetworkPolicy(ctx, stream, k8sClient, resp.NetworkPolicy)
			}
		}
	}
}

// handleNetworkPolicy validates network policies from the server and sends appropriate responses
func (s *StreamClient) handleNetworkPolicy(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	k8sClient *kubernetes.Clientset,
	policy *v1.NetworkPolicyWithError,
) {
	s.Logger.Info("Received network policy from server", zap.String("policy_id", policy.PolicyId))
	s.Logger.Info("Network policy YAML", zap.String("yaml", policy.NetworkPolicy))
	// Check if network policy is correct and can be applied
	err := ingestion.CheckNetworkPolicy(s.Logger, policy.NetworkPolicy)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			s.Logger.Info("Network policy already exists, skipping", zap.String("policy_id", policy.PolicyId))
			s.sendPolicyValidationAck(stream, policy)
			return
		}
		s.handleInvalidPolicy(stream, policy, err)
		return
	}

	// Policy is valid - apply it immediately
	policyParsed, err := ingestion.ParseNetworkPolicyYAML(s.Logger, policy.NetworkPolicy)
	if err != nil {
		s.Logger.Error("Failed to parse network policy YAML", zap.Error(err))
		s.handleInvalidPolicy(stream, policy, err)
		return
	}

	// Apply the valid policy directly
	err = ingestion.ApplyNetworkPolicy(ctx, k8sClient, policyParsed)
	if err != nil {
		s.Logger.Error("Failed to apply network policy", zap.Error(err))
		// Send back as an invalid policy with the application error
		s.handleInvalidPolicy(stream, policy, err)
		return
	}

	s.Logger.Info("Successfully applied network policy from server",
		zap.String("name", policyParsed.Name),
		zap.String("namespace", policyParsed.Namespace),
		zap.String("policy_id", policy.PolicyId))

	// Send ACK back to server to indicate this policy was valid and applied
	s.sendPolicyValidationAck(stream, policy)
}

// handleInvalidPolicy sends error feedback for invalid policies
func (s *StreamClient) handleInvalidPolicy(
	stream v1.StreamService_StreamDataClient,
	policy *v1.NetworkPolicyWithError,
	validationErr error,
) {
	s.Logger.Error("Failed to check network policy", zap.Error(validationErr))

	// Create error policy structure
	failedNetworkPolicy := &v1.NetworkPolicyWithError{
		NetworkPolicy: policy.NetworkPolicy,
		ErrorMessage:  validationErr.Error(),
		PolicyId:      policy.PolicyId, // Preserve the policy_id field
	}

	// Send the error immediately
	policiesWithErrors := &v1.NetworkPoliciesWithErrors{
		Policies: []*v1.NetworkPolicyWithError{failedNetworkPolicy},
	}

	// Send error back to server
	if err := stream.Send(&v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_NetworkPolicyWithErrors{
			NetworkPolicyWithErrors: policiesWithErrors,
		},
	}); err != nil {
		s.Logger.Error("Failed to send policy validation errors", zap.Error(err))
	} else {
		s.Logger.Debug("Successfully sent policy validation errors to server")
	}
}

// sendPolicyValidationAck sends acknowledgment for a valid policy
func (s *StreamClient) sendPolicyValidationAck(stream v1.StreamService_StreamDataClient, policy *v1.NetworkPolicyWithError) {
	s.Logger.Info("Policy validation successful - sending ACK to server")

	// Send ACK back to server to indicate this policy is valid
	if err := stream.Send(&v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_NetworkPolicyWithErrors{
			NetworkPolicyWithErrors: &v1.NetworkPoliciesWithErrors{
				Policies: []*v1.NetworkPolicyWithError{
					{
						NetworkPolicy: policy.NetworkPolicy,
						ErrorMessage:  "",
						PolicyId:      policy.PolicyId, // Preserve the policy_id field
					},
				},
			},
		},
	}); err != nil {
		s.Logger.Error("Failed to send policy validation ACK", zap.Error(err))
	}
}

// sendFlowData collects flow data and sends it to the server
func (s *StreamClient) sendFlowData(ctx context.Context, stream v1.StreamService_StreamDataClient, flowChan <-chan smartcache.FlowCount, done chan<- error) {
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
				if err := s.sendFlowToServer(stream, flowData); err != nil {
					s.Logger.Error("Failed to send flow data", zap.Error(err))
					done <- err
					return
				}
			}
		}
	}
}

// sendFlowToServer sends a single flow to the server
func (s *StreamClient) sendFlowToServer(stream v1.StreamService_StreamDataClient, flowData smartcache.FlowCount) error {
	// Convert flow data to proto message and send
	flowMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Flow{
			Flow: convertToProtoFlow(flowData),
		},
	}
	return stream.Send(flowMsg)
}

// convertToProtoNetworkPolicy converts a native K8s NetworkPolicy
// (k8s.io/api/networking/v1) to our cloud.v1 proto representation.
func convertToProtoNetworkPolicy(np networkingv1.NetworkPolicy, targetWorkloads []string) *v1.NetworkPolicy {
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
		TargetWorkloads: targetWorkloads,
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

	// Convert map[string]struct{} labels to []string
	srcLabels := make([]string, 0, len(flowData.FlowMetadata.SourceLabels))
	for label := range flowData.FlowMetadata.SourceLabels {
		srcLabels = append(srcLabels, label)
	}
	dstLabels := make([]string, 0, len(flowData.FlowMetadata.DestLabels))
	for label := range flowData.FlowMetadata.DestLabels {
		dstLabels = append(dstLabels, label)
	}

	return &v1.Flow{
		Src: &v1.Endpoint{
			Ns:     flowKey.SourceNamespace,
			Kind:   flowKey.SourceKind,
			Name:   flowKey.SourceName,
			Labels: srcLabels,
		},
		Dst: &v1.Endpoint{
			Ns:     flowKey.DestinationNamespace,
			Kind:   flowKey.DestinationKind,
			Name:   flowKey.DestinationName,
			Labels: dstLabels,
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
