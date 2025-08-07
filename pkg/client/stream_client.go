package client

import (
	"context"
	"crypto/tls"
	"fmt"
	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/auth"
	"operator/pkg/cilium"
	"operator/pkg/ingestion"
	"operator/pkg/k8s_api"
	"operator/pkg/k8s_helper"
	"operator/pkg/shell_executor"
	smartcache "operator/pkg/smart_cache"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	serverv1 "server/api/gen/server/v1"

	"log"

	"github.com/cilium/cilium/api/v1/flow"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	Logger        *zap.Logger
	Client        *grpc.ClientConn
	Config        ServerConfig
	sendMu        sync.Mutex // Protects concurrent stream.Send calls
	apiExecutor   *k8s_api.APIExecutor
	shellExecutor *shell_executor.ShellExecutor
}

const (
	runtimeSecretName    = "kestrel-operator-jwt-runtime"
	tokenRenewalInterval = 3 * time.Hour // Based on 24 hour JWT TTL
)

// protectedSend ensures thread-safe sending on the gRPC stream
func (s *StreamClient) protectedSend(stream v1.StreamService_StreamDataClient, req *v1.StreamDataRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return stream.Send(req)
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

	// Initialize Kubernetes client for API execution
	k8sClient, err := k8s_helper.NewClientSet()
	if err != nil {
		_ = conn.Close()
		logger.Error("Failed to create Kubernetes clientset", zap.Error(err))
		return nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	k8sConfig, err := k8s_helper.NewRestConfig()
	if err != nil {
		_ = conn.Close()
		logger.Error("Failed to create Kubernetes rest config", zap.Error(err))
		return nil, fmt.Errorf("failed to create k8s rest config: %w", err)
	}

	// Initialize API executor
	apiExecutor := k8s_api.NewAPIExecutor(logger, k8sClient, k8sConfig)

	// Initialize shell executor
	shellExecutor := shell_executor.NewShellExecutor(logger)

	return &StreamClient{
		Logger:        logger,
		Client:        conn,
		Config:        config,
		apiExecutor:   apiExecutor,
		shellExecutor: shellExecutor,
	}, nil
}

// LoadConfigFromEnv loads server configuration from environment variables
func LoadConfigFromEnv() (*ServerConfig, error) {
	// Get server configuration from environment variables (set by Helm)
	host := getEnvOrDefault("SERVER_HOST", "auto-np-server")
	portStr := getEnvOrDefault("SERVER_PORT", "50051")
	useTLSStr := getEnvOrDefault("SERVER_USE_TLS", "true")

	// Token loading strategy: Check runtime secret first, fallback to Helm-managed secret
	token, err := loadTokenWithFallback()
	if err != nil {
		return nil, fmt.Errorf("failed to load authentication token: %w", err)
	}

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

// loadTokenWithFallback implements the token loading strategy:
// 1. Check runtime secret first (self-updated tokens)
// 2. Fallback to Helm-managed secret (bootstrap token)
func loadTokenWithFallback() (string, error) {
	// First, try to load from runtime secret, which contains the most recently renewed token
	runtimeToken, err := loadTokenFromSecret(runtimeSecretName)
	if err == nil && runtimeToken != "" {
		log.Printf("Using renewed token from runtime secret: %s", runtimeSecretName)
		return runtimeToken, nil
	}

	// Fallback to Helm-managed initial secret via environment variable
	helmToken := getEnvOrDefault("AUTH_TOKEN", "")
	if helmToken != "" {
		log.Printf("Using initial token from Helm-managed secret")
		return helmToken, nil
	}

	return "", fmt.Errorf("no valid token found in runtime secret or initial Helm-managed secret")
}

// loadTokenFromSecret loads a token from a Kubernetes secret
func loadTokenFromSecret(secretName string) (string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "kestrel-ai" // fallback
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	tokenBytes, exists := secret.Data["token"]
	if !exists {
		return "", fmt.Errorf("token key not found in secret %s", secretName)
	}

	return string(tokenBytes), nil
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

	// Set up workload, namespace, network policy, and service ingestion channels
	workloadChan := make(chan *v1.Workload, 1000)
	namespaceChan := make(chan *v1.Namespace, 100)
	networkPolicyChan := make(chan *v1.NetworkPolicy, 100)
	serviceChan := make(chan *v1.Service, 100)

	// Create network policy ingester with new pattern
	networkPolicyIngester, err := ingestion.NewNetworkPolicyIngester(s.Logger, networkPolicyChan)
	if err != nil {
		s.Logger.Error("Failed to create network policy ingester", zap.Error(err))
		return err
	}

	// Create workload ingester
	workloadIngester, err := ingestion.NewWorkloadIngester(s.Logger, workloadChan)
	if err != nil {
		s.Logger.Error("Failed to create workload ingester", zap.Error(err))
		return err
	}

	// Create namespace ingester
	namespaceIngester, err := ingestion.NewNamespaceIngester(s.Logger, namespaceChan)
	if err != nil {
		s.Logger.Error("Failed to create namespace ingester", zap.Error(err))
		return err
	}

	// Create service ingester
	serviceIngester, err := ingestion.NewServiceIngester(s.Logger, serviceChan)
	if err != nil {
		s.Logger.Error("Failed to create service ingester", zap.Error(err))
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

		// Start sending inventory channel readers before ingesters start
		inventoryDone := make(chan error, 1)
		go s.sendInventoryData(ctx, stream, workloadChan, namespaceChan, networkPolicyChan, serviceChan, inventoryDone)

		// Create channels to signal when initial inventory sync is complete
		workloadSyncDone := make(chan error, 1)
		namespaceSyncDone := make(chan error, 1)
		networkPolicySyncDone := make(chan error, 1)
		serviceSyncDone := make(chan error, 1)

		// Start workload ingester
		workloadCtx, workloadCancel := context.WithCancel(ctx)
		defer workloadCancel()
		go func() {
			if err := workloadIngester.StartSync(workloadCtx, workloadSyncDone); err != nil {
				s.Logger.Error("Workload ingester failed", zap.Error(err))
				workloadSyncDone <- err
			}
		}()

		// Start namespace ingester
		namespaceCtx, namespaceCancel := context.WithCancel(ctx)
		defer namespaceCancel()
		go func() {
			if err := namespaceIngester.StartSync(namespaceCtx, namespaceSyncDone); err != nil {
				s.Logger.Error("Namespace ingester failed", zap.Error(err))
				namespaceSyncDone <- err
			}
		}()

		// Start network policy ingester
		networkPolicyCtx, networkPolicyCancel := context.WithCancel(ctx)
		defer networkPolicyCancel()
		go func() {
			if err := networkPolicyIngester.StartSync(networkPolicyCtx, networkPolicySyncDone); err != nil {
				s.Logger.Error("Network policy ingester failed", zap.Error(err))
				networkPolicySyncDone <- err
			}
		}()

		// Start service ingester
		serviceCtx, serviceCancel := context.WithCancel(ctx)
		defer serviceCancel()
		go func() {
			if err := serviceIngester.StartSync(serviceCtx, serviceSyncDone); err != nil {
				s.Logger.Error("Service ingester failed", zap.Error(err))
				serviceSyncDone <- err
			}
		}()

		// Wait for all ingesters to complete their initial sync
		s.Logger.Info("Waiting for initial inventory sync to complete...")

		// Wait for workload sync
		select {
		case err := <-workloadSyncDone:
			if err != nil {
				s.Logger.Error("Workload initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Workload initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for namespace sync
		select {
		case err := <-namespaceSyncDone:
			if err != nil {
				s.Logger.Error("Namespace initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Namespace initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for network policy sync
		select {
		case err := <-networkPolicySyncDone:
			if err != nil {
				s.Logger.Error("Network policy initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Network policy initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for service sync
		select {
		case err := <-serviceSyncDone:
			if err != nil {
				s.Logger.Error("Service initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Service initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Send inventory commit message to signal that initial inventory is complete
		s.Logger.Info("Sending inventory commit message to server")
		commitMsg := &v1.StreamDataRequest{
			Request: &v1.StreamDataRequest_InventoryCommit{
				InventoryCommit: &v1.InventoryCommit{},
			},
		}
		if err := s.protectedSend(stream, commitMsg); err != nil {
			s.Logger.Error("Failed to send inventory commit message", zap.Error(err))
			return err
		}
		s.Logger.Info("Successfully sent inventory commit message")

		// Start exporting Cilium flows with context
		flowCtx, flowCancel := context.WithCancel(ctx)
		defer flowCancel() // Ensure flowCollector stops if this function exits

		go s.exportCiliumFlows(flowCtx, flowCollector, flowCancel)
		s.Logger.Info("Started exporting Cilium flows")

		// Handle bidirectional streaming (inventory data already being sent via earlier goroutine)
		return s.handleBidirectionalStreamWithFlows(ctx, stream, flowChan, inventoryDone)
	}

	// Use the retry logic to handle stream reconnection
	return WithReconnect(ctx, streamFunc)
}

// setupFlowComponents creates and initializes the flow cache and collector
func (s *StreamClient) setupFlowComponents(ctx context.Context) (chan smartcache.FlowCount, *smartcache.SmartCache, *cilium.FlowCollector, error) {
	// Create a channel for flow data - buffer large enough for one purge-cycle (~5 min) worth of flows
	flowChan := make(chan smartcache.FlowCount, 20_000)

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

// exportCiliumFlows starts the flow collector in a background goroutine
func (s *StreamClient) exportCiliumFlows(ctx context.Context, flowCollector *cilium.FlowCollector, cancel context.CancelFunc) {
	// If ExportCiliumFlows returns, consider it an error and cancel the parent context
	err := flowCollector.ExportCiliumFlows(ctx)
	if err != nil && ctx.Err() == nil {
		s.Logger.Error("Flow collector failed unexpectedly", zap.Error(err))
		cancel() // Cancel this context to signal other components
	}
}

// handleBidirectionalStreamWithFlows manages the bidirectional streaming for flows only
// (inventory data is already being handled by a separate goroutine)
func (s *StreamClient) handleBidirectionalStreamWithFlows(ctx context.Context, stream v1.StreamService_StreamDataClient, flowChan chan smartcache.FlowCount, inventoryDone <-chan error) error {
	// Create a channel to handle stream closure
	done := make(chan error, 1)

	// Start goroutine to handle incoming network policies from server
	go s.handleServerMessages(ctx, stream, done)

	// Start goroutine to collect flow data and send to server
	go s.sendFlowData(ctx, stream, flowChan, done)

	// Start periodic token renewal to ensure tokens are refreshed during long-lived connections
	go s.periodicTokenRenewal(ctx)

	// Monitor the inventory data goroutine for errors
	go func() {
		select {
		case err := <-inventoryDone:
			if err != nil {
				s.Logger.Error("Inventory data sending failed", zap.Error(err))
				select {
				case done <- err:
				default:
				}
			}
		case <-ctx.Done():
			return
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

// handleServerMessages processes incoming messages from the server
func (s *StreamClient) handleServerMessages(ctx context.Context, stream v1.StreamService_StreamDataClient, done chan<- error) {
	k8sClient, err := k8s_helper.NewClientSet()
	if err != nil {
		s.Logger.Error("Failed to create k8s client", zap.Error(err))
		select {
		case done <- err:
		default:
		}
		return
	}

	for {
		select {
		case <-ctx.Done():
			select {
			case done <- ctx.Err():
			default:
			}
			return
		default:
			response, err := stream.Recv()
			if err != nil {
				select {
				case done <- err:
				default:
				}
				return
			}

			// Handle the response based on its type
			switch resp := response.Response.(type) {
			case *v1.StreamDataResponse_Ack:
				// Server ACK is no longer needed for policy application
				s.Logger.Debug("Received acknowledgment from server")
			case *v1.StreamDataResponse_NetworkPolicy:
				s.handleNetworkPolicy(ctx, stream, k8sClient, resp.NetworkPolicy)
			case *v1.StreamDataResponse_KubernetesApiRequest:
				s.handleKubernetesAPIRequest(ctx, stream, resp.KubernetesApiRequest)
			case *v1.StreamDataResponse_ShellCommandRequest:
				s.handleShellCommandRequest(ctx, stream, resp.ShellCommandRequest)
			}
		}
	}
}

// handleKubernetesAPIRequest processes Kubernetes API requests from the server
func (s *StreamClient) handleKubernetesAPIRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	apiRequest *v1.KubernetesAPIRequest,
) {
	s.Logger.Info("Received Kubernetes API request from server",
		zap.String("request_id", apiRequest.RequestId),
		zap.Int("api_paths_count", len(apiRequest.ApiPaths)))

	// Execute the API requests using our API executor
	apiResponse := s.apiExecutor.ExecuteAPIRequests(ctx, apiRequest)

	// Send the response back to the server
	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_KubernetesApiResponse{
			KubernetesApiResponse: apiResponse,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send Kubernetes API response to server",
			zap.String("request_id", apiRequest.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent Kubernetes API response to server",
			zap.String("request_id", apiRequest.RequestId),
			zap.Int("results_count", len(apiResponse.Results)))
	}
}

// handleShellCommandRequest processes shell command requests from the server
func (s *StreamClient) handleShellCommandRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	shellRequest *v1.ShellCommandRequest,
) {
	s.Logger.Info("Received shell command request from server",
		zap.String("request_id", shellRequest.RequestId),
		zap.Int("commands_count", len(shellRequest.Commands)))

	// Execute the shell commands using our shell executor
	shellResponse := s.shellExecutor.ExecuteShellCommands(ctx, shellRequest)

	// Send the response back to the server
	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_ShellCommandResponse{
			ShellCommandResponse: shellResponse,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send shell command response to server",
			zap.String("request_id", shellRequest.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent shell command response to server",
			zap.String("request_id", shellRequest.RequestId),
			zap.Int("results_count", len(shellResponse.Results)))
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
	if err := s.protectedSend(stream, &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_NetworkPolicyWithErrors{
			NetworkPolicyWithErrors: policiesWithErrors,
		},
	}); err != nil {
		s.Logger.Error("Failed to send policy validation errors", zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent policy validation errors to server",
			zap.Int("num_validation_errors", len(policiesWithErrors.Policies)))
	}
}

// sendPolicyValidationAck sends acknowledgment for a valid policy
func (s *StreamClient) sendPolicyValidationAck(stream v1.StreamService_StreamDataClient, policy *v1.NetworkPolicyWithError) {
	s.Logger.Info("Policy validation successful - sending ACK to server")

	// Send ACK back to server to indicate this policy is valid
	if err := s.protectedSend(stream, &v1.StreamDataRequest{
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
				select {
				case done <- fmt.Errorf("flow channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendFlowToServer(stream, flowData); err != nil {
					s.Logger.Error("Failed to send flow data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
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
	return s.protectedSend(stream, flowMsg)
}

// sendInventoryData collects workload and namespace data and sends it to the server
func (s *StreamClient) sendInventoryData(ctx context.Context, stream v1.StreamService_StreamDataClient, workloadChan <-chan *v1.Workload, namespaceChan <-chan *v1.Namespace, networkPolicyChan <-chan *v1.NetworkPolicy, serviceChan <-chan *v1.Service, done chan<- error) {
	for {
		select {
		case <-ctx.Done():
			// Context is done, exit the goroutine
			return
		case workload, ok := <-workloadChan:
			// Check if workload channel was closed
			if !ok {
				s.Logger.Warn("Workload channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("workload channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendWorkloadToServer(stream, workload); err != nil {
					s.Logger.Error("Failed to send workload data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case namespace, ok := <-namespaceChan:
			// Check if namespace channel was closed
			if !ok {
				s.Logger.Warn("Namespace channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("namespace channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendNamespaceToServer(stream, namespace); err != nil {
					s.Logger.Error("Failed to send namespace data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case networkPolicy, ok := <-networkPolicyChan:
			// Check if network policy channel was closed
			if !ok {
				s.Logger.Warn("Network policy channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("network policy channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendNetworkPolicyToServer(stream, networkPolicy); err != nil {
					s.Logger.Error("Failed to send network policy data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case service, ok := <-serviceChan:
			// Check if service channel was closed
			if !ok {
				s.Logger.Warn("Service channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("service channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendServiceToServer(stream, service); err != nil {
					s.Logger.Error("Failed to send service data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		}
	}
}

// sendWorkloadToServer sends a single workload to the server
func (s *StreamClient) sendWorkloadToServer(stream v1.StreamService_StreamDataClient, workload *v1.Workload) error {
	// Convert workload data to proto message and send
	workloadMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Workload{
			Workload: workload,
		},
	}
	return s.protectedSend(stream, workloadMsg)
}

// sendNamespaceToServer sends a single namespace to the server
func (s *StreamClient) sendNamespaceToServer(stream v1.StreamService_StreamDataClient, namespace *v1.Namespace) error {
	// Convert namespace data to proto message and send
	namespaceMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Namespace{
			Namespace: namespace,
		},
	}
	return s.protectedSend(stream, namespaceMsg)
}

// sendNetworkPolicyToServer sends a single network policy to the server
func (s *StreamClient) sendNetworkPolicyToServer(stream v1.StreamService_StreamDataClient, networkPolicy *v1.NetworkPolicy) error {
	// Convert network policy data to proto message and send
	networkPolicyMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_NetworkPolicy{
			NetworkPolicy: networkPolicy,
		},
	}
	return s.protectedSend(stream, networkPolicyMsg)
}

// sendServiceToServer sends a single service to the server
func (s *StreamClient) sendServiceToServer(stream v1.StreamService_StreamDataClient, service *v1.Service) error {
	// Convert service data to proto message and send
	serviceMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Service{
			Service: service,
		},
	}
	return s.protectedSend(stream, serviceMsg)
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

	convertPolicy := func(p *flow.Policy) *v1.Policy {
		return &v1.Policy{
			Name:      p.Name,
			Namespace: p.Namespace,
			Labels:    p.Labels,
			Revision:  p.Revision,
			Kind:      p.Kind,
		}
	}
	ingressAllowedBy := make([]*v1.Policy, 0, len(flowData.FlowMetadata.IngressAllowedBy))
	for _, policy := range flowData.FlowMetadata.IngressAllowedBy {
		ingressAllowedBy = append(ingressAllowedBy, convertPolicy(policy))
	}
	egressAllowedBy := make([]*v1.Policy, 0, len(flowData.FlowMetadata.EgressAllowedBy))
	for _, policy := range flowData.FlowMetadata.EgressAllowedBy {
		egressAllowedBy = append(egressAllowedBy, convertPolicy(policy))
	}

	return &v1.Flow{
		SrcIp: flowKey.SourceIPAddress,
		DstIp: flowKey.DestinationIPAddress,
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
		Direction:        flowKey.Direction,
		Port:             flowKey.DestinationPort,
		Protocol:         flowKey.Protocol,
		Allowed:          flowKey.Verdict == "FORWARDED", // Assuming "FORWARDED" means allowed
		Count:            flowData.Count,
		FirstSeen:        flowData.FlowMetadata.FirstSeen,
		LastSeen:         flowData.FlowMetadata.LastSeen,
		IngressAllowedBy: ingressAllowedBy,
		EgressAllowedBy:  egressAllowedBy,
	}
}

// periodicTokenRenewal handles token renewal every tokenRenewalInterval to ensure fresh tokens during long-lived connections
func (s *StreamClient) periodicTokenRenewal(ctx context.Context) {
	ticker := time.NewTicker(tokenRenewalInterval)
	defer ticker.Stop()

	s.Logger.Info("Starting periodic token renewal", zap.Duration("interval", tokenRenewalInterval))

	for {
		select {
		case <-ctx.Done():
			s.Logger.Info("Stopping periodic token renewal due to context cancellation")
			return
		case <-ticker.C:
			s.Logger.Info("Performing periodic token renewal")

			currentToken := s.Config.Token
			serverClient := serverv1.NewAutonpServerServiceClient(s.Client)

			// Call RenewClusterToken directly
			renewCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, err := serverClient.RenewClusterToken(renewCtx, &serverv1.RenewClusterTokenRequest{
				CurrentToken: currentToken,
			})
			cancel()

			if err != nil {
				s.Logger.Warn("Periodic token renewal failed", zap.Error(err))
				continue
			}

			s.Logger.Info("Token renewal successful, updating runtime secret")

			// Update the runtime secret with the new token, so that if the operator is restarted,
			// it will use the new token in the runtime secret.
			if err := s.updateRuntimeTokenSecret(resp.AccessToken); err != nil {
				s.Logger.Error("Failed to update runtime token secret", zap.Error(err))
			} else {
				s.Logger.Info("Successfully updated runtime token secret")

				// Update config token for the next renewal cycle
				s.Config.Token = resp.AccessToken
			}
		}
	}
}

// updateRuntimeTokenSecret updates the runtime secret with a new token
func (s *StreamClient) updateRuntimeTokenSecret(newToken string) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "kestrel-ai" // fallback
	}

	// Create or update the runtime secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runtimeSecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(newToken),
		},
	}

	// Try to update first, create if it doesn't exist
	_, err = clientset.CoreV1().Secrets(namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	if err != nil {
		// If update failed, try to create
		_, err = clientset.CoreV1().Secrets(namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		s.Logger.Info("Created runtime token secret", zap.String("secretName", runtimeSecretName))
	} else {
		s.Logger.Info("Successfully updated runtime token secret", zap.String("secretName", runtimeSecretName))
	}

	return nil
}
