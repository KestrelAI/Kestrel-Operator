package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/auth"
	"operator/pkg/cilium"
	"operator/pkg/ingestion"
	"operator/pkg/k8s_api"
	"operator/pkg/k8s_helper"
	"operator/pkg/metrics_store"
	"operator/pkg/otel_receiver"
	"operator/pkg/shell_executor"
	smartcache "operator/pkg/smart_cache"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	serverv1 "operator/api/gen/server/v1"

	"log"

	"github.com/cilium/cilium/api/v1/flow"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sInformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"operator/pkg/envoy_als"
	"sync/atomic"
)

var _ = appsv1.Deployment{}

// ServerConfig holds the configuration for connecting to the server
type ServerConfig struct {
	Host  string
	Port  int
	Token string
	// mTLS configuration (always enabled)
	ClientCertFile string
	ClientKeyFile  string
	CACertFile     string
	ServerName     string // For SNI and certificate verification
}

// StreamClient is the client for streaming data to and from the server
type StreamClient struct {
	Logger        *zap.Logger
	Client        *grpc.ClientConn
	Config        ServerConfig
	sendMu        sync.Mutex // Protects concurrent stream.Send calls
	apiExecutor   *k8s_api.APIExecutor
	shellExecutor *shell_executor.ShellExecutor

	// OTEL Metrics Store for local metrics storage and querying
	metricsStore *metrics_store.MetricsStore
	podResolver  *metrics_store.PodWorkloadResolver
	otelReceiver *otel_receiver.OTelReceiverServer

	// Health tracking fields for liveness probe
	streamHealthy   int64 // atomic boolean (1 = healthy, 0 = unhealthy)
	lastHealthyTime int64 // atomic unix timestamp of last healthy operation
	eofErrorCount   int64 // atomic counter for consecutive EOF errors
	// New fields for persistent EOF tracking
	totalEOFErrors   int64 // atomic counter for total EOF errors in time window
	eofTrackingStart int64 // atomic unix timestamp when EOF tracking started
	lastEOFTime      int64 // atomic unix timestamp of last EOF error
	healthMu         sync.RWMutex
	lastError        error // last error encountered (protected by healthMu)
}

const (
	runtimeSecretName    = "kestrel-operator-jwt-runtime"
	tokenRenewalInterval = 3 * time.Hour // Based on 24 hour JWT TTL

	defaultHubbleRelayNamespace     = "kube-system"
	dataplaneV2HubbleRelayNamespace = "gke-managed-dpv2-observability"

	// Constants for liveness probe
	minEOFErrorsForLiveness  = 3                // Minimum EOF errors for liveness failure
	minUnhealthyForLiveness  = 1 * time.Minute  // Minimum unhealthy duration for liveness failure
	maxHealthyGapForLiveness = 30 * time.Second // Max gap between EOFs for liveness failure
)

// protectedSend ensures thread-safe sending on the gRPC stream
func (s *StreamClient) protectedSend(stream v1.StreamService_StreamDataClient, req *v1.StreamDataRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	err := stream.Send(req)

	// Update health status based on send result
	if err != nil {
		s.recordStreamError(err)
	} else {
		s.recordStreamHealthy()
	}

	return err
}

// recordStreamHealthy marks the stream as healthy
func (s *StreamClient) recordStreamHealthy() {
	atomic.StoreInt64(&s.streamHealthy, 1)
	atomic.StoreInt64(&s.lastHealthyTime, time.Now().Unix())

	// Only reset EOF counters if we've been healthy for a sustained period (5 minutes)
	// This prevents resetting counters immediately after reconnection
	lastHealthyTime := atomic.LoadInt64(&s.lastHealthyTime)
	eofTrackingStart := atomic.LoadInt64(&s.eofTrackingStart)

	const healthyResetThreshold = 5 * time.Minute

	// If we have EOF tracking data and we've been healthy for long enough, reset EOF counters
	if eofTrackingStart > 0 && lastHealthyTime > 0 {
		timeSinceEOFTracking := time.Unix(lastHealthyTime, 0).Sub(time.Unix(eofTrackingStart, 0))
		if timeSinceEOFTracking > healthyResetThreshold {
			atomic.StoreInt64(&s.eofErrorCount, 0)
			atomic.StoreInt64(&s.totalEOFErrors, 0)
			atomic.StoreInt64(&s.eofTrackingStart, 0)
			atomic.StoreInt64(&s.lastEOFTime, 0)
		}
	}

	s.healthMu.Lock()
	s.lastError = nil
	s.healthMu.Unlock()
}

// recordStreamError records a stream error and updates health status
func (s *StreamClient) recordStreamError(err error) {
	atomic.StoreInt64(&s.streamHealthy, 0)

	s.healthMu.Lock()
	s.lastError = err
	s.healthMu.Unlock()

	// Check if this is an EOF error and increment counters
	if err != nil && (strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection closed")) {
		now := time.Now().Unix()
		atomic.StoreInt64(&s.lastEOFTime, now)

		// Initialize EOF tracking if this is the first EOF error
		if atomic.LoadInt64(&s.eofTrackingStart) == 0 {
			atomic.StoreInt64(&s.eofTrackingStart, now)
		}

		// Increment both consecutive and total EOF counters
		atomic.AddInt64(&s.eofErrorCount, 1)
		atomic.AddInt64(&s.totalEOFErrors, 1)

		eofCount := atomic.LoadInt64(&s.eofErrorCount)
		totalEOFCount := atomic.LoadInt64(&s.totalEOFErrors)

		s.Logger.Warn("EOF error detected on gRPC stream",
			zap.Error(err),
			zap.Int64("consecutive_eof_count", eofCount),
			zap.Int64("total_eof_count", totalEOFCount))
	} else {
		// Non-EOF error, reset consecutive EOF counter but keep total tracking
		atomic.StoreInt64(&s.eofErrorCount, 0)
	}
}

// IsStreamHealthy returns true if the stream is currently healthy
func (s *StreamClient) IsStreamHealthy() bool {
	return atomic.LoadInt64(&s.streamHealthy) == 1
}

// GetStreamHealthInfo returns detailed health information for debugging
func (s *StreamClient) GetStreamHealthInfo() (bool, time.Time, int64, error) {
	healthy := atomic.LoadInt64(&s.streamHealthy) == 1
	lastHealthyUnix := atomic.LoadInt64(&s.lastHealthyTime)
	lastHealthyTime := time.Unix(lastHealthyUnix, 0)
	eofCount := atomic.LoadInt64(&s.eofErrorCount)

	s.healthMu.RLock()
	lastErr := s.lastError
	s.healthMu.RUnlock()

	return healthy, lastHealthyTime, eofCount, lastErr
}

// GetDetailedStreamHealthInfo returns comprehensive health information including persistent EOF tracking
func (s *StreamClient) GetDetailedStreamHealthInfo() (bool, time.Time, int64, int64, time.Time, time.Time, error) {
	healthy := atomic.LoadInt64(&s.streamHealthy) == 1
	lastHealthyUnix := atomic.LoadInt64(&s.lastHealthyTime)
	lastHealthyTime := time.Unix(lastHealthyUnix, 0)
	eofCount := atomic.LoadInt64(&s.eofErrorCount)
	totalEOFCount := atomic.LoadInt64(&s.totalEOFErrors)
	eofTrackingStartUnix := atomic.LoadInt64(&s.eofTrackingStart)
	eofTrackingStart := time.Unix(eofTrackingStartUnix, 0)
	lastEOFTimeUnix := atomic.LoadInt64(&s.lastEOFTime)
	lastEOFTime := time.Unix(lastEOFTimeUnix, 0)

	s.healthMu.RLock()
	lastErr := s.lastError
	s.healthMu.RUnlock()

	return healthy, lastHealthyTime, eofCount, totalEOFCount, eofTrackingStart, lastEOFTime, lastErr
}

// IsStreamUnhealthyForLiveness returns true if the stream should trigger a liveness probe failure
func (s *StreamClient) IsStreamUnhealthyForLiveness() bool {
	totalEOFErrors := atomic.LoadInt64(&s.totalEOFErrors)
	eofTrackingStartUnix := atomic.LoadInt64(&s.eofTrackingStart)
	lastEOFTimeUnix := atomic.LoadInt64(&s.lastEOFTime)

	// No EOF errors recorded, stream is healthy for liveness
	if totalEOFErrors == 0 || eofTrackingStartUnix == 0 {
		return false
	}

	now := time.Now()
	eofTrackingStart := time.Unix(eofTrackingStartUnix, 0)
	lastEOFTime := time.Unix(lastEOFTimeUnix, 0)

	// Calculate time since EOF tracking started
	timeSinceEOFTrackingStarted := now.Sub(eofTrackingStart)
	timeSinceLastEOF := now.Sub(lastEOFTime)

	// Check if we have enough EOF errors to indicate a persistent problem
	if totalEOFErrors < minEOFErrorsForLiveness {
		return false
	}

	// Check if we've been unhealthy long enough to make a liveness determination
	if timeSinceEOFTrackingStarted < minUnhealthyForLiveness {
		return false
	}

	// Check if the last EOF was recent enough to consider the loop active
	if timeSinceLastEOF > maxHealthyGapForLiveness {
		return false
	}

	s.Logger.Warn("Stream unhealthy for liveness probe",
		zap.Int64("total_eof_errors", totalEOFErrors),
		zap.Duration("unhealthy_duration", timeSinceEOFTrackingStarted),
		zap.Duration("time_since_last_eof", timeSinceLastEOF))

	return true
}

// SimulateEOF simulates EOF errors for testing purposes
func (s *StreamClient) SimulateEOF() {
	s.Logger.Warn("Simulating EOF errors for testing")

	// Create a mock EOF error
	eofErr := fmt.Errorf("rpc error: code = Unavailable desc = EOF")

	// Simulate multiple EOF errors to trigger EOF loop detection
	for i := 0; i < 8; i++ {
		s.recordStreamError(eofErr)
		// Add small delay between errors to simulate real conditions
		time.Sleep(10 * time.Millisecond)
	}

	// Set EOF tracking start time to more than 5 minutes ago to trigger EOF loop
	atomic.StoreInt64(&s.eofTrackingStart, time.Now().Add(-8*time.Minute).Unix())
	// Set last EOF time to recent to keep loop active
	atomic.StoreInt64(&s.lastEOFTime, time.Now().Unix())

	s.Logger.Warn("Simulated EOF loop condition",
		zap.Int64("total_eof_count", atomic.LoadInt64(&s.totalEOFErrors)),
		zap.Int64("consecutive_eof_count", atomic.LoadInt64(&s.eofErrorCount)),
		zap.Bool("unhealthy_for_liveness", s.IsStreamUnhealthyForLiveness()))
}

// NewStreamClient creates a new StreamClient with the given configuration
func NewStreamClient(ctx context.Context, logger *zap.Logger, config ServerConfig) (*StreamClient, error) {
	var opts []grpc.DialOption
	var creds credentials.TransportCredentials

	// Always use mTLS for server authentication
	// Load client certificate and key
	logger.Info("Loading client certificates for mTLS",
		zap.String("client_cert_file", config.ClientCertFile),
		zap.String("client_key_file", config.ClientKeyFile))

	clientCert, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
	if err != nil {
		logger.Error("Failed to load client certificate",
			zap.Error(err),
			zap.String("client_cert_file", config.ClientCertFile),
			zap.String("client_key_file", config.ClientKeyFile))
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	// Log certificate details for debugging
	if len(clientCert.Certificate) > 0 {
		cert, err := x509.ParseCertificate(clientCert.Certificate[0])
		if err == nil {
			logger.Info("Loaded client certificate successfully",
				zap.String("subject", cert.Subject.String()),
				zap.Time("expires_at", cert.NotAfter),
				zap.String("serial_number", cert.SerialNumber.String()))
		}
	}

	// For server certificate verification, we need to use the system CA bundle
	// which includes Let's Encrypt and other public CAs. The custom CA certificate
	// provided in config.CACertFile is for mTLS client certificate verification.
	var caCertPool *x509.CertPool

	// Start with system CA bundle for server certificate verification
	caCertPool, err = x509.SystemCertPool()
	if err != nil {
		logger.Warn("Failed to load system CA pool, using empty pool", zap.Error(err))
		caCertPool = x509.NewCertPool()
	}

	// If a custom CA certificate is provided, add it to the pool
	if config.CACertFile != "" {
		caCertData, err := os.ReadFile(config.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		if !caCertPool.AppendCertsFromPEM(caCertData) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		logger.Info("Added CA certificate to cert pool", zap.String("ca_cert_file", config.CACertFile))
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		ServerName:   config.ServerName,
	}

	creds = credentials.NewTLS(tlsConfig)
	logger.Info("Using mTLS with client certificate authentication",
		zap.String("client_cert", config.ClientCertFile),
		zap.String("server_name", config.ServerName))
	opts = append(opts, grpc.WithTransportCredentials(creds))

	// Add keepalive parameters for long-lived streams (24 hours)
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                5 * time.Minute,  // Send pings every 5 minutes during idle
		Timeout:             30 * time.Second, // Wait 30 seconds for ping response
		PermitWithoutStream: true,             // Send pings even without active streams
	}))

	maxMsgSize := 10 * 1024 * 1024 // 10MB
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(maxMsgSize),
		grpc.MaxCallSendMsgSize(maxMsgSize),
	))

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

	// Initialize metrics store, pod resolver, and OTEL receiver if enabled
	var metricsStoreInstance *metrics_store.MetricsStore
	var otelReceiverInstance *otel_receiver.OTelReceiverServer
	var podResolver *metrics_store.PodWorkloadResolver
	if getEnvOrDefault("ENABLE_OTEL_METRICS_STORE", "false") == "true" {
		// Parse configuration from environment
		retention := parseMetricsRetention()
		maxSeries := parseMetricsMaxSeries()
		ringSize := parseMetricsRingSize()
		otelPort := parseOTelReceiverPort()

		// Initialize pod resolver only when metrics are enabled
		podResolver = metrics_store.NewPodWorkloadResolver(logger)

		metricsStoreInstance = metrics_store.New(
			logger,
			podResolver,
			metrics_store.WithRetention(retention),
			metrics_store.WithMaxSeries(maxSeries),
			metrics_store.WithRingBufferSize(ringSize),
		)
		logger.Info("OTEL metrics store initialized",
			zap.Duration("retention", retention),
			zap.Int("max_series", maxSeries),
			zap.Int("ring_size", ringSize))

		// Create OTEL receiver (will be started later via StartOTelReceiver)
		otelReceiverInstance = otel_receiver.NewOTelReceiverServer(
			logger,
			metricsStoreInstance,
			otelPort,
		)
		logger.Info("OTEL receiver created",
			zap.Int("port", otelPort))
	} else {
		logger.Info("OTEL metrics store disabled")
	}

	return &StreamClient{
		Logger:        logger,
		Client:        conn,
		Config:        config,
		apiExecutor:   apiExecutor,
		shellExecutor: shellExecutor,
		metricsStore:  metricsStoreInstance,
		podResolver:   podResolver,
		otelReceiver:  otelReceiverInstance,
		// Initialize health tracking - start as unhealthy until first successful operation
		streamHealthy:    0,
		lastHealthyTime:  0,
		eofErrorCount:    0,
		totalEOFErrors:   0,
		eofTrackingStart: 0,
		lastEOFTime:      0,
	}, nil
}

// LoadConfigFromEnv loads server configuration from environment variables
func LoadConfigFromEnv() (*ServerConfig, error) {
	// Get server configuration from environment variables (set by Helm)
	host := getEnvOrDefault("SERVER_HOST", "auto-np-server")
	portStr := getEnvOrDefault("SERVER_PORT", "50051")

	// mTLS configuration
	clientCertFile := getEnvOrDefault("CLIENT_CERT_FILE", "/tls/client.crt")
	clientKeyFile := getEnvOrDefault("CLIENT_KEY_FILE", "/tls/client.key")
	caCertFile := getEnvOrDefault("CA_CERT_FILE", "/tls/ca.crt")
	serverName := getEnvOrDefault("SERVER_NAME", host)

	// Log certificate file paths for debugging
	log.Printf("Certificate file paths: cert=%s, key=%s, ca=%s", clientCertFile, clientKeyFile, caCertFile)

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

	return &ServerConfig{
		Host:           host,
		Port:           port,
		Token:          token,
		ClientCertFile: clientCertFile,
		ClientKeyFile:  clientKeyFile,
		CACertFile:     caCertFile,
		ServerName:     serverName,
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

// parseMetricsRetention parses the METRICS_STORE_RETENTION environment variable.
func parseMetricsRetention() time.Duration {
	retentionStr := getEnvOrDefault("METRICS_STORE_RETENTION", "30m")
	retention, err := time.ParseDuration(retentionStr)
	if err != nil {
		return metrics_store.DefaultRetentionDuration
	}
	return retention
}

// parseMetricsMaxSeries parses the METRICS_STORE_MAX_SERIES environment variable.
func parseMetricsMaxSeries() int {
	maxSeriesStr := getEnvOrDefault("METRICS_STORE_MAX_SERIES", "100000")
	maxSeries, err := strconv.Atoi(maxSeriesStr)
	if err != nil || maxSeries <= 0 {
		return metrics_store.DefaultMaxSeries
	}
	return maxSeries
}

// parseMetricsRingSize parses the METRICS_STORE_RING_SIZE environment variable.
func parseMetricsRingSize() int {
	ringSizeStr := getEnvOrDefault("METRICS_STORE_RING_SIZE", "60")
	ringSize, err := strconv.Atoi(ringSizeStr)
	if err != nil || ringSize <= 0 {
		return metrics_store.DefaultRingBufferSize
	}
	return ringSize
}

// parseOTelReceiverPort parses the OTEL_RECEIVER_PORT environment variable.
func parseOTelReceiverPort() int {
	portStr := getEnvOrDefault("OTEL_RECEIVER_PORT", "4317")
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return 4317 // Default OTLP gRPC port
	}
	return port
}

// startOperator starts the operator and begins to stream data to the server and listens to cilium flows.
func (s *StreamClient) StartOperator(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create flow components
	flowChan, _, flowCollector, l7FlowChan, l7Cache, err := s.setupFlowComponents(ctx)
	if err != nil {
		return err
	}

	// Ensure cleanup of L7 cache on shutdown
	defer l7Cache.Stop()

	// Start metrics store eviction goroutine if enabled
	s.StartMetricsEviction(ctx)

	// Start OTEL receiver for metrics ingestion if enabled
	if err := s.StartOTelReceiver(ctx); err != nil {
		s.Logger.Error("Failed to start OTEL receiver, continuing without metrics ingestion", zap.Error(err))
	}

	// Set up workload, namespace, network policy, service, and authorization policy ingestion channels
	workloadChan := make(chan *v1.Workload, 1000)
	namespaceChan := make(chan *v1.Namespace, 100)
	networkPolicyChan := make(chan *v1.NetworkPolicy, 100)
	serviceChan := make(chan *v1.Service, 100)
	authorizationPolicyChan := make(chan *v1.AuthorizationPolicy, 100)
	podChan := make(chan *v1.Pod, 2000) // Larger buffer for pods
	nodeChan := make(chan *v1.Node, 100)

	// Set up incident detection channels
	eventChan := make(chan *v1.KubernetesEvent, 500)               // Kubernetes events
	podStatusChan := make(chan *v1.PodStatusChange, 500)           // Pod status changes
	nodeConditionChan := make(chan *v1.NodeConditionChange, 100)   // Node condition changes
	rolloutStatusChan := make(chan *v1.WorkloadRolloutStatus, 200) // Workload rollout status
	podLogsChan := make(chan *v1.PodLogs, 1000)                    // Pod logs (larger buffer for log batches)

	// Create a new stream service client
	streamClient := v1.NewStreamServiceClient(s.Client)

	// Define the stream function that will be retried
	streamFunc := func(ctx context.Context) error {
		// Create a shared clientset and informer factory for all standard ingesters.
		// SharedInformerFactories cannot be restarted after stopping,
		// so we must create new ones on each reconnection.
		s.Logger.Info("Creating shared clientset and informer factory for this connection")

		sharedClientset, err := k8s_helper.NewClientSet()
		if err != nil {
			s.Logger.Error("Failed to create shared kubernetes clientset", zap.Error(err))
			return err
		}
		sharedFactory := k8sInformers.NewSharedInformerFactory(sharedClientset, 30*time.Second)

		// Create network policy ingester (only if Cilium flows are enabled)
		var networkPolicyIngester *ingestion.NetworkPolicyIngester
		disableCilium := getEnvOrDefault("DISABLE_CILIUM_FLOWS", "false")
		if disableCilium != "true" {
			networkPolicyIngester = ingestion.NewNetworkPolicyIngester(s.Logger, networkPolicyChan, sharedClientset, sharedFactory)
			s.Logger.Info("Network policy ingester created")
		} else {
			s.Logger.Info("Network policy ingester disabled (Cilium flows disabled)")
		}

		// Create workload ingester
		workloadIngester := ingestion.NewWorkloadIngester(s.Logger, workloadChan, sharedClientset, sharedFactory)

		// Create namespace ingester
		namespaceIngester := ingestion.NewNamespaceIngester(s.Logger, namespaceChan, sharedClientset, sharedFactory)

		// Create service ingester
		serviceIngester := ingestion.NewServiceIngester(s.Logger, serviceChan, sharedClientset, sharedFactory)

		// Create pod ingester
		podIngester := ingestion.NewPodIngester(s.Logger, podChan, sharedClientset, sharedFactory)

		// Create node ingester (for VPC Flow Logs node IP resolution)
		nodeIngester := ingestion.NewNodeIngester(s.Logger, nodeChan, sharedClientset, sharedFactory)

		// Create authorization policy ingester (only if Istio is enabled)
		// Note: AuthorizationPolicyIngester uses its own DynamicSharedInformerFactory
		// for Istio CRDs, so it is NOT included in the shared factory.
		var authorizationPolicyIngester *ingestion.AuthorizationPolicyIngester
		enableIstioALS := getEnvOrDefault("ENABLE_ISTIO_ALS", "false")
		if enableIstioALS == "true" {
			var err error
			authorizationPolicyIngester, err = ingestion.NewAuthorizationPolicyIngester(s.Logger, authorizationPolicyChan)
			if err != nil {
				s.Logger.Error("Failed to create authorization policy ingester", zap.Error(err))
				return err
			}
			s.Logger.Info("Authorization policy ingester created")
		} else {
			s.Logger.Info("Authorization policy ingester disabled (Istio ALS not enabled)")
		}

		// Create incident detection ingesters
		eventIngester := ingestion.NewEventIngester(s.Logger, eventChan, sharedClientset, sharedFactory)
		podStatusMonitor := ingestion.NewPodStatusMonitor(s.Logger, podStatusChan, sharedClientset, sharedFactory)
		nodeConditionMonitor := ingestion.NewNodeConditionMonitor(s.Logger, nodeConditionChan, sharedClientset, sharedFactory)
		rolloutMonitor := ingestion.NewWorkloadRolloutMonitor(s.Logger, rolloutStatusChan, sharedClientset, sharedFactory)
		podLogStreamer := ingestion.NewPodLogStreamer(s.Logger, podLogsChan, sharedClientset, sharedFactory)

		s.Logger.Info("All ingesters created successfully for this connection")

		// Create stream with tenant context
		stream, ctx, err := s.createStreamWithTenantContext(ctx, streamClient)
		if err != nil {
			return err
		}

		// Start sending inventory channel readers before ingesters start
		inventoryDone := make(chan error, 1)
		go s.sendInventoryData(ctx, stream, workloadChan, namespaceChan, networkPolicyChan, serviceChan, authorizationPolicyChan, podChan, nodeChan, inventoryDone)

		// Start sending incident detection data (separate from inventory)
		incidentDone := make(chan error, 1)
		go s.sendIncidentData(ctx, stream, eventChan, podStatusChan, nodeConditionChan, rolloutStatusChan, podLogsChan, incidentDone)

		// Create channels to signal when initial inventory sync is complete
		workloadSyncDone := make(chan error, 1)
		namespaceSyncDone := make(chan error, 1)
		networkPolicySyncDone := make(chan error, 1)
		serviceSyncDone := make(chan error, 1)
		authorizationPolicySyncDone := make(chan error, 1)
		podSyncDone := make(chan error, 1)
		nodeSyncDone := make(chan error, 1)

		// Create channels for incident detection sync
		eventSyncDone := make(chan error, 1)
		podStatusSyncDone := make(chan error, 1)
		nodeConditionSyncDone := make(chan error, 1)
		rolloutSyncDone := make(chan error, 1)
		podLogsSyncDone := make(chan error, 1)

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

		// Start pod ingester
		podCtx, podCancel := context.WithCancel(ctx)
		defer podCancel()
		go func() {
			if err := podIngester.StartSync(podCtx, podSyncDone); err != nil {
				s.Logger.Error("Pod ingester failed", zap.Error(err))
				podSyncDone <- err
			}
		}()

		// Start node ingester
		nodeCtx, nodeCancel := context.WithCancel(ctx)
		defer nodeCancel()
		go func() {
			if err := nodeIngester.StartSync(nodeCtx, nodeSyncDone); err != nil {
				s.Logger.Error("Node ingester failed", zap.Error(err))
				nodeSyncDone <- err
			}
		}()

		// Start network policy ingester (only if Cilium flows are enabled)
		var networkPolicyCtx context.Context
		var networkPolicyCancel context.CancelFunc
		if networkPolicyIngester != nil {
			networkPolicyCtx, networkPolicyCancel = context.WithCancel(ctx)
			defer networkPolicyCancel()
			go func() {
				if err := networkPolicyIngester.StartSync(networkPolicyCtx, networkPolicySyncDone); err != nil {
					s.Logger.Error("Network policy ingester failed", zap.Error(err))
					networkPolicySyncDone <- err
				}
			}()
		} else {
			// If network policy ingester is disabled, signal completion immediately
			go func() {
				networkPolicySyncDone <- nil
			}()
		}

		// Start service ingester
		serviceCtx, serviceCancel := context.WithCancel(ctx)
		defer serviceCancel()
		go func() {
			if err := serviceIngester.StartSync(serviceCtx, serviceSyncDone); err != nil {
				s.Logger.Error("Service ingester failed", zap.Error(err))
				serviceSyncDone <- err
			}
		}()

		// Start authorization policy ingester (only if Istio is enabled)
		var authorizationPolicyCtx context.Context
		var authorizationPolicyCancel context.CancelFunc
		if authorizationPolicyIngester != nil {
			authorizationPolicyCtx, authorizationPolicyCancel = context.WithCancel(ctx)
			defer authorizationPolicyCancel()
			go func() {
				if err := authorizationPolicyIngester.StartSync(authorizationPolicyCtx, authorizationPolicySyncDone); err != nil {
					s.Logger.Error("Authorization policy ingester failed", zap.Error(err))
					authorizationPolicySyncDone <- err
				}
			}()
		} else {
			// If authorization policy ingester is disabled, signal completion immediately
			go func() {
				authorizationPolicySyncDone <- nil
			}()
		}

		// Start incident detection ingesters
		s.Logger.Info("Starting incident detection ingesters")

		// Event ingester
		eventCtx, eventCancel := context.WithCancel(ctx)
		defer eventCancel()
		go func() {
			if err := eventIngester.StartSync(eventCtx, eventSyncDone); err != nil {
				s.Logger.Error("Event ingester failed", zap.Error(err))
				eventSyncDone <- err
			}
		}()

		// Pod status monitor
		podStatusCtx, podStatusCancel := context.WithCancel(ctx)
		defer podStatusCancel()
		go func() {
			if err := podStatusMonitor.StartSync(podStatusCtx, podStatusSyncDone); err != nil {
				s.Logger.Error("Pod status monitor failed", zap.Error(err))
				podStatusSyncDone <- err
			}
		}()

		// Node condition monitor
		nodeConditionCtx, nodeConditionCancel := context.WithCancel(ctx)
		defer nodeConditionCancel()
		go func() {
			if err := nodeConditionMonitor.StartSync(nodeConditionCtx, nodeConditionSyncDone); err != nil {
				s.Logger.Error("Node condition monitor failed", zap.Error(err))
				nodeConditionSyncDone <- err
			}
		}()

		// Workload rollout monitor
		rolloutCtx, rolloutCancel := context.WithCancel(ctx)
		defer rolloutCancel()
		go func() {
			if err := rolloutMonitor.StartSync(rolloutCtx, rolloutSyncDone); err != nil {
				s.Logger.Error("Workload rollout monitor failed", zap.Error(err))
				rolloutSyncDone <- err
			}
		}()

		// Pod log streamer
		podLogsCtx, podLogsCancel := context.WithCancel(ctx)
		defer podLogsCancel()
		go func() {
			if err := podLogStreamer.StartSync(podLogsCtx, podLogsSyncDone); err != nil {
				s.Logger.Error("Pod log streamer failed", zap.Error(err))
				podLogsSyncDone <- err
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

		// Wait for authorization policy sync
		select {
		case err := <-authorizationPolicySyncDone:
			if err != nil {
				s.Logger.Error("Authorization policy initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Authorization policy initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for incident detection ingesters to complete initial sync
		s.Logger.Info("Waiting for incident detection ingesters to complete initial sync...")

		// Wait for event sync
		select {
		case err := <-eventSyncDone:
			if err != nil {
				s.Logger.Error("Event ingester initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Event ingester initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for pod status sync
		select {
		case err := <-podStatusSyncDone:
			if err != nil {
				s.Logger.Error("Pod status monitor initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Pod status monitor initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for node condition sync
		select {
		case err := <-nodeConditionSyncDone:
			if err != nil {
				s.Logger.Error("Node condition monitor initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Node condition monitor initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for rollout monitor sync
		select {
		case err := <-rolloutSyncDone:
			if err != nil {
				s.Logger.Error("Workload rollout monitor initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Workload rollout monitor initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		// Wait for pod log streamer sync
		select {
		case err := <-podLogsSyncDone:
			if err != nil {
				s.Logger.Error("Pod log streamer initial sync failed", zap.Error(err))
				return err
			}
			s.Logger.Info("Pod log streamer initial sync completed")
		case <-ctx.Done():
			return ctx.Err()
		}

		s.Logger.Info("All incident detection ingesters initialized successfully")

		// Start the shared informer factory and wait for all caches to sync.
		// All ingesters have registered their event handlers at this point
		// (syncDone signals ensure setupXxxInformer methods completed).
		s.Logger.Info("Starting shared informer factory")
		sharedFactory.Start(ctx.Done())

		s.Logger.Info("Waiting for shared informer caches to sync...")
		syncResults := sharedFactory.WaitForCacheSync(ctx.Done())
		for informerType, synced := range syncResults {
			if !synced {
				s.Logger.Error("Failed to sync informer cache", zap.String("type", informerType.String()))
				return fmt.Errorf("failed to sync shared informer cache for %v", informerType)
			}
		}
		s.Logger.Info("All shared informer caches synced successfully")

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

		// Start exporting Cilium flows with context (only if Cilium is available)
		if flowCollector != nil {
			flowCtx, flowCancel := context.WithCancel(ctx)
			defer flowCancel() // Ensure flowCollector stops if this function exits

			go s.exportCiliumFlows(flowCtx, flowCollector, flowCancel)
			s.Logger.Info("Started exporting Cilium flows")
		} else {
			s.Logger.Info("Cilium flow collection disabled, continuing without network flow data")
		}

		// Handle bidirectional streaming (inventory and incident data already being sent via earlier goroutines)
		return s.handleBidirectionalStreamWithFlows(ctx, stream, flowChan, inventoryDone, incidentDone, l7FlowChan)
	}

	// Use the retry logic to handle stream reconnection
	return WithReconnect(ctx, streamFunc)
}

// setupFlowComponents creates and initializes the flow cache and collector
func (s *StreamClient) setupFlowComponents(ctx context.Context) (chan smartcache.FlowCount, *smartcache.SmartCache, *cilium.FlowCollector, chan smartcache.L7Flow, *smartcache.L7SmartCache, error) {
	// Create a channel for L3/L4 flow data - buffer large enough for one purge-cycle (~5 min) worth of flows
	flowChan := make(chan smartcache.FlowCount, 20_000)

	// Initialize L3/L4 flow cache
	cache := smartcache.InitFlowCache(ctx, flowChan)

	// Create L7 flow channel and cache
	l7FlowChan := make(chan smartcache.L7Flow, 10_000)
	l7Cache := smartcache.InitL7FlowCache(ctx, l7FlowChan)

	// Check if Cilium flow collection is explicitly disabled
	disableCilium := getEnvOrDefault("DISABLE_CILIUM_FLOWS", "false")
	if disableCilium == "true" {
		s.Logger.Info("Cilium flow collection explicitly disabled via DISABLE_CILIUM_FLOWS environment variable")

		// Still set up Istio ALS even without Cilium
		s.setupIstioALSIfEnabled(ctx, l7Cache)

		return flowChan, cache, nil, l7FlowChan, l7Cache, nil
	}

	// Try to set up the flow collector to monitor Cilium flows
	// Check kube-system first, then fallback to gke-managed-dpv2-observability for GKE Dataplane V2
	var flowCollector *cilium.FlowCollector
	var err error

	// First try kube-system namespace
	flowCollector, err = cilium.NewFlowCollector(ctx, s.Logger, defaultHubbleRelayNamespace, cache)
	if err != nil {
		s.Logger.Info("Hubble relay not found in kube-system, trying gke-managed-dpv2-observability namespace", zap.Error(err))

		// Try GKE Dataplane V2 observability namespace
		flowCollector, err = cilium.NewFlowCollector(ctx, s.Logger, dataplaneV2HubbleRelayNamespace, cache)
		if err != nil {
			s.Logger.Warn("Failed to create flow collector in both kube-system and gke-managed-dpv2-observability namespaces, continuing without Cilium flow collection", zap.Error(err))
			s.Logger.Info("Operator will continue with resource ingestion and other functions, but network flow data will not be available")
			s.Logger.Info("To suppress this warning, set DISABLE_CILIUM_FLOWS=true environment variable")

			// Still set up Istio ALS even without Cilium
			s.setupIstioALSIfEnabled(ctx, l7Cache)

			// Return nil flowCollector to indicate Cilium is not available, but don't fail startup
			return flowChan, cache, nil, l7FlowChan, l7Cache, nil
		} else {
			s.Logger.Info("Successfully connected to Cilium hubble-relay in gke-managed-dpv2-observability namespace")
		}
	} else {
		s.Logger.Info("Successfully connected to Cilium hubble-relay in kube-system namespace")
	}

	s.Logger.Info("Cilium network flow collection enabled")

	// Set up Istio ALS alongside Cilium if enabled
	s.setupIstioALSIfEnabled(ctx, l7Cache)

	return flowChan, cache, flowCollector, l7FlowChan, l7Cache, nil
}

// setupIstioALSIfEnabled starts the Istio Access Log Service if enabled and connects it to the L7 cache
func (s *StreamClient) setupIstioALSIfEnabled(ctx context.Context, l7Cache *smartcache.L7SmartCache) {
	// Check if Istio ALS is enabled
	enableIstioALS := getEnvOrDefault("ENABLE_ISTIO_ALS", "false")
	if enableIstioALS == "true" {
		alsPortStr := getEnvOrDefault("ISTIO_ALS_PORT", "8080")
		alsPort, err := strconv.Atoi(alsPortStr)
		if err != nil {
			s.Logger.Warn("Invalid ISTIO_ALS_PORT, using default 8080", zap.String("port", alsPortStr))
			alsPort = 8080
		}

		// Create L7 access log channel that feeds directly into the cache
		l7LogChan := make(chan *v1.L7AccessLog, 1000)

		// Create ALS server
		alsServer := envoy_als.NewALSServer(s.Logger, l7LogChan, alsPort)

		// Start ALS server in background with retry logic
		go func() {
			s.Logger.Info("Starting Istio Access Log Service with retry capability", zap.Int("port", alsPort))

			// Use retry logic for ALS server similar to stream connections
			alsFunc := func(ctx context.Context) error {
				s.Logger.Info("Starting/Restarting Istio Access Log Service", zap.Int("port", alsPort))
				return alsServer.StartServer(ctx)
			}

			// This will automatically retry if the ALS server fails
			WithReconnect(ctx, alsFunc)
		}()

		// Start goroutine to read from ALS channel and feed into L7 cache
		// This follows the same pattern as Cilium flows feeding into the smart cache
		go func() {
			for {
				select {
				case accessLog := <-l7LogChan:
					if accessLog != nil {
						l7Cache.AddL7AccessLog(accessLog)
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		s.Logger.Info("Istio Access Log Service enabled and connected to L7 cache", zap.Int("port", alsPort))
	} else {
		s.Logger.Info("Istio Access Log Service disabled")
	}
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

	s.Logger.Info("Attempting to establish stream with server",
		zap.String("tenantID", tenantID),
		zap.String("serverAddr", fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)))

	// Start the bidirectional stream
	stream, err := streamClient.StreamData(ctxWithMetadata)
	if err != nil {
		s.Logger.Error("Failed to establish stream",
			zap.Error(err),
			zap.String("tenantID", tenantID),
			zap.String("serverAddr", fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)))
		return nil, nil, err
	}

	s.Logger.Info("Successfully established stream with server",
		zap.String("tenantID", tenantID))

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
// (inventory and incident data are already being handled by separate goroutines)
func (s *StreamClient) handleBidirectionalStreamWithFlows(ctx context.Context, stream v1.StreamService_StreamDataClient, flowChan chan smartcache.FlowCount, inventoryDone <-chan error, incidentDone <-chan error, l7FlowChan <-chan smartcache.L7Flow) error {
	// Create a channel to handle stream closure
	done := make(chan error, 1)

	// Start goroutine to handle incoming network policies from server
	go s.handleServerMessages(ctx, stream, done)

	// Start goroutine to collect flow data and send to server
	go s.sendFlowData(ctx, stream, flowChan, done)

	// Start goroutine to collect L7 flows and send to server
	if l7FlowChan != nil {
		s.Logger.Info("Starting L7 flow processing goroutine")
		go s.sendL7FlowsToStreamWithDone(ctx, stream, l7FlowChan, done)
	} else {
		s.Logger.Info("No L7 flow channel available, skipping L7 log processing")
	}

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

	// Monitor the incident data goroutine for errors
	go func() {
		select {
		case err := <-incidentDone:
			if err != nil {
				s.Logger.Error("Incident data sending failed", zap.Error(err))
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
				s.recordStreamError(err)
				select {
				case done <- err:
				default:
				}
				return
			}

			// Successfully received a message - mark stream as healthy
			s.recordStreamHealthy()

			// Log all received messages for debugging
			s.Logger.Debug("Received message from server", zap.String("message_type", fmt.Sprintf("%T", response.Response)))

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
			case *v1.StreamDataResponse_YamlDryRunRequest:
				s.handleYamlDryRunRequest(ctx, stream, resp.YamlDryRunRequest)
			case *v1.StreamDataResponse_YamlApplyRequest:
				s.handleYamlApplyRequest(ctx, stream, resp.YamlApplyRequest)
			case *v1.StreamDataResponse_CertificateRenewalRequest:
				s.handleCertificateRenewalRequest(ctx, stream, resp.CertificateRenewalRequest)
			case *v1.StreamDataResponse_OperatorRestartRequest:
				s.handleOperatorRestartRequest(ctx, stream, resp.OperatorRestartRequest)
			case *v1.StreamDataResponse_MetricsQueryRequest:
				s.handleMetricsQueryRequest(ctx, stream, resp.MetricsQueryRequest)
			default:
				s.Logger.Warn("Received unhandled message type from server",
					zap.String("message_type", fmt.Sprintf("%T", response.Response)))
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

// handleYamlDryRunRequest processes YAML dry-run validation requests from the server
func (s *StreamClient) handleYamlDryRunRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	yamlRequest *v1.YamlDryRunRequest,
) {
	s.Logger.Info("Received YAML dry-run validation request from server",
		zap.String("request_id", yamlRequest.RequestId),
		zap.Int("manifests_count", len(yamlRequest.YamlManifests)))

	// Create YAML validator
	yamlValidator, err := ingestion.NewYamlValidator(s.Logger)
	if err != nil {
		s.Logger.Error("Failed to create YAML validator", zap.Error(err))
		// Send error response
		errorResponse := &v1.YamlDryRunResponse{
			RequestId:          yamlRequest.RequestId,
			GlobalErrorMessage: fmt.Sprintf("Failed to create YAML validator: %v", err),
		}
		s.sendYamlDryRunResponse(stream, errorResponse)
		return
	}

	// Validate YAML manifests
	yamlResponse := yamlValidator.ValidateYamlManifests(ctx, yamlRequest)

	// Send the response back to the server
	s.sendYamlDryRunResponse(stream, yamlResponse)
}

// handleYamlApplyRequest processes YAML apply requests from the server
func (s *StreamClient) handleYamlApplyRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	yamlRequest *v1.YamlApplyRequest,
) {
	s.Logger.Info("Received YAML apply request from server",
		zap.String("request_id", yamlRequest.RequestId),
		zap.Int("manifests_count", len(yamlRequest.YamlManifests)))

	// Process each manifest
	results := make([]*v1.YamlApplyResult, 0, len(yamlRequest.YamlManifests))

	for _, manifest := range yamlRequest.YamlManifests {
		result := s.applyYamlManifest(ctx, manifest)
		results = append(results, result)
	}

	// Create response
	yamlResponse := &v1.YamlApplyResponse{
		RequestId: yamlRequest.RequestId,
		Results:   results,
	}

	// Send the response back to the server
	s.sendYamlApplyResponse(stream, yamlResponse)
}

// sendYamlDryRunResponse sends a YAML dry-run validation response to the server
func (s *StreamClient) sendYamlDryRunResponse(stream v1.StreamService_StreamDataClient, yamlResponse *v1.YamlDryRunResponse) {
	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_YamlDryRunResponse{
			YamlDryRunResponse: yamlResponse,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send YAML dry-run response to server",
			zap.String("request_id", yamlResponse.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent YAML dry-run response to server",
			zap.String("request_id", yamlResponse.RequestId),
			zap.Int("results_count", len(yamlResponse.Results)))
	}
}

// sendYamlApplyResponse sends a YAML apply response to the server
func (s *StreamClient) sendYamlApplyResponse(stream v1.StreamService_StreamDataClient, yamlResponse *v1.YamlApplyResponse) {
	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_YamlApplyResponse{
			YamlApplyResponse: yamlResponse,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send YAML apply response to server",
			zap.String("request_id", yamlResponse.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent YAML apply response to server",
			zap.String("request_id", yamlResponse.RequestId),
			zap.Int("results_count", len(yamlResponse.Results)))
	}
}

// applyYamlManifest applies a single YAML manifest to the cluster
func (s *StreamClient) applyYamlManifest(ctx context.Context, manifest *v1.YamlManifest) *v1.YamlApplyResult {
	result := &v1.YamlApplyResult{
		ManifestId: manifest.ManifestId,
	}

	s.Logger.Info("Applying YAML manifest",
		zap.String("manifest_id", manifest.ManifestId),
		zap.String("resource_type", manifest.ResourceType),
		zap.String("namespace", manifest.Namespace))

	// Apply the YAML using kubectl
	err := s.applyResourceToCluster(ctx, manifest.YamlContent, manifest.ResourceType, manifest.Namespace)
	if err != nil {
		s.Logger.Error("Failed to apply YAML manifest",
			zap.String("manifest_id", manifest.ManifestId),
			zap.Error(err))
		result.Success = false
		result.ErrorMessage = err.Error()
		result.StatusCode = 500
		return result
	}

	result.Success = true
	result.StatusCode = 200
	result.AppliedResourceInfo = fmt.Sprintf("Applied %s in namespace %s", manifest.ResourceType, manifest.Namespace)

	s.Logger.Info("Successfully applied YAML manifest",
		zap.String("manifest_id", manifest.ManifestId),
		zap.String("applied_info", result.AppliedResourceInfo))

	return result
}

// immutableResourceTypes lists Kubernetes resource types whose spec is almost
// entirely immutable after creation. These always use `kubectl replace --force`
// (delete + recreate) because `kubectl apply` will fail on virtually any change.
//
// Resources with only a few immutable fields (PVC, PV, Service) are NOT listed
// here — they use the normal apply-first path with automatic fallback to replace
// when an immutable-field error is detected (see isImmutableFieldError). This
// avoids unnecessary delete+recreate, which is destructive for stateful resources
// (PVC/PV deletion can destroy data, Service deletion deprovisions load balancers).
var immutableResourceTypes = map[string]bool{
	// Pod — almost all spec fields are immutable after creation
	"pod": true,
	// Job — spec.selector and spec.template are immutable
	"job": true,
	// CronJob's jobTemplate is effectively immutable once child Jobs exist;
	// replace --force on the CronJob itself is safe (only future runs affected)
	"cronjob": true,
}

// applyResourceToCluster applies YAML content to the cluster using kubectl.
// Pods, Jobs, and CronJobs always use delete+recreate. All other resources
// use kubectl apply first, with automatic fallback to delete+recreate if
// an immutable-field error is detected.
func (s *StreamClient) applyResourceToCluster(ctx context.Context, yamlContent, resourceType, namespace string) error {
	// Create a temporary file for the YAML content
	tmpFile, err := os.CreateTemp("", "kestrel-apply-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write YAML content to temporary file
	if _, err := tmpFile.WriteString(yamlContent); err != nil {
		return fmt.Errorf("failed to write YAML to temporary file: %w", err)
	}
	tmpFile.Close()

	if immutableResourceTypes[strings.ToLower(resourceType)] {
		return s.replaceResource(ctx, tmpFile.Name(), resourceType, namespace)
	}

	// For mutable resources, try kubectl apply first. If it fails because of
	// an immutable-field error, fall back to replace automatically.
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", tmpFile.Name(), "--timeout=30s")
	output, err := cmd.CombinedOutput()
	if err != nil {
		outStr := string(output)
		if isImmutableFieldError(outStr) {
			s.Logger.Warn("kubectl apply hit immutable field — falling back to replace (delete + recreate)",
				zap.String("resource_type", resourceType),
				zap.String("namespace", namespace))
			return s.replaceResource(ctx, tmpFile.Name(), resourceType, namespace)
		}
		return fmt.Errorf("kubectl apply failed: %w, output: %s", err, outStr)
	}

	s.Logger.Info("kubectl apply completed successfully",
		zap.String("output", string(output)),
		zap.String("resource_type", resourceType),
		zap.String("namespace", namespace))
	return nil
}

// replaceResource performs a delete+recreate using kubectl replace --force.
// If the resource doesn't exist yet, it falls back to kubectl create.
func (s *StreamClient) replaceResource(ctx context.Context, filePath, resourceType, namespace string) error {
	s.Logger.Info("Using replace strategy (delete + recreate) for resource with immutable fields",
		zap.String("resource_type", resourceType),
		zap.String("namespace", namespace))

	cmd := exec.CommandContext(ctx, "kubectl", "replace", "--force", "-f", filePath, "--timeout=30s")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "not found") {
			s.Logger.Info("Resource not found, using create instead of replace",
				zap.String("resource_type", resourceType),
				zap.String("namespace", namespace))

			createCmd := exec.CommandContext(ctx, "kubectl", "create", "-f", filePath, "--timeout=30s")
			createOutput, createErr := createCmd.CombinedOutput()
			if createErr != nil {
				return fmt.Errorf("kubectl create failed: %w, output: %s", createErr, string(createOutput))
			}

			s.Logger.Info("kubectl create completed successfully",
				zap.String("output", string(createOutput)),
				zap.String("resource_type", resourceType),
				zap.String("namespace", namespace))
			return nil
		}
		return fmt.Errorf("kubectl replace failed: %w, output: %s", err, string(output))
	}

	s.Logger.Info("kubectl replace completed successfully (resource deleted and recreated)",
		zap.String("output", string(output)),
		zap.String("resource_type", resourceType),
		zap.String("namespace", namespace))
	return nil
}

// isImmutableFieldError returns true if kubectl output indicates the apply failed
// because of immutable fields (covers the common Kubernetes API error patterns).
func isImmutableFieldError(output string) bool {
	lower := strings.ToLower(output)
	return strings.Contains(lower, "spec is immutable") ||
		strings.Contains(lower, "field is immutable") ||
		strings.Contains(lower, "forbidden: spec") ||
		strings.Contains(lower, "immutable after creation")
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

// sendInventoryData collects workload, namespace, network policy, service, and authorization policy data and sends it to the server
func (s *StreamClient) sendInventoryData(ctx context.Context, stream v1.StreamService_StreamDataClient, workloadChan <-chan *v1.Workload, namespaceChan <-chan *v1.Namespace, networkPolicyChan <-chan *v1.NetworkPolicy, serviceChan <-chan *v1.Service, authorizationPolicyChan <-chan *v1.AuthorizationPolicy, podChan <-chan *v1.Pod, nodeChan <-chan *v1.Node, done chan<- error) {
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
		case authorizationPolicy, ok := <-authorizationPolicyChan:
			// Check if authorization policy channel was closed
			if !ok {
				s.Logger.Warn("Authorization policy channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("authorization policy channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendAuthorizationPolicyToServer(stream, authorizationPolicy); err != nil {
					s.Logger.Error("Failed to send authorization policy data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case pod, ok := <-podChan:
			// Check if pod channel was closed
			if !ok {
				s.Logger.Warn("Pod channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("pod channel closed"):
				default:
				}
				return
			}

			// Register/unregister pod with the pod resolver for metrics workload lookup
			if s.podResolver != nil {
				switch pod.Action {
				case v1.Action_ACTION_CREATE, v1.Action_ACTION_UPDATE:
					s.podResolver.RegisterPod(pod)
				case v1.Action_ACTION_DELETE:
					s.podResolver.UnregisterPod(pod.Namespace, pod.Name, pod.Uid)
				}
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendPodToServer(stream, pod); err != nil {
					s.Logger.Error("Failed to send pod data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}

		case node, ok := <-nodeChan:
			// Check if node channel was closed
			if !ok {
				s.Logger.Warn("Node channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("node channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendNodeToServer(stream, node); err != nil {
					s.Logger.Error("Failed to send node data", zap.Error(err))
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

// sendAuthorizationPolicyToServer sends a single authorization policy to the server
func (s *StreamClient) sendAuthorizationPolicyToServer(stream v1.StreamService_StreamDataClient, authorizationPolicy *v1.AuthorizationPolicy) error {
	// Convert authorization policy data to proto message and send
	authorizationPolicyMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_AuthorizationPolicy{
			AuthorizationPolicy: authorizationPolicy,
		},
	}
	return s.protectedSend(stream, authorizationPolicyMsg)
}

// sendPodToServer sends a single pod to the server
func (s *StreamClient) sendPodToServer(stream v1.StreamService_StreamDataClient, pod *v1.Pod) error {
	// Convert pod data to proto message and send
	podMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Pod{
			Pod: pod,
		},
	}
	return s.protectedSend(stream, podMsg)
}

// sendNodeToServer sends a single node to the server
func (s *StreamClient) sendNodeToServer(stream v1.StreamService_StreamDataClient, node *v1.Node) error {
	// Convert node data to proto message and send
	nodeMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_Node{
			Node: node,
		},
	}
	return s.protectedSend(stream, nodeMsg)
}

// sendIncidentData collects incident detection data and sends it to the server
func (s *StreamClient) sendIncidentData(ctx context.Context, stream v1.StreamService_StreamDataClient, eventChan <-chan *v1.KubernetesEvent, podStatusChan <-chan *v1.PodStatusChange, nodeConditionChan <-chan *v1.NodeConditionChange, rolloutStatusChan <-chan *v1.WorkloadRolloutStatus, podLogsChan <-chan *v1.PodLogs, done chan<- error) {
	for {
		select {
		case <-ctx.Done():
			// Context is done, exit the goroutine
			return
		case event, ok := <-eventChan:
			// Check if event channel was closed
			if !ok {
				s.Logger.Warn("Event channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("event channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendKubernetesEventToServer(stream, event); err != nil {
					s.Logger.Error("Failed to send Kubernetes event data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case podStatus, ok := <-podStatusChan:
			// Check if pod status channel was closed
			if !ok {
				s.Logger.Warn("Pod status channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("pod status channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendPodStatusChangeToServer(stream, podStatus); err != nil {
					s.Logger.Error("Failed to send pod status change data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case nodeCondition, ok := <-nodeConditionChan:
			// Check if node condition channel was closed
			if !ok {
				s.Logger.Warn("Node condition channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("node condition channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendNodeConditionChangeToServer(stream, nodeCondition); err != nil {
					s.Logger.Error("Failed to send node condition change data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case rolloutStatus, ok := <-rolloutStatusChan:
			// Check if rollout status channel was closed
			if !ok {
				s.Logger.Warn("Rollout status channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("rollout status channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendWorkloadRolloutStatusToServer(stream, rolloutStatus); err != nil {
					s.Logger.Error("Failed to send workload rollout status data", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		case podLogs, ok := <-podLogsChan:
			// Check if pod logs channel was closed
			if !ok {
				s.Logger.Warn("Pod logs channel closed unexpectedly")
				select {
				case done <- fmt.Errorf("pod logs channel closed"):
				default:
				}
				return
			}

			// Check context again before sending
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.sendPodLogsToServer(stream, podLogs); err != nil {
					s.Logger.Error("Failed to send pod logs data", zap.Error(err))
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

// sendKubernetesEventToServer sends a Kubernetes event to the server
func (s *StreamClient) sendKubernetesEventToServer(stream v1.StreamService_StreamDataClient, event *v1.KubernetesEvent) error {
	eventMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_KubernetesEvent{
			KubernetesEvent: event,
		},
	}
	return s.protectedSend(stream, eventMsg)
}

// sendPodStatusChangeToServer sends a pod status change to the server
func (s *StreamClient) sendPodStatusChangeToServer(stream v1.StreamService_StreamDataClient, podStatus *v1.PodStatusChange) error {
	podStatusMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_PodStatusChange{
			PodStatusChange: podStatus,
		},
	}
	return s.protectedSend(stream, podStatusMsg)
}

// sendNodeConditionChangeToServer sends a node condition change to the server
func (s *StreamClient) sendNodeConditionChangeToServer(stream v1.StreamService_StreamDataClient, nodeCondition *v1.NodeConditionChange) error {
	nodeConditionMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_NodeConditionChange{
			NodeConditionChange: nodeCondition,
		},
	}
	return s.protectedSend(stream, nodeConditionMsg)
}

// sendWorkloadRolloutStatusToServer sends a workload rollout status to the server
func (s *StreamClient) sendWorkloadRolloutStatusToServer(stream v1.StreamService_StreamDataClient, rolloutStatus *v1.WorkloadRolloutStatus) error {
	rolloutStatusMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_WorkloadRolloutStatus{
			WorkloadRolloutStatus: rolloutStatus,
		},
	}
	return s.protectedSend(stream, rolloutStatusMsg)
}

// sendPodLogsToServer sends pod logs to the server
func (s *StreamClient) sendPodLogsToServer(stream v1.StreamService_StreamDataClient, podLogs *v1.PodLogs) error {
	podLogsMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_PodLogs{
			PodLogs: podLogs,
		},
	}
	return s.protectedSend(stream, podLogsMsg)
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

// sendL7FlowsToStreamWithDone sends L7 flows to the server stream with done channel
func (s *StreamClient) sendL7FlowsToStreamWithDone(ctx context.Context, stream v1.StreamService_StreamDataClient, l7FlowChan <-chan smartcache.L7Flow, done chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case l7Flow, ok := <-l7FlowChan:
			// Check if channel was closed
			if !ok {
				s.Logger.Info("L7 flow channel closed")
				return
			}
			if l7Flow.AccessLog != nil {
				// The access log already has all aggregated values from the L7 cache
				// No need to modify anything - just send it as-is

				// Send L7 access log to server with aggregated data
				logMsg := &v1.StreamDataRequest{
					Request: &v1.StreamDataRequest_L7AccessLog{
						L7AccessLog: l7Flow.AccessLog,
					},
				}

				if err := s.protectedSend(stream, logMsg); err != nil {
					s.Logger.Error("Failed to send L7 access log", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}

				// Create base log fields (include aggregation info)
				l7Log := l7Flow.AccessLog
				logFields := []zap.Field{
					zap.String("l7_protocol", l7Log.L7Protocol.String()),
					zap.String("node_id", l7Log.NodeId),
					zap.String("protocol", l7Log.Protocol),
					zap.Int64("duration_ms", l7Log.DurationMs),
					zap.Uint64("bytes_sent", l7Log.BytesSent),
					zap.Uint64("bytes_received", l7Log.BytesReceived),
					zap.Bool("allowed", l7Log.Allowed),
					zap.String("cluster_name", l7Log.ClusterName),
					zap.Int64("count", l7Log.Count),
				}

				if l7Log.Timestamp != nil {
					logFields = append(logFields, zap.String("timestamp", l7Log.Timestamp.String()))
				}

				if l7Log.FirstSeen != nil {
					logFields = append(logFields, zap.String("first_seen", l7Log.FirstSeen.String()))
				}

				if l7Log.LastSeen != nil {
					logFields = append(logFields, zap.String("last_seen", l7Log.LastSeen.String()))
				}

				if l7Log.Source != nil {
					logFields = append(logFields,
						zap.String("src_ip", l7Log.Source.Ip),
						zap.String("src_port", strconv.Itoa(int(l7Log.Source.Port))),
						zap.String("src_ns", l7Log.Source.Namespace),
						zap.String("src_name", l7Log.Source.Name),
						zap.String("src_service", l7Log.Source.ServiceName),
						zap.String("src_kind", l7Log.Source.Kind),
					)
				}

				if l7Log.Destination != nil {
					logFields = append(logFields,
						zap.String("dst_ip", l7Log.Destination.Ip),
						zap.String("dst_port", strconv.Itoa(int(l7Log.Destination.Port))),
						zap.String("dst_ns", l7Log.Destination.Namespace),
						zap.String("dst_name", l7Log.Destination.Name),
						zap.String("dst_service", l7Log.Destination.ServiceName),
						zap.String("dst_kind", l7Log.Destination.Kind),
					)
				}

				// Add HTTP-specific fields if HttpData is not nil
				if l7Log.HttpData != nil {
					logFields = append(logFields,
						zap.String("http_method", l7Log.HttpData.Method),
						zap.String("http_path", l7Log.HttpData.Path),
						zap.String("http_version", l7Log.HttpData.Version),
						zap.String("http_host", l7Log.HttpData.Host),
						zap.String("http_user_agent", l7Log.HttpData.UserAgent),
						zap.String("http_referer", l7Log.HttpData.Referer),
						zap.String("http_response_code", strconv.Itoa(int(l7Log.HttpData.ResponseCode))),
						zap.String("http_response_size", strconv.Itoa(int(l7Log.HttpData.ResponseSize))),
						zap.String("http_response_flags", strings.Join(l7Log.HttpData.ResponseFlags, ",")),
					)

					// Add request headers if they exist
					if len(l7Log.HttpData.RequestHeaders) > 0 {
						headerStrs := make([]string, 0, len(l7Log.HttpData.RequestHeaders))
						for k, v := range l7Log.HttpData.RequestHeaders {
							headerStrs = append(headerStrs, fmt.Sprintf("%s:%s", k, v))
						}
						logFields = append(logFields, zap.String("http_request_headers", strings.Join(headerStrs, ",")))
					}
				}

				// Add TCP-specific fields if TcpData is not nil
				if l7Log.TcpData != nil {
					logFields = append(logFields,
						zap.String("tcp_connection_state", l7Log.TcpData.ConnectionState),
						zap.String("tcp_access_log_type", l7Log.TcpData.AccessLogType),
						zap.Uint64("tcp_received_bytes", l7Log.TcpData.ReceivedBytes),
						zap.Uint64("tcp_sent_bytes", l7Log.TcpData.SentBytes),
						zap.String("tcp_stream_id", l7Log.TcpData.StreamId),
						zap.String("tcp_termination_details", l7Log.TcpData.ConnectionTerminationDetails),
					)
				}

				s.Logger.Debug("Sent L7 access log", logFields...)

				// Validate if we have sufficient data for Authorization Policy generation
				if l7Log.HttpData != nil && l7Log.Source != nil && l7Log.Destination != nil {
					authPolicyData := []string{}
					if l7Log.Source.Namespace != "" && l7Log.Source.Name != "" {
						authPolicyData = append(authPolicyData, fmt.Sprintf("src=%s/%s", l7Log.Source.Namespace, l7Log.Source.Name))
					}
					if l7Log.Destination.Namespace != "" && l7Log.Destination.ServiceName != "" {
						authPolicyData = append(authPolicyData, fmt.Sprintf("dst=%s/%s", l7Log.Destination.Namespace, l7Log.Destination.ServiceName))
					}
					if l7Log.HttpData.Method != "" && l7Log.HttpData.Path != "" {
						authPolicyData = append(authPolicyData, fmt.Sprintf("method=%s", l7Log.HttpData.Method))
						authPolicyData = append(authPolicyData, fmt.Sprintf("path=%s", l7Log.HttpData.Path))
					}

					if len(authPolicyData) >= 3 {
						s.Logger.Debug("L7 log contains sufficient data for Authorization Policy",
							zap.String("policy_data", strings.Join(authPolicyData, ", ")))
					} else {
						s.Logger.Debug("L7 log missing data for Authorization Policy generation",
							zap.String("available_data", strings.Join(authPolicyData, ", ")))
					}
				}
			}
		}
	}
}

// handleCertificateRenewalRequest processes certificate renewal requests from the server
func (s *StreamClient) handleCertificateRenewalRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	renewalRequest *v1.CertificateRenewalRequest,
) {
	s.Logger.Info("Received certificate renewal request from server",
		zap.String("request_id", renewalRequest.RequestId),
		zap.String("serial_number", renewalRequest.SerialNumber),
		zap.Time("expires_at", renewalRequest.ExpiresAt.AsTime()))

	// Apply the new certificates
	success, errorMessage := s.applyCertificateRenewal(ctx, renewalRequest)

	// Send response back to server
	response := &v1.CertificateRenewalResponse{
		RequestId:    renewalRequest.RequestId,
		Success:      success,
		ErrorMessage: errorMessage,
		AppliedAt:    timestamppb.Now(),
	}

	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_CertificateRenewalResponse{
			CertificateRenewalResponse: response,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send certificate renewal response to server",
			zap.String("request_id", renewalRequest.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent certificate renewal response to server",
			zap.String("request_id", renewalRequest.RequestId),
			zap.Bool("success", success))
	}
}

// applyCertificateRenewal applies new certificates by updating the mounted secret
func (s *StreamClient) applyCertificateRenewal(ctx context.Context, renewalRequest *v1.CertificateRenewalRequest) (bool, string) {
	s.Logger.Info("Applying certificate renewal",
		zap.String("request_id", renewalRequest.RequestId))

	// Decode base64-encoded certificate data
	clientCertData, err := base64.StdEncoding.DecodeString(renewalRequest.ClientCert)
	if err != nil {
		s.Logger.Error("Failed to decode base64 client certificate", zap.Error(err))
		return false, fmt.Sprintf("Failed to decode client certificate: %v", err)
	}

	clientKeyData, err := base64.StdEncoding.DecodeString(renewalRequest.ClientKey)
	if err != nil {
		s.Logger.Error("Failed to decode base64 client key", zap.Error(err))
		return false, fmt.Sprintf("Failed to decode client key: %v", err)
	}

	caCertData, err := base64.StdEncoding.DecodeString(renewalRequest.CaCert)
	if err != nil {
		s.Logger.Error("Failed to decode base64 CA certificate", zap.Error(err))
		return false, fmt.Sprintf("Failed to decode CA certificate: %v", err)
	}

	s.Logger.Info("Successfully decoded certificate data",
		zap.Int("client_cert_bytes", len(clientCertData)),
		zap.Int("client_key_bytes", len(clientKeyData)),
		zap.Int("ca_cert_bytes", len(caCertData)))

	// Need to update the secret that contains the client certificates
	// The operator deployment mounts this secret at /tls
	secretName := os.Getenv("CLIENT_CERT_SECRET_NAME")
	if secretName == "" {
		s.Logger.Error("CLIENT_CERT_SECRET_NAME environment variable is not set")
		return false, "CLIENT_CERT_SECRET_NAME environment variable is not set"
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		s.Logger.Error("POD_NAMESPACE environment variable is not set")
		return false, "POD_NAMESPACE environment variable is not set"
	}

	// Create Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		s.Logger.Error("Failed to create Kubernetes config for certificate renewal", zap.Error(err))
		return false, fmt.Sprintf("Failed to create Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		s.Logger.Error("Failed to create Kubernetes clientset for certificate renewal", zap.Error(err))
		return false, fmt.Sprintf("Failed to create Kubernetes clientset: %v", err)
	}

	// Get the existing secret first to preserve any additional data
	existingSecret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		s.Logger.Error("Failed to get existing certificate secret",
			zap.String("secret_name", secretName),
			zap.String("namespace", namespace),
			zap.Error(err))
		return false, fmt.Sprintf("Failed to get existing certificate secret: %v", err)
	}

	// Update the certificate data while preserving other fields
	// Use the decoded certificate data (already converted from base64)
	existingSecret.Data["tls.crt"] = clientCertData
	existingSecret.Data["tls.key"] = clientKeyData
	existingSecret.Data["ca.crt"] = caCertData
	// Alternative names for backward compatibility
	existingSecret.Data["client.crt"] = clientCertData
	existingSecret.Data["client.key"] = clientKeyData

	// Add annotation to trigger pod restart for certificate pickup
	if existingSecret.Annotations == nil {
		existingSecret.Annotations = make(map[string]string)
	}
	existingSecret.Annotations["kestrel.ai/cert-renewed-at"] = time.Now().Format(time.RFC3339)

	// Update the secret
	_, err = clientset.CoreV1().Secrets(namespace).Update(ctx, existingSecret, metav1.UpdateOptions{})
	if err != nil {
		s.Logger.Error("Failed to update certificate secret",
			zap.String("secret_name", secretName),
			zap.String("namespace", namespace),
			zap.Error(err))
		return false, fmt.Sprintf("Failed to update certificate secret: %v", err)
	}

	s.Logger.Info("Successfully updated certificate secret",
		zap.String("secret_name", secretName),
		zap.String("namespace", namespace),
		zap.String("expires_at", renewalRequest.ExpiresAt.AsTime().Format(time.RFC3339)))

	// Trigger pod restart by updating deployment annotation
	// This ensures the new certificates are picked up immediately
	err = s.triggerPodRestart(ctx, clientset, namespace)
	if err != nil {
		s.Logger.Warn("Failed to trigger pod restart for certificate renewal",
			zap.Error(err))
		// Don't fail the renewal for this - the certificates are updated
	} else {
		s.Logger.Info("Triggered pod restart to apply new certificates")
	}

	return true, ""
}

// triggerPodRestart triggers a rolling restart of the operator deployment to pick up new certificates
func (s *StreamClient) triggerPodRestart(ctx context.Context, clientset *kubernetes.Clientset, namespace string) error {
	// Get deployment name from environment or infer from hostname
	deploymentName := os.Getenv("DEPLOYMENT_NAME")
	if deploymentName == "" {
		s.Logger.Error("DEPLOYMENT_NAME environment variable is not set")
		return fmt.Errorf("DEPLOYMENT_NAME environment variable is not set")
	}

	// Get the deployment
	deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment %s: %w", deploymentName, err)
	}

	// Add annotation to trigger rolling restart
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = make(map[string]string)
	}
	deployment.Spec.Template.Annotations["kestrel.ai/cert-renewed-at"] = time.Now().Format(time.RFC3339)

	// Update the deployment
	_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update deployment %s: %w", deploymentName, err)
	}

	s.Logger.Info("Successfully triggered rolling restart for certificate renewal",
		zap.String("deployment", deploymentName),
		zap.String("namespace", namespace))

	return nil
}

// handleOperatorRestartRequest processes operator restart requests from the server
func (s *StreamClient) handleOperatorRestartRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	restartRequest *v1.OperatorRestartRequest,
) {
	s.Logger.Info("Received operator restart request from server",
		zap.String("request_id", restartRequest.RequestId),
		zap.String("reason", restartRequest.Reason),
		zap.Int32("delay_seconds", restartRequest.DelaySeconds),
		zap.Bool("graceful", restartRequest.Graceful))

	// Apply the operator restart
	success, errorMessage := s.applyOperatorRestart(ctx, restartRequest)

	// Send response back to server
	response := &v1.OperatorRestartResponse{
		RequestId:    restartRequest.RequestId,
		Success:      success,
		ErrorMessage: errorMessage,
		InitiatedAt:  timestamppb.Now(),
	}

	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_OperatorRestartResponse{
			OperatorRestartResponse: response,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send operator restart response to server",
			zap.String("request_id", restartRequest.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent operator restart response to server",
			zap.String("request_id", restartRequest.RequestId),
			zap.Bool("success", success))
	}
}

// applyOperatorRestart handles the actual operator restart process
func (s *StreamClient) applyOperatorRestart(ctx context.Context, restartRequest *v1.OperatorRestartRequest) (bool, string) {
	s.Logger.Info("Applying operator restart",
		zap.String("request_id", restartRequest.RequestId),
		zap.String("reason", restartRequest.Reason))

	// Get required environment variables for restart
	deploymentName := os.Getenv("DEPLOYMENT_NAME")
	if deploymentName == "" {
		s.Logger.Error("DEPLOYMENT_NAME environment variable is not set")
		return false, "DEPLOYMENT_NAME environment variable is not set"
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		s.Logger.Error("POD_NAMESPACE environment variable is not set")
		return false, "POD_NAMESPACE environment variable is not set"
	}

	// Create Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		s.Logger.Error("Failed to create Kubernetes config for operator restart", zap.Error(err))
		return false, fmt.Sprintf("Failed to create Kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		s.Logger.Error("Failed to create Kubernetes clientset for operator restart", zap.Error(err))
		return false, fmt.Sprintf("Failed to create Kubernetes clientset: %v", err)
	}

	// Apply delay if specified
	if restartRequest.DelaySeconds > 0 {
		s.Logger.Info("Applying restart delay",
			zap.Int32("delay_seconds", restartRequest.DelaySeconds))
		time.Sleep(time.Duration(restartRequest.DelaySeconds) * time.Second)
	}

	// Determine restart strategy
	if restartRequest.Graceful {
		s.Logger.Info("Performing graceful operator restart via deployment annotation update")
		return s.performGracefulRestart(ctx, clientset, namespace, deploymentName, restartRequest.Reason)
	} else {
		s.Logger.Info("Performing immediate operator restart via pod deletion")
		return s.performImmediateRestart(ctx, clientset, namespace, deploymentName)
	}
}

// performGracefulRestart triggers a rolling restart by updating deployment annotations
func (s *StreamClient) performGracefulRestart(ctx context.Context, clientset *kubernetes.Clientset, namespace, deploymentName, reason string) (bool, string) {
	// Get the deployment
	deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		s.Logger.Error("Failed to get deployment for graceful restart",
			zap.String("deployment", deploymentName),
			zap.String("namespace", namespace),
			zap.Error(err))
		return false, fmt.Sprintf("Failed to get deployment %s: %v", deploymentName, err)
	}

	// Add annotation to trigger rolling restart
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = make(map[string]string)
	}

	// Use timestamp and reason for the restart annotation
	restartAnnotation := fmt.Sprintf("admin-restart-%d", time.Now().Unix())
	deployment.Spec.Template.Annotations["kestrel.ai/admin-restart"] = restartAnnotation
	deployment.Spec.Template.Annotations["kestrel.ai/restart-reason"] = reason
	deployment.Spec.Template.Annotations["kestrel.ai/restart-timestamp"] = time.Now().Format(time.RFC3339)

	// Update the deployment
	_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		s.Logger.Error("Failed to update deployment for graceful restart",
			zap.String("deployment", deploymentName),
			zap.String("namespace", namespace),
			zap.Error(err))
		return false, fmt.Sprintf("Failed to update deployment %s: %v", deploymentName, err)
	}

	s.Logger.Info("Successfully triggered graceful operator restart",
		zap.String("deployment", deploymentName),
		zap.String("namespace", namespace),
		zap.String("reason", reason),
		zap.String("restart_annotation", restartAnnotation))

	return true, ""
}

// performImmediateRestart deletes the current pod to force immediate restart
func (s *StreamClient) performImmediateRestart(ctx context.Context, clientset *kubernetes.Clientset, namespace, deploymentName string) (bool, string) {
	// Get current pod name from hostname
	hostname, err := os.Hostname()
	if err != nil {
		s.Logger.Error("Failed to get hostname for immediate restart", zap.Error(err))
		return false, fmt.Sprintf("Failed to get hostname: %v", err)
	}

	s.Logger.Info("Performing immediate restart by deleting current pod",
		zap.String("pod_name", hostname),
		zap.String("namespace", namespace))

	// Delete current pod to trigger immediate restart
	err = clientset.CoreV1().Pods(namespace).Delete(ctx, hostname, metav1.DeleteOptions{
		GracePeriodSeconds: func() *int64 { grace := int64(0); return &grace }(), // Immediate deletion
	})
	if err != nil {
		s.Logger.Error("Failed to delete current pod for immediate restart",
			zap.String("pod_name", hostname),
			zap.String("namespace", namespace),
			zap.Error(err))
		return false, fmt.Sprintf("Failed to delete pod %s: %v", hostname, err)
	}

	s.Logger.Info("Successfully initiated immediate operator restart",
		zap.String("pod_name", hostname),
		zap.String("namespace", namespace))

	return true, ""
}

// handleMetricsQueryRequest processes metrics query requests from the server.
// This allows the server (and agents service) to query recent metrics data
// stored locally on the operator for incident verification and RCA.
func (s *StreamClient) handleMetricsQueryRequest(
	ctx context.Context,
	stream v1.StreamService_StreamDataClient,
	queryRequest *v1.MetricsQueryRequest,
) {
	// Validate that queryRequest is not nil
	if queryRequest == nil {
		s.Logger.Warn("Received nil metrics query request from server")
		response := &v1.MetricsQueryResponse{
			RequestId:    "",
			ErrorMessage: "invalid request: query request is nil",
		}
		responseMsg := &v1.StreamDataRequest{
			Request: &v1.StreamDataRequest_MetricsQueryResponse{
				MetricsQueryResponse: response,
			},
		}
		if err := s.protectedSend(stream, responseMsg); err != nil {
			s.Logger.Error("Failed to send error response for nil metrics query request", zap.Error(err))
		}
		return
	}

	reqID := queryRequest.RequestId

	// Validate time range fields
	if queryRequest.StartTime == nil || queryRequest.EndTime == nil {
		s.Logger.Warn("Metrics query request missing required time range fields",
			zap.String("request_id", reqID))
		response := &v1.MetricsQueryResponse{
			RequestId:    reqID,
			ErrorMessage: "invalid request: start_time and end_time are required",
		}
		responseMsg := &v1.StreamDataRequest{
			Request: &v1.StreamDataRequest_MetricsQueryResponse{
				MetricsQueryResponse: response,
			},
		}
		if err := s.protectedSend(stream, responseMsg); err != nil {
			s.Logger.Error("Failed to send error response for invalid metrics query request",
				zap.String("request_id", reqID),
				zap.Error(err))
		}
		return
	}

	startTime := queryRequest.StartTime.AsTime()
	endTime := queryRequest.EndTime.AsTime()
	if startTime.After(endTime) {
		s.Logger.Warn("Metrics query request has invalid time range: start_time > end_time",
			zap.String("request_id", reqID),
			zap.Time("start_time", startTime),
			zap.Time("end_time", endTime))
		response := &v1.MetricsQueryResponse{
			RequestId:    reqID,
			ErrorMessage: "invalid request: start_time must be <= end_time",
		}
		responseMsg := &v1.StreamDataRequest{
			Request: &v1.StreamDataRequest_MetricsQueryResponse{
				MetricsQueryResponse: response,
			},
		}
		if err := s.protectedSend(stream, responseMsg); err != nil {
			s.Logger.Error("Failed to send error response for invalid time range",
				zap.String("request_id", reqID),
				zap.Error(err))
		}
		return
	}

	// Log after successful validation
	s.Logger.Info("Received metrics query request from server",
		zap.String("request_id", reqID),
		zap.String("namespace", queryRequest.Namespace),
		zap.String("workload_name", queryRequest.WorkloadName),
		zap.String("workload_kind", queryRequest.WorkloadKind),
		zap.String("pod_name", queryRequest.PodName),
		zap.Int("metric_names_count", len(queryRequest.MetricNames)))

	var response *v1.MetricsQueryResponse

	if s.metricsStore == nil {
		s.Logger.Warn("Metrics query received but metrics store is not enabled",
			zap.String("request_id", queryRequest.RequestId))
		response = &v1.MetricsQueryResponse{
			RequestId:    queryRequest.RequestId,
			ErrorMessage: "metrics store not enabled on this operator",
		}
	} else {
		var err error
		response, err = s.metricsStore.Query(queryRequest)
		if err != nil {
			s.Logger.Error("Failed to execute metrics query",
				zap.String("request_id", queryRequest.RequestId),
				zap.Error(err))
			response = &v1.MetricsQueryResponse{
				RequestId:    queryRequest.RequestId,
				ErrorMessage: err.Error(),
			}
		}
	}

	s.Logger.Info("Sending metrics query response to server",
		zap.String("request_id", queryRequest.RequestId),
		zap.Int32("series_matched", response.TotalSeriesMatched),
		zap.Int32("series_returned", response.TotalSeriesReturned),
		zap.Bool("truncated", response.Truncated))

	// Send the response back to the server
	responseMsg := &v1.StreamDataRequest{
		Request: &v1.StreamDataRequest_MetricsQueryResponse{
			MetricsQueryResponse: response,
		},
	}

	if err := s.protectedSend(stream, responseMsg); err != nil {
		s.Logger.Error("Failed to send metrics query response to server",
			zap.String("request_id", queryRequest.RequestId),
			zap.Error(err))
	} else {
		s.Logger.Info("Successfully sent metrics query response to server",
			zap.String("request_id", queryRequest.RequestId),
			zap.Int("metrics_count", len(response.Metrics)))
	}
}

// RegisterPodForMetrics registers a pod with the pod resolver for workload resolution.
// This should be called when pods are ingested to enable metrics workload lookup.
func (s *StreamClient) RegisterPodForMetrics(pod *v1.Pod) {
	if s.podResolver != nil {
		s.podResolver.RegisterPod(pod)
	}
}

// UnregisterPodForMetrics removes a pod from the pod resolver.
// This should be called when pods are deleted.
func (s *StreamClient) UnregisterPodForMetrics(namespace, name, uid string) {
	if s.podResolver != nil {
		s.podResolver.UnregisterPod(namespace, name, uid)
	}
}

// GetMetricsStore returns the metrics store instance (for testing or external use).
func (s *StreamClient) GetMetricsStore() *metrics_store.MetricsStore {
	return s.metricsStore
}

// GetPodResolver returns the pod resolver instance (for testing or external use).
func (s *StreamClient) GetPodResolver() *metrics_store.PodWorkloadResolver {
	return s.podResolver
}

// StartMetricsEviction starts the background eviction goroutine for the metrics store.
// This should be called once when the operator starts.
func (s *StreamClient) StartMetricsEviction(ctx context.Context) {
	if s.metricsStore != nil {
		s.metricsStore.StartEviction(ctx)
		s.Logger.Info("Started metrics store eviction goroutine")
	}
}

// StartOTelReceiver starts the OTEL gRPC receiver for accepting metrics from
// customer OpenTelemetry Collectors. This should be called once when the operator starts.
func (s *StreamClient) StartOTelReceiver(ctx context.Context) error {
	if s.otelReceiver == nil {
		s.Logger.Debug("OTEL receiver not configured, skipping start")
		return nil
	}

	if err := s.otelReceiver.Start(ctx); err != nil {
		s.Logger.Error("Failed to start OTEL receiver", zap.Error(err))
		return err
	}

	s.Logger.Info("Started OTEL metrics receiver")
	return nil
}

// GetOTelReceiver returns the OTEL receiver instance (for testing or monitoring).
func (s *StreamClient) GetOTelReceiver() *otel_receiver.OTelReceiverServer {
	return s.otelReceiver
}
