package cilium

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"operator/pkg/k8s_helper"

	smartcache "operator/pkg/smart_cache"

	flow "github.com/cilium/cilium/api/v1/flow"
	observer "github.com/cilium/cilium/api/v1/observer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	ciliumHubbleRelayMaxFlowCount uint64 = 100
	ciliumHubbleRelayServiceName  string = "hubble-relay"

	// Secret names for certificate discovery
	hubbleRelayClientSecretName = "hubble-relay-client-certs"
	hubbleServerCertsSecretName = "hubble-relay-server-certs"
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
		return "", fmt.Errorf("failed to get hubble-relay service in namespace %s: %w", ciliumNamespace, err)
	}

	if len(service.Spec.Ports) == 0 {
		return "", fmt.Errorf("no ports found in hubble-relay service in namespace %s", ciliumNamespace)
	}

	// Use the service port (which should be the external port)
	address := fmt.Sprintf("%s:%d", service.Spec.ClusterIP, service.Spec.Ports[0].Port)
	return address, nil
}

// HubbleTLSConfig holds TLS configuration for Hubble relay
type HubbleTLSConfig struct {
	ClientCert []byte
	ClientKey  []byte
	CACert     []byte
	ServerName string
}

// loadHubbleTLSCredentials loads TLS credentials for connecting to Hubble relay using multiple discovery methods
func loadHubbleTLSCredentials(logger *zap.Logger, namespace string) (credentials.TransportCredentials, error) {
	logger.Info("Attempting to discover Hubble TLS certificates",
		zap.String("namespace", namespace))

	tlsConfig, err := discoverHubbleCertificates(logger, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to discover Hubble TLS certificates: %w", err)
	}

	// Validate and parse client certificate and key
	cert, err := tls.X509KeyPair(tlsConfig.ClientCert, tlsConfig.ClientKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse client certificate and key: %w", err)
	}

	// Validate that we have a valid certificate
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("no certificates found in client certificate")
	}

	// Parse the client certificate to verify it's valid
	clientCertParsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse client certificate: %w", err)
	}

	// Log certificate details for debugging
	logger.Debug("Loaded client certificate details",
		zap.String("subject", clientCertParsed.Subject.String()),
		zap.Time("not_before", clientCertParsed.NotBefore),
		zap.Time("not_after", clientCertParsed.NotAfter))

	// Create certificate pool and add CA
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(tlsConfig.CACert) {
		return nil, fmt.Errorf("failed to parse CA certificate - invalid PEM format")
	}

	// The AppendCertsFromPEM success is sufficient validation for PEM certificates
	logger.Debug("Successfully loaded and validated CA certificate for TLS connection")

	// Create TLS config
	grpcTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		ServerName:   tlsConfig.ServerName,
	}

	logger.Info("Successfully configured TLS for Hubble relay",
		zap.String("server_name", tlsConfig.ServerName),
		zap.String("namespace", namespace))

	return credentials.NewTLS(grpcTLSConfig), nil
}

// discoverHubbleCertificates attempts to discover Hubble TLS certificates from Kubernetes secrets
func discoverHubbleCertificates(logger *zap.Logger, namespace string) (*HubbleTLSConfig, error) {
	// Load certificates from Kubernetes secrets (GKE Dataplane V2 and standard Cilium)
	if config, err := loadCertificatesFromKubernetesSecrets(logger, namespace); err == nil {
		logger.Info("Loaded Hubble certificates from Kubernetes secrets")
		return config, nil
	} else {
		logger.Debug("Failed to load certificates from Kubernetes secrets", zap.Error(err))
		return nil, fmt.Errorf("no valid Hubble TLS certificates found - ensure hubble-relay-client-certs secret exists in namespace %s: %w", namespace, err)
	}
}

// loadCertificatesFromKubernetesSecrets loads certificates from Kubernetes secrets
func loadCertificatesFromKubernetesSecrets(logger *zap.Logger, namespace string) (*HubbleTLSConfig, error) {
	// Get Kubernetes clientset
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create context with timeout for secret retrieval
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to load from hubble-relay-client-certs secret in the Hubble namespace
	logger.Debug("Attempting to load client certificates from Hubble namespace",
		zap.String("namespace", namespace),
		zap.String("secret_name", hubbleRelayClientSecretName))

	clientSecret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, hubbleRelayClientSecretName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get client secret %s in namespace %s: %w", hubbleRelayClientSecretName, namespace, err)
	}

	// Extract client certificate and key from secret (standard kubernetes.io/tls format)
	clientCert, exists := clientSecret.Data["tls.crt"]
	if !exists || len(clientCert) == 0 {
		return nil, fmt.Errorf("tls.crt not found or empty in secret %s", hubbleRelayClientSecretName)
	}

	clientKey, exists := clientSecret.Data["tls.key"]
	if !exists || len(clientKey) == 0 {
		return nil, fmt.Errorf("tls.key not found or empty in secret %s", hubbleRelayClientSecretName)
	}

	// Try to get CA certificate from client secret first
	caCert, exists := clientSecret.Data["ca.crt"]
	if !exists {
		// If CA cert is not in client secret, try to get it from server secret in the same namespace
		logger.Debug("CA certificate not found in client secret, trying server secret",
			zap.String("server_secret_name", hubbleServerCertsSecretName))

		serverSecret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, hubbleServerCertsSecretName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get server secret %s for CA certificate in namespace %s: %w",
				hubbleServerCertsSecretName, namespace, err)
		}

		caCert, exists = serverSecret.Data["ca.crt"]
		if !exists || len(caCert) == 0 {
			return nil, fmt.Errorf("ca.crt not found or empty in either client secret %s or server secret %s",
				hubbleRelayClientSecretName, hubbleServerCertsSecretName)
		}

		logger.Debug("Successfully loaded CA certificate from server secret", zap.String("server_secret_namespace", namespace))
	} else {
		logger.Debug("Successfully loaded CA certificate from client secret")
	}

	// Server name must match the server certificate, not the client certificate
	// E.g. in GKE Dataplane V2, the server certificate is valid for *.gke-managed-dpv2-observability.svc.cluster.local
	serverName := fmt.Sprintf("hubble-relay.%s.svc.cluster.local", namespace)

	logger.Info("Successfully loaded Hubble certificates from Kubernetes secrets",
		zap.String("client_secret", hubbleRelayClientSecretName),
		zap.String("namespace", namespace),
		zap.String("server_name", serverName))

	return &HubbleTLSConfig{
		ClientCert: clientCert,
		ClientKey:  clientKey,
		CACert:     caCert,
		ServerName: serverName,
	}, nil
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

	logger.Info("Attempting to connect to Hubble relay",
		zap.String("address", hubbleAddress),
		zap.String("namespace", ciliumNamespace))

	var conn *grpc.ClientConn

	// First try to connect with TLS (required for GKE Dataplane V2)
	tlsCreds, tlsErr := loadHubbleTLSCredentials(logger, ciliumNamespace)
	if tlsErr == nil {
		logger.Info("Attempting TLS connection to Hubble relay")
		dialOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(tlsCreds),
		}

		tlsConn, tlsConnErr := grpc.NewClient(hubbleAddress, dialOpts...)
		if tlsConnErr != nil {
			logger.Warn("Failed to create TLS gRPC client, will try insecure connection",
				zap.Error(tlsConnErr))
		} else {
			logger.Info("Successfully established TLS gRPC connection to Hubble relay",
				zap.String("address", hubbleAddress))
			conn = tlsConn
		}
	} else {
		logger.Info("TLS credentials not available, will try insecure connection", zap.Error(tlsErr))
	}

	// Fallback to insecure connection if TLS failed
	if conn == nil {
		logger.Info("Attempting insecure connection to Hubble relay")
		dialOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}

		insecureConn, insecureErr := grpc.NewClient(hubbleAddress, dialOpts...)
		if insecureErr != nil {
			return nil, fmt.Errorf("failed to connect to Cilium Hubble Relay at %s (TLS err: %v, insecure err: %w)", hubbleAddress, tlsErr, insecureErr)
		}

		logger.Info("Successfully established insecure gRPC connection to Hubble relay",
			zap.String("address", hubbleAddress))
		conn = insecureConn
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

	fm.logger.Info("Successfully established flow stream to Hubble relay")
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
		f, err := stream.Recv()
		if err != nil {
			fm.logger.Warn("Failed to get flow log from stream", zap.Error(err))
			return err
		}

		// Skip reply-direction packets; they never need a NetworkPolicy rule.
		// K8s NetworkPolicy is state-aware: once the policy allows the initiating packet,
		// all packets in the reverse / reply direction of that connection are automatically accepted.
		// We only write policies for the request direction (reply traffic matches conntrack and is allowed implicitly).
		if ir := f.GetFlow().GetIsReply(); ir != nil && ir.GetValue() {
			continue
		}

		flowKey := createFlowKey(f.GetFlow())

		// Convert string slice labels to map[string]struct{}
		srcLabels := make(map[string]struct{})
		dstLabels := make(map[string]struct{})
		for _, label := range f.GetFlow().GetSource().GetLabels() {
			srcLabels[label] = struct{}{}
		}
		for _, label := range f.GetFlow().GetDestination().GetLabels() {
			dstLabels[label] = struct{}{}
		}

		ingressAllowedBy := make(map[string]*flow.Policy)
		egressAllowedBy := make(map[string]*flow.Policy)
		for _, policy := range f.GetFlow().GetIngressAllowedBy() {
			ingressAllowedBy[smartcache.PolicyKey(policy)] = policy
		}
		for _, policy := range f.GetFlow().GetEgressAllowedBy() {
			egressAllowedBy[smartcache.PolicyKey(policy)] = policy
		}

		flowMetadata := &smartcache.FlowMetadata{
			FirstSeen:        &timestamppb.Timestamp{},
			LastSeen:         &timestamppb.Timestamp{},
			SourceLabels:     srcLabels,
			DestLabels:       dstLabels,
			IngressAllowedBy: ingressAllowedBy,
			EgressAllowedBy:  egressAllowedBy,
		}
		fm.cache.AddFlowKey(*flowKey, f.GetFlow(), flowMetadata)
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
