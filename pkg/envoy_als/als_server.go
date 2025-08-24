package envoy_als

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	accesslogdata "github.com/envoyproxy/go-control-plane/envoy/data/accesslog/v3"
	accesslogv3 "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "operator/api/gen/cloud/v1"
)

// ALSServer implements the Envoy Access Log Service
type ALSServer struct {
	accesslogv3.UnimplementedAccessLogServiceServer
	logger     *zap.Logger
	l7LogChan  chan *v1.L7AccessLog
	serverPort int
	// streamNodes stores node IDs per stream to handle identifier-only-in-first-message optimization
	streamNodes sync.Map // map[stream]*string
}

// NewALSServer creates a new Access Log Service server
func NewALSServer(logger *zap.Logger, l7LogChan chan *v1.L7AccessLog, port int) *ALSServer {
	return &ALSServer{
		logger:     logger,
		l7LogChan:  l7LogChan,
		serverPort: port,
	}
}

// StreamAccessLogs implements the gRPC streaming interface for Envoy access logs
func (s *ALSServer) StreamAccessLogs(stream accesslogv3.AccessLogService_StreamAccessLogsServer) error {
	s.logger.Info("New Envoy access log stream connected")

	// Store stream reference for node tracking
	defer func() {
		// Clean up node tracking when stream ends
		s.streamNodes.Delete(stream)
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			s.logger.Info("Access log stream ended", zap.Error(err))
			return err
		}

		s.processAccessLogRequest(req, stream)
	}
}

// processAccessLogRequest processes incoming access log entries and converts them to L7 access logs
func (s *ALSServer) processAccessLogRequest(req *accesslogv3.StreamAccessLogsMessage, stream accesslogv3.AccessLogService_StreamAccessLogsServer) {
	s.logger.Debug("Processing access log request",
		zap.Bool("has_identifier", req.Identifier != nil),
		zap.Bool("has_http_logs", req.GetHttpLogs() != nil),
		zap.Bool("has_tcp_logs", req.GetTcpLogs() != nil))

	nodeName := "unknown"

	// Check if identifier is present (first message on stream)
	if req.Identifier != nil && req.Identifier.Node != nil && req.Identifier.Node.Id != "" {
		nodeName = req.Identifier.Node.Id
		// Store node name for this stream for subsequent messages
		s.streamNodes.Store(stream, nodeName)
		s.logger.Info("Stored node name for stream", zap.String("node_id", nodeName))
	} else {
		// Try to get stored node name for this stream
		if storedNodeName, exists := s.streamNodes.Load(stream); exists {
			if name, ok := storedNodeName.(string); ok {
				nodeName = name
			}
		}
	}

	// Track if we successfully processed any HTTP logs to avoid duplicate TCP processing
	httpLogsProcessed := 0

	// Process HTTP access log entries (these are actual HTTP traffic detected by Envoy)
	if httpLogs := req.GetHttpLogs(); httpLogs != nil {
		s.logger.Debug("Processing HTTP logs", zap.Int("count", len(httpLogs.LogEntry)))
		for _, logEntry := range httpLogs.LogEntry {
			// Only process if this is actually valid HTTP traffic
			if s.isValidHTTPTraffic(logEntry) {
				l7Log := s.convertHTTPLogToL7AccessLog(logEntry, nodeName)
				if l7Log != nil {
					select {
					case s.l7LogChan <- l7Log:
						s.logger.Debug("Successfully sent HTTP L7 log to channel")
						httpLogsProcessed++
					default:
						s.logger.Warn("L7 log channel full, dropping log")
					}
				}
			} else {
				s.logger.Debug("Skipping invalid HTTP traffic entry - treating as raw TCP instead")
			}
		}
	}

	// Process TCP access log entries (for non-HTTP traffic or when HTTP processing failed)
	if tcpLogs := req.GetTcpLogs(); tcpLogs != nil {
		s.logger.Debug("Processing TCP logs", zap.Int("count", len(tcpLogs.LogEntry)))

		// If we successfully processed HTTP logs, we typically want to skip TCP logs for the same connections
		// to avoid duplicates. However, we'll still process TCP logs if no valid HTTP logs were found.
		shouldProcessTCP := httpLogsProcessed == 0 || req.GetHttpLogs() == nil

		if !shouldProcessTCP {
			s.logger.Debug("Skipping TCP logs - already processed valid HTTP logs for this request")
		} else {
			for _, logEntry := range tcpLogs.LogEntry {
				l7Log := s.convertTCPLogToL7AccessLog(logEntry, nodeName)
				if l7Log != nil {
					select {
					case s.l7LogChan <- l7Log:
						s.logger.Debug("Successfully sent TCP L7 log to channel")
					default:
						s.logger.Warn("L7 log channel full, dropping log")
					}
				}
			}
		}
	}

	// Log if neither HTTP nor TCP logs were present
	if req.GetHttpLogs() == nil && req.GetTcpLogs() == nil {
		s.logger.Debug("Received access log request with no HTTP or TCP logs - likely identifier-only message")
	}
}

// convertHTTPLogToL7AccessLog converts an Envoy HTTP access log to L7AccessLog format
func (s *ALSServer) convertHTTPLogToL7AccessLog(httpLog *accesslogdata.HTTPAccessLogEntry, nodeName string) *v1.L7AccessLog {
	if httpLog.Request == nil || httpLog.CommonProperties == nil {
		s.logger.Warn("HTTP log entry missing required fields",
			zap.Bool("has_request", httpLog.Request != nil),
			zap.Bool("has_common_properties", httpLog.CommonProperties != nil))
		return nil
	}

	// Extract timestamp
	timestamp := time.Now()
	if httpLog.CommonProperties.StartTime != nil {
		timestamp = httpLog.CommonProperties.StartTime.AsTime()
	}

	// Extract source and destination information
	source := s.extractL7SourceInfo(httpLog.CommonProperties, nodeName)
	destination := s.extractL7DestinationInfo(httpLog.CommonProperties, httpLog.Request)

	// Extract HTTP-specific data
	httpData := &v1.HTTPAccessLogData{
		Method:          httpLog.Request.RequestMethod.String(),
		Path:            httpLog.Request.Path,
		Version:         s.extractHTTPVersion(httpLog.ProtocolVersion),
		Host:            httpLog.Request.Authority,
		UserAgent:       s.extractHeaderValue(httpLog.Request.RequestHeaders, "user-agent"),
		Referer:         s.extractHeaderValue(httpLog.Request.RequestHeaders, "referer"),
		ResponseCode:    0,
		RequestSize:     httpLog.Request.RequestBodyBytes,
		ResponseSize:    0,
		RequestHeaders:  s.convertHeaders(httpLog.Request.RequestHeaders),
		ResponseHeaders: make(map[string]string),
		// CommonProperties fields for consistency
		ConnectionTerminationDetails:     httpLog.CommonProperties.ConnectionTerminationDetails,
		UpstreamTransportFailureReason:   httpLog.CommonProperties.UpstreamTransportFailureReason,
		DownstreamTransportFailureReason: httpLog.CommonProperties.DownstreamTransportFailureReason,
		AccessLogType:                    s.convertAccessLogType(httpLog.CommonProperties.AccessLogType),
		StreamId:                         httpLog.CommonProperties.StreamId,
		UpstreamRequestAttemptCount:      httpLog.CommonProperties.UpstreamRequestAttemptCount,
	}

	// Extract response data if available
	if httpLog.Response != nil {
		httpData.ResponseCode = httpLog.Response.ResponseCode.GetValue()
		httpData.ResponseSize = httpLog.Response.ResponseBodyBytes
		httpData.ResponseHeaders = s.convertHeaders(httpLog.Response.ResponseHeaders)
	}

	// Extract response flags
	if httpLog.CommonProperties.ResponseFlags != nil {
		httpData.ResponseFlags = s.extractResponseFlags(httpLog.CommonProperties.ResponseFlags)
	}

	// Calculate duration
	var durationMs int64
	if httpLog.CommonProperties.Duration != nil {
		durationMs = httpLog.CommonProperties.Duration.AsDuration().Milliseconds()
	}

	// Determine if request was allowed by checking for authorization denial flags
	// If the request reached the application and got a response, it was allowed by Istio policies
	// Authorization denials would typically result in no response or specific response flags
	allowed := true
	if httpLog.CommonProperties.ResponseFlags != nil {
		if httpLog.CommonProperties.ResponseFlags.GetUnauthorizedDetails() != nil {
			allowed = false
		}
		// Also check for other denial indicators
		if httpLog.CommonProperties.ResponseFlags.GetNoRouteFound() ||
			httpLog.CommonProperties.ResponseFlags.GetNoClusterFound() {
			allowed = false
		}
	}

	// Filter out logs with internal Istio/Envoy service destinations that cannot have authorization policies
	if destination != nil && s.isInternalIstioService(destination.ServiceName) {
		s.logger.Debug("Skipping access log for internal Istio service", zap.String("service", destination.ServiceName))
		return nil
	}

	l7Log := &v1.L7AccessLog{
		Timestamp:     timestamppb.New(timestamp),
		Source:        source,
		Destination:   destination,
		Protocol:      "TCP",
		L7Protocol:    v1.L7ProtocolType_L7_PROTOCOL_HTTP,
		HttpData:      httpData,
		DurationMs:    durationMs,
		BytesSent:     httpLog.CommonProperties.UpstreamWireBytesSent,
		BytesReceived: httpLog.CommonProperties.UpstreamWireBytesReceived,
		Allowed:       allowed,
		NodeId:        nodeName,
		ClusterName:   httpLog.CommonProperties.UpstreamCluster,
	}

	s.logger.Debug("Converted HTTP access log to L7AccessLog",
		zap.String("method", httpData.Method),
		zap.String("path", httpData.Path),
		zap.String("host", httpData.Host),
		zap.Uint32("status", httpData.ResponseCode),
		zap.Bool("allowed", allowed))

	return l7Log
}

// convertTCPLogToL7AccessLog converts an Envoy TCP access log to L7AccessLog format
func (s *ALSServer) convertTCPLogToL7AccessLog(tcpLog *accesslogdata.TCPAccessLogEntry, nodeName string) *v1.L7AccessLog {
	if tcpLog.CommonProperties == nil {
		s.logger.Warn("TCP log entry missing common properties")
		return nil
	}

	// Extract timestamp
	timestamp := time.Now()
	if tcpLog.CommonProperties.StartTime != nil {
		timestamp = tcpLog.CommonProperties.StartTime.AsTime()
	}

	// Extract source and destination information
	source := s.extractL7SourceInfoTCP(tcpLog.CommonProperties, nodeName)
	destination := s.extractL7DestinationInfoTCP(tcpLog.CommonProperties)

	// Extract TCP-specific data from ConnectionProperties and CommonProperties
	tcpData := &v1.TCPAccessLogData{
		ConnectionState:                  s.deriveConnectionState(tcpLog.CommonProperties),
		ReceivedBytes:                    0, // Will be populated from ConnectionProperties
		SentBytes:                        0, // Will be populated from ConnectionProperties
		ConnectionTerminationDetails:     tcpLog.CommonProperties.ConnectionTerminationDetails,
		UpstreamTransportFailureReason:   tcpLog.CommonProperties.UpstreamTransportFailureReason,
		DownstreamTransportFailureReason: tcpLog.CommonProperties.DownstreamTransportFailureReason,
		AccessLogType:                    s.convertAccessLogType(tcpLog.CommonProperties.AccessLogType),
		StreamId:                         tcpLog.CommonProperties.StreamId,
		UpstreamRequestAttemptCount:      tcpLog.CommonProperties.UpstreamRequestAttemptCount,
	}

	// Populate ConnectionProperties data if available
	if tcpLog.ConnectionProperties != nil {
		tcpData.ReceivedBytes = tcpLog.ConnectionProperties.ReceivedBytes
		tcpData.SentBytes = tcpLog.ConnectionProperties.SentBytes
	}

	// Filter out logs with internal Istio/Envoy service destinations that cannot have authorization policies
	if destination != nil && s.isInternalIstioService(destination.ServiceName) {
		s.logger.Debug("Skipping TCP access log for internal Istio service", zap.String("service", destination.ServiceName))
		return nil
	}

	// Calculate duration
	var durationMs int64
	if tcpLog.CommonProperties.Duration != nil {
		durationMs = tcpLog.CommonProperties.Duration.AsDuration().Milliseconds()
	}

	l7Log := &v1.L7AccessLog{
		Timestamp:     timestamppb.New(timestamp),
		Source:        source,
		Destination:   destination,
		Protocol:      "TCP",
		L7Protocol:    v1.L7ProtocolType_L7_PROTOCOL_TCP,
		TcpData:       tcpData,
		DurationMs:    durationMs,
		BytesSent:     tcpLog.CommonProperties.UpstreamWireBytesSent,
		BytesReceived: tcpLog.CommonProperties.UpstreamWireBytesReceived,
		Allowed:       s.isTCPConnectionAllowed(tcpLog.CommonProperties), // Check if TCP connection was allowed
		NodeId:        nodeName,
		ClusterName:   tcpLog.CommonProperties.UpstreamCluster,
	}

	s.logger.Debug("Converted TCP access log to L7AccessLog",
		zap.String("src_ip", source.Ip),
		zap.String("dst_ip", destination.Ip),
		zap.Uint32("dst_port", destination.Port),
		zap.Int64("duration_ms", durationMs))

	return l7Log
}

// isValidHTTPTraffic checks if an HTTP log entry represents actual HTTP traffic
func (s *ALSServer) isValidHTTPTraffic(httpLog *accesslogdata.HTTPAccessLogEntry) bool {
	// If Envoy sent us an HTTP log, it detected HTTP traffic.
	// Just do basic sanity checks.
	return httpLog.Request != nil && httpLog.CommonProperties != nil
}

// extractL7SourceInfo extracts source information for L7 logs
func (s *ALSServer) extractL7SourceInfo(props *accesslogdata.AccessLogCommon, nodeName string) *v1.L7Endpoint {
	endpoint := &v1.L7Endpoint{
		Kind: "Pod",
	}

	// Extract source IP and port
	if props.DownstreamRemoteAddress != nil {
		if socketAddr := props.DownstreamRemoteAddress.GetSocketAddress(); socketAddr != nil {
			endpoint.Ip = socketAddr.Address
			endpoint.Port = uint32(socketAddr.GetPortValue())
		}
	}

	// Extract namespace and workload from node ID if possible
	// Node ID format in Istio: sidecar~<IP>~<workload>~<namespace>.svc.cluster.local
	namespace, workload := s.parseNodeID(nodeName)
	endpoint.Namespace = namespace
	endpoint.Name = workload

	return endpoint
}

// extractL7DestinationInfo extracts destination information for HTTP logs
func (s *ALSServer) extractL7DestinationInfo(props *accesslogdata.AccessLogCommon, req *accesslogdata.HTTPRequestProperties) *v1.L7Endpoint {
	endpoint := &v1.L7Endpoint{
		Kind: "Service",
	}

	// Extract destination IP and port
	if props.UpstreamRemoteAddress != nil {
		if socketAddr := props.UpstreamRemoteAddress.GetSocketAddress(); socketAddr != nil {
			endpoint.Ip = socketAddr.Address
			endpoint.Port = uint32(socketAddr.GetPortValue())
		}
	}

	// Parse service info from UpstreamCluster (most reliable source)
	// Format: outbound|8000||httpbin.foo.svc.cluster.local or inbound|8000||
	serviceName, namespace := s.parseUpstreamCluster(props.UpstreamCluster)
	if serviceName != "" {
		endpoint.ServiceName = serviceName
		endpoint.Name = serviceName
		endpoint.Namespace = namespace
	} else {
		// Fallback to Authority header parsing
		serviceName, namespace = s.parseServiceFromAuthority(req.Authority)
		endpoint.ServiceName = serviceName
		endpoint.Name = serviceName
		endpoint.Namespace = namespace
	}

	return endpoint
}

// extractL7SourceInfoTCP extracts source information for TCP logs
func (s *ALSServer) extractL7SourceInfoTCP(props *accesslogdata.AccessLogCommon, nodeName string) *v1.L7Endpoint {
	endpoint := &v1.L7Endpoint{
		Kind: "Pod",
	}

	// Extract source IP and port
	if props.DownstreamRemoteAddress != nil {
		if socketAddr := props.DownstreamRemoteAddress.GetSocketAddress(); socketAddr != nil {
			endpoint.Ip = socketAddr.Address
			endpoint.Port = uint32(socketAddr.GetPortValue())
		}
	}

	// Extract namespace and workload from node ID
	namespace, workload := s.parseNodeID(nodeName)
	endpoint.Namespace = namespace
	endpoint.Name = workload

	return endpoint
}

// extractL7DestinationInfoTCP extracts destination information for TCP logs
func (s *ALSServer) extractL7DestinationInfoTCP(props *accesslogdata.AccessLogCommon) *v1.L7Endpoint {
	endpoint := &v1.L7Endpoint{
		Kind: "Service",
	}

	// Extract destination IP and port
	if props.UpstreamRemoteAddress != nil {
		if socketAddr := props.UpstreamRemoteAddress.GetSocketAddress(); socketAddr != nil {
			endpoint.Ip = socketAddr.Address
			endpoint.Port = uint32(socketAddr.GetPortValue())
		}
	}

	// Parse service info from UpstreamCluster
	serviceName, namespace := s.parseUpstreamCluster(props.UpstreamCluster)
	endpoint.ServiceName = serviceName
	endpoint.Name = serviceName
	endpoint.Namespace = namespace

	return endpoint
}

// parseNodeID extracts namespace and workload from Istio node ID
// Format: sidecar~<IP>~<workload>~<namespace>.svc.cluster.local
func (s *ALSServer) parseNodeID(nodeID string) (namespace, workload string) {
	if nodeID == "" || nodeID == "unknown" {
		return "default", "unknown-workload"
	}

	// Split by ~ separator
	parts := strings.Split(nodeID, "~")
	if len(parts) >= 4 {
		workload = parts[2]
		// Parse namespace from parts[3] which might be like "default.svc.cluster.local"
		nsParts := strings.Split(parts[3], ".")
		if len(nsParts) > 0 {
			namespace = nsParts[0]
		}
	} else {
		// Fallback: try to extract workload from simple format
		parts = strings.Split(nodeID, "-")
		if len(parts) >= 2 {
			workload = parts[0]
		} else {
			workload = nodeID
		}
	}

	if namespace == "" {
		namespace = "default"
	}
	if workload == "" {
		workload = "unknown-workload"
	}

	return namespace, workload
}

// parseUpstreamCluster extracts service name and namespace from upstream cluster
// Format examples:
// - outbound|8000||httpbin.foo.svc.cluster.local
// - inbound|8000||
// - outbound|80||kubernetes.default.svc.cluster.local
// - PassthroughCluster (for direct IP access)
// - InboundPassthroughCluster (for inbound direct access)
func (s *ALSServer) parseUpstreamCluster(upstreamCluster string) (serviceName, namespace string) {
	if upstreamCluster == "" {
		return "", "default"
	}

	// Handle special cluster names
	switch upstreamCluster {
	case "PassthroughCluster":
		return "passthrough-cluster", "istio-system"
	case "InboundPassthroughCluster":
		return "inbound-passthrough-cluster", "istio-system"
	case "BlackHoleCluster":
		return "blackhole-cluster", "istio-system"
	}

	// Split by | separator
	parts := strings.Split(upstreamCluster, "|")
	if len(parts) >= 4 {
		// parts[0] = outbound/inbound
		// parts[1] = port
		// parts[2] = subset (usually empty)
		// parts[3] = service FQDN
		serviceFQDN := parts[3]

		if serviceFQDN == "" {
			// Handle inbound case where FQDN is empty
			// Try to determine from the cluster direction
			if len(parts) >= 1 && parts[0] == "inbound" {
				return "inbound-service", "default"
			}
			return "unknown-service", "default"
		}

		// Parse service.namespace.svc.cluster.local
		serviceParts := strings.Split(serviceFQDN, ".")
		if len(serviceParts) >= 2 {
			serviceName = serviceParts[0]
			namespace = serviceParts[1]
		} else {
			serviceName = serviceFQDN
			namespace = "default"
		}
	} else {
		// Fallback: treat the whole string as service name
		serviceName = upstreamCluster
		namespace = "default"
	}

	if serviceName == "" {
		serviceName = "unknown-service"
	}
	if namespace == "" {
		namespace = "default"
	}

	return serviceName, namespace
}

func (s *ALSServer) parseServiceFromAuthority(authority string) (string, string) {
	serviceName := "unknown"
	namespace := "default"

	if strings.Contains(authority, ".svc.cluster.local") {
		parts := strings.Split(authority, ".")
		if len(parts) >= 2 {
			serviceName = parts[0]
			namespace = parts[1]
		}
	} else {
		serviceName = strings.Split(authority, ":")[0] // Remove port if present
	}

	return serviceName, namespace
}

func (s *ALSServer) extractHeaderValue(headers map[string]string, key string) string {
	if headers == nil {
		return ""
	}

	// Try both lowercase and original case
	if val, exists := headers[key]; exists {
		return val
	}
	if val, exists := headers[strings.ToLower(key)]; exists {
		return val
	}

	return ""
}

func (s *ALSServer) convertHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return make(map[string]string)
	}

	result := make(map[string]string)
	for k, v := range headers {
		result[k] = v
	}
	return result
}

func (s *ALSServer) extractResponseFlags(flags *accesslogdata.ResponseFlags) []string {
	var responseFlags []string

	if flags == nil {
		return responseFlags
	}

	// HTTP and TCP flags
	if flags.GetNoHealthyUpstream() {
		responseFlags = append(responseFlags, "UH") // NoHealthyUpstream
	}
	if flags.GetUpstreamConnectionFailure() {
		responseFlags = append(responseFlags, "UF") // UpstreamConnectionFailure
	}
	if flags.GetUpstreamOverflow() {
		responseFlags = append(responseFlags, "UO") // UpstreamOverflow
	}
	if flags.GetNoRouteFound() {
		responseFlags = append(responseFlags, "NR") // NoRouteFound
	}
	if flags.GetUpstreamRetryLimitExceeded() {
		responseFlags = append(responseFlags, "URX") // UpstreamRetryLimitExceeded
	}
	if flags.GetNoClusterFound() {
		responseFlags = append(responseFlags, "NC") // NoClusterFound
	}
	if flags.GetDurationTimeout() {
		responseFlags = append(responseFlags, "DT") // DurationTimeout
	}

	// HTTP only flags
	if flags.GetDownstreamConnectionTermination() {
		responseFlags = append(responseFlags, "DC") // DownstreamConnectionTermination
	}
	if flags.GetFailedLocalHealthcheck() {
		responseFlags = append(responseFlags, "LH") // FailedLocalHealthCheck
	}
	if flags.GetUpstreamRequestTimeout() {
		responseFlags = append(responseFlags, "UT") // UpstreamRequestTimeout
	}
	if flags.GetLocalReset() {
		responseFlags = append(responseFlags, "LR") // LocalReset
	}
	if flags.GetUpstreamRemoteReset() {
		responseFlags = append(responseFlags, "UR") // UpstreamRemoteReset
	}
	if flags.GetUpstreamConnectionTermination() {
		responseFlags = append(responseFlags, "UC") // UpstreamConnectionTermination
	}
	if flags.GetDelayInjected() {
		responseFlags = append(responseFlags, "DI") // DelayInjected
	}
	if flags.GetFaultInjected() {
		responseFlags = append(responseFlags, "FI") // FaultInjected
	}
	if flags.GetRateLimited() {
		responseFlags = append(responseFlags, "RL") // RateLimited
	}
	if flags.GetUnauthorizedDetails() != nil {
		responseFlags = append(responseFlags, "UAEX") // UnauthorizedExternalService
	}
	if flags.GetRateLimitServiceError() {
		responseFlags = append(responseFlags, "RLSE") // RateLimitServiceError
	}
	if flags.GetInvalidEnvoyRequestHeaders() {
		responseFlags = append(responseFlags, "IH") // InvalidEnvoyRequestHeaders
	}
	if flags.GetStreamIdleTimeout() {
		responseFlags = append(responseFlags, "SI") // StreamIdleTimeout
	}
	if flags.GetDownstreamProtocolError() {
		responseFlags = append(responseFlags, "DPE") // DownstreamProtocolError
	}
	if flags.GetUpstreamProtocolError() {
		responseFlags = append(responseFlags, "UPE") // UpstreamProtocolError
	}
	if flags.GetUpstreamMaxStreamDurationReached() {
		responseFlags = append(responseFlags, "UMSDR") // UpstreamMaxStreamDurationReached
	}
	if flags.GetResponseFromCacheFilter() {
		responseFlags = append(responseFlags, "RFCF") // ResponseFromCacheFilter
	}
	if flags.GetNoFilterConfigFound() {
		responseFlags = append(responseFlags, "NFCF") // NoFilterConfigFound
	}
	if flags.GetOverloadManager() {
		responseFlags = append(responseFlags, "OM") // OverloadManagerTerminated
	}
	if flags.GetDnsResolutionFailure() {
		responseFlags = append(responseFlags, "DF") // DnsResolutionFailed
	}
	if flags.GetDownstreamRemoteReset() {
		responseFlags = append(responseFlags, "DR") // DownstreamRemoteReset
	}

	return responseFlags
}

// extractHTTPVersion converts Envoy HTTP protocol version enum to string
func (s *ALSServer) extractHTTPVersion(version accesslogdata.HTTPAccessLogEntry_HTTPVersion) string {
	switch version {
	case accesslogdata.HTTPAccessLogEntry_HTTP10:
		return "1.0"
	case accesslogdata.HTTPAccessLogEntry_HTTP11:
		return "1.1"
	case accesslogdata.HTTPAccessLogEntry_HTTP2:
		return "2.0"
	case accesslogdata.HTTPAccessLogEntry_HTTP3:
		return "3.0"
	default:
		return "unknown"
	}
}

// deriveConnectionState determines connection state from access log type and termination details
func (s *ALSServer) deriveConnectionState(props *accesslogdata.AccessLogCommon) string {
	// Use access log type to infer connection state
	switch props.AccessLogType {
	case accesslogdata.AccessLogType_TcpUpstreamConnected:
		return "CONNECTED"
	case accesslogdata.AccessLogType_TcpConnectionEnd:
		return "CLOSED"
	case accesslogdata.AccessLogType_TcpPeriodic:
		return "ESTABLISHED"
	case accesslogdata.AccessLogType_DownstreamStart:
		return "STARTING"
	case accesslogdata.AccessLogType_DownstreamEnd:
		return "ENDED"
	default:
		// Fallback: use termination details if available
		if props.ConnectionTerminationDetails != "" {
			return "TERMINATED"
		}
		return "ESTABLISHED"
	}
}

// convertAccessLogType converts Envoy AccessLogType enum to string
func (s *ALSServer) convertAccessLogType(logType accesslogdata.AccessLogType) string {
	switch logType {
	case accesslogdata.AccessLogType_NotSet:
		return "NotSet"
	case accesslogdata.AccessLogType_TcpUpstreamConnected:
		return "TcpUpstreamConnected"
	case accesslogdata.AccessLogType_TcpPeriodic:
		return "TcpPeriodic"
	case accesslogdata.AccessLogType_TcpConnectionEnd:
		return "TcpConnectionEnd"
	case accesslogdata.AccessLogType_DownstreamStart:
		return "DownstreamStart"
	case accesslogdata.AccessLogType_DownstreamPeriodic:
		return "DownstreamPeriodic"
	case accesslogdata.AccessLogType_DownstreamEnd:
		return "DownstreamEnd"
	case accesslogdata.AccessLogType_UpstreamPoolReady:
		return "UpstreamPoolReady"
	case accesslogdata.AccessLogType_UpstreamPeriodic:
		return "UpstreamPeriodic"
	case accesslogdata.AccessLogType_UpstreamEnd:
		return "UpstreamEnd"
	case accesslogdata.AccessLogType_DownstreamTunnelSuccessfullyEstablished:
		return "DownstreamTunnelSuccessfullyEstablished"
	case accesslogdata.AccessLogType_UdpTunnelUpstreamConnected:
		return "UdpTunnelUpstreamConnected"
	case accesslogdata.AccessLogType_UdpPeriodic:
		return "UdpPeriodic"
	case accesslogdata.AccessLogType_UdpSessionEnd:
		return "UdpSessionEnd"
	default:
		return "Unknown"
	}
}

// isInternalIstioService checks if a service name represents an internal Istio/Envoy concept
// that should not have authorization policies defined on it
func (s *ALSServer) isInternalIstioService(serviceName string) bool {
	switch serviceName {
	case "inbound-service":
		return true
	case "passthrough-cluster":
		return true
	case "inbound-passthrough-cluster":
		return true
	case "blackhole-cluster":
		return true
	case "unknown-service":
		return true
	default:
		return false
	}
}

// isTCPConnectionAllowed determines if a TCP connection was allowed based on response flags
func (s *ALSServer) isTCPConnectionAllowed(props *accesslogdata.AccessLogCommon) bool {
	if props.ResponseFlags != nil {
		// Check for denial indicators
		if props.ResponseFlags.GetUnauthorizedDetails() != nil ||
			props.ResponseFlags.GetNoRouteFound() ||
			props.ResponseFlags.GetNoClusterFound() ||
			props.ResponseFlags.GetUpstreamConnectionFailure() {
			return false
		}
	}

	// If we have a TCP log entry, the connection was established and allowed
	return true
}

// StartServer starts the ALS gRPC server
func (s *ALSServer) StartServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.serverPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.serverPort, err)
	}

	grpcServer := grpc.NewServer()
	accesslogv3.RegisterAccessLogServiceServer(grpcServer, s)

	s.logger.Info("Envoy Access Log Service listening on port",
		zap.Int("port", s.serverPort))

	go func() {
		<-ctx.Done()
		s.logger.Info("Shutting down ALS server")
		grpcServer.GracefulStop()
	}()

	s.logger.Info("Envoy Access Log Service server starting to serve requests")
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve ALS: %w", err)
	}

	return nil
}
