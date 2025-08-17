package smartcache

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// L7FlowKey represents a unique identifier for an L7 network flow
// This key is designed to aggregate flows that would result in the same authorization policy
type L7FlowKey struct {
	// Source identification
	SourceNamespace string
	SourceName      string
	SourceKind      string

	// Destination identification
	DestinationNamespace   string
	DestinationServiceName string
	DestinationKind        string
	DestinationPort        uint32

	// L7 Protocol information
	L7Protocol v1.L7ProtocolType

	// HTTP-specific fields (only for HTTP traffic)
	HTTPMethod string
	HTTPPath   string

	// Connection outcome
	Allowed bool
}

// L7Flow represents an aggregated L7 flow ready to be sent to the server
type L7Flow struct {
	Key       L7FlowKey
	AccessLog *v1.L7AccessLog
}

// L7SmartCache manages L7 access log aggregation
type L7SmartCache struct {
	flows    map[L7FlowKey]*v1.L7AccessLog
	mu       sync.RWMutex
	stopCh   chan struct{}
	flowChan chan L7Flow
}

// String returns a string representation of the L7FlowKey
func (k L7FlowKey) String() string {
	return fmt.Sprintf("%s.%s.%s->%s.%s.%s:%d|%s|%s|%s|%t",
		k.SourceNamespace, k.SourceName, k.SourceKind,
		k.DestinationNamespace, k.DestinationServiceName, k.DestinationKind,
		k.DestinationPort, k.L7Protocol.String(), k.HTTPMethod, k.HTTPPath, k.Allowed)
}

// InitL7FlowCache initializes a new L7SmartCache with the given flow channel
func InitL7FlowCache(ctx context.Context, flowChan chan L7Flow) *L7SmartCache {
	cache := &L7SmartCache{
		flows:    make(map[L7FlowKey]*v1.L7AccessLog),
		stopCh:   make(chan struct{}),
		flowChan: flowChan,
	}

	// Start the cache purging goroutine with jitter
	go cache.startPurging(ctx)
	return cache
}

func (c *L7SmartCache) startPurging(ctx context.Context) {
	// Generate random jitter between 1 and 5 seconds
	jitter := time.Duration(rand.Intn(4000)+1000) * time.Millisecond
	select {
	case <-time.After(jitter):
		// Continue with normal operation
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Printf("Purging L7 cache, num flows=%d\n", len(c.flows))
			c.purgeCache()
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *L7SmartCache) purgeCache() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, accessLog := range c.flows {
		fmt.Printf("[DEBUG] Sending L7 flow: %s->%s:%d %s %s count=%d firstSeen=%v lastSeen=%v\n",
			key.SourceNamespace+"/"+key.SourceName,
			key.DestinationNamespace+"/"+key.DestinationServiceName,
			key.DestinationPort, key.HTTPMethod, key.HTTPPath, accessLog.Count, accessLog.FirstSeen, accessLog.LastSeen)

		select {
		case c.flowChan <- L7Flow{
			Key:       key,
			AccessLog: accessLog,
		}:
		case <-c.stopCh:
			return
		}
	}

	// Clear the cache
	c.flows = make(map[L7FlowKey]*v1.L7AccessLog)
}

func (c *L7SmartCache) Stop() {
	close(c.stopCh)
}

// createKey creates a cache key from an L7 access log
func (c *L7SmartCache) createKey(accessLog *v1.L7AccessLog) L7FlowKey {
	key := L7FlowKey{
		L7Protocol: accessLog.L7Protocol,
		Allowed:    accessLog.Allowed,
	}

	if src := accessLog.Source; src != nil {
		key.SourceNamespace = src.Namespace
		key.SourceName = src.Name
		key.SourceKind = src.Kind
	}

	if dst := accessLog.Destination; dst != nil {
		key.DestinationNamespace = dst.Namespace
		key.DestinationServiceName = dst.ServiceName
		key.DestinationKind = dst.Kind
		key.DestinationPort = dst.Port
	}

	// Include HTTP method and path for HTTP traffic
	if httpData := accessLog.HttpData; httpData != nil && accessLog.L7Protocol == v1.L7ProtocolType_L7_PROTOCOL_HTTP {
		key.HTTPMethod = httpData.Method
		key.HTTPPath = normalizePath(httpData.Path)
	}

	return key
}

// AddL7AccessLog adds an L7 access log to the cache, aggregating with existing entries
func (c *L7SmartCache) AddL7AccessLog(accessLog *v1.L7AccessLog) {
	if accessLog == nil {
		return
	}

	key := c.createKey(accessLog)

	c.mu.Lock()
	defer c.mu.Unlock()

	existing, exists := c.flows[key]
	if !exists {
		// First occurrence - initialize aggregation fields
		accessLog.Count = 1
		accessLog.FirstSeen = timestamppb.Now()
		accessLog.LastSeen = timestamppb.Now()
		c.flows[key] = accessLog
	} else {
		// Simple aggregation - just update the counters and timestamps
		existing.Count++
		existing.LastSeen = timestamppb.Now()
		existing.BytesSent += accessLog.BytesSent
		existing.BytesReceived += accessLog.BytesReceived
		existing.DurationMs += accessLog.DurationMs
	}
}

// normalizePath normalizes HTTP paths for better aggregation
func normalizePath(path string) string {
	if path == "" {
		return "/"
	}

	// Remove query parameters
	if idx := strings.Index(path, "?"); idx != -1 {
		path = path[:idx]
	}

	// Remove trailing slashes except for root
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	// Replace numeric IDs and UUIDs with placeholders for better grouping
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if len(part) > 0 && isNumeric(part) {
			parts[i] = ":id"
		} else if len(part) == 36 && strings.Count(part, "-") == 4 {
			parts[i] = ":uuid"
		}
	}

	return strings.Join(parts, "/")
}

// isNumeric checks if a string contains only digits
func isNumeric(s string) bool {
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return len(s) > 0
}

// GetAccessLog returns the access log for a given key
func (c *L7SmartCache) GetAccessLog(key L7FlowKey) (*v1.L7AccessLog, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	accessLog, exists := c.flows[key]
	return accessLog, exists
}

// GetAllFlows returns all flows in the cache
func (c *L7SmartCache) GetAllFlows() map[L7FlowKey]*v1.L7AccessLog {
	c.mu.RLock()
	defer c.mu.RUnlock()
	flows := make(map[L7FlowKey]*v1.L7AccessLog, len(c.flows))
	for k, v := range c.flows {
		flows[k] = v
	}
	return flows
}

// GetCount returns the aggregated count for a given flow key
func (c *L7SmartCache) GetCount(key L7FlowKey) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if accessLog, exists := c.flows[key]; exists {
		return accessLog.Count
	}
	return 0
}
