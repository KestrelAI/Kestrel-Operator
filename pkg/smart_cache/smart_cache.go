package smartcache

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"maps"

	v1 "github.com/cilium/cilium/api/v1/flow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//TODO:  I think this cache needs to have an in and out channel to avoid blocking operations.

type FlowMetadata struct {
	FirstSeen    *timestamppb.Timestamp
	LastSeen     *timestamppb.Timestamp
	SourceLabels []string
	DestLabels   []string
}

type FlowData struct {
	Count        int64
	Flow         *v1.Flow
	FlowMetadata *FlowMetadata
}

// FlowKey represents a unique identifier for a network flow
type FlowKey struct {
	SourceIPAddress      string
	SourceNamespace      string
	SourceKind           string
	SourceName           string
	DestinationIPAddress string
	DestinationNamespace string
	DestinationKind      string
	DestinationName      string
	SourcePort           uint32
	DestinationPort      uint32
	Protocol             string
	Direction            string
	Verdict              string
}

// This cache needs to hold flowkeys and flowcount for each flowkey.
type SmartCache struct {
	FlowKeys map[FlowKey]FlowData
	mu       sync.RWMutex
	stopCh   chan struct{}
	flowChan chan FlowCount
}

type FlowCount struct {
	FlowKey      FlowKey
	Count        int64
	Flow         *v1.Flow
	FlowMetadata *FlowMetadata
}

// String returns a string representation of the FlowKey
func (f FlowKey) String() string {
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s.%d.%d.%s.%s.%s",
		f.SourceNamespace,
		f.SourceKind,
		f.SourceName,
		f.DestinationNamespace,
		f.DestinationKind,
		f.DestinationName,
		f.SourcePort,
		f.DestinationPort,
		f.Protocol,
		f.Direction,
		f.Verdict)
}

// InitFlowCache initializes a new SmartCache with the given flow channel
func InitFlowCache(ctx context.Context, flowChan chan FlowCount) *SmartCache {

	cache := &SmartCache{
		FlowKeys: make(map[FlowKey]FlowData),
		stopCh:   make(chan struct{}),
		flowChan: flowChan,
	}

	// Start the cache purging goroutine with jitter
	go cache.startPurging(ctx)
	return cache
}

func (s *SmartCache) startPurging(ctx context.Context) {
	// Generate random jitter between 1 and 5 seconds
	jitter := time.Duration(rand.Intn(4000)+1000) * time.Millisecond
	select {
	case <-time.After(jitter):
		// Continue with normal operation
	case <-ctx.Done():
		// Context was cancelled during initial jitter
		return
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.purgeCache()
		case <-s.stopCh:
			return
		case <-ctx.Done():
			// Context was cancelled, stop the purging
			return
		}
	}
}

func (s *SmartCache) purgeCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for flowKey, flowData := range s.FlowKeys {
		select {
		case s.flowChan <- FlowCount{
			FlowKey:      flowKey,
			Count:        flowData.Count,
			Flow:         flowData.Flow,
			FlowMetadata: flowData.FlowMetadata,
		}:
		default:
		}
	}
	// Clear the cache regardless of whether we could send all flows
	s.FlowKeys = make(map[FlowKey]FlowData)
}

func (s *SmartCache) Stop() {
	close(s.stopCh)
}

func (s *SmartCache) AddFlowKey(key FlowKey, flow *v1.Flow, flowMetadata *FlowMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fd := s.FlowKeys[key] // if key does not exist, fd.Count will be 0.
	fd.Count += 1
	fd.Flow = flow
	fd.FlowMetadata.LastSeen = timestamppb.Now()
	if fd.FlowMetadata.FirstSeen == nil {
		fd.FlowMetadata.FirstSeen = timestamppb.Now()
	}
	fd.FlowMetadata.SourceLabels = flowMetadata.SourceLabels
	fd.FlowMetadata.DestLabels = flowMetadata.DestLabels
	s.FlowKeys[key] = fd
}

func (s *SmartCache) GetFlowKey(flowKey FlowKey) (FlowData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, exists := s.FlowKeys[flowKey]
	return data, exists
}

func (s *SmartCache) RemoveFlowKey(flowKey FlowKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.FlowKeys, flowKey)
}

func (s *SmartCache) GetAllFlowKeys() map[FlowKey]FlowData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	flowKeys := make(map[FlowKey]FlowData, len(s.FlowKeys))
	maps.Copy(flowKeys, s.FlowKeys)
	return flowKeys
}

// GetFlowCount returns the count for a given flow key and a boolean indicating if the key exists
func (s *SmartCache) GetFlowCount(flowKey FlowKey) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, exists := s.FlowKeys[flowKey]
	if !exists {
		return -1
	}
	return data.Count
}

func (s *SmartCache) GetFlowKeys() []FlowKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	flowKeys := make([]FlowKey, 0, len(s.FlowKeys))
	for flowKey := range s.FlowKeys {
		flowKeys = append(flowKeys, flowKey)
	}
	return flowKeys
}
