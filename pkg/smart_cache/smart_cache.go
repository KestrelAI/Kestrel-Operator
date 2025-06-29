package smartcache

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/cilium/cilium/api/v1/flow"
	v1 "github.com/cilium/cilium/api/v1/flow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//TODO:  I think this cache needs to have an in and out channel to avoid blocking operations.

type FlowMetadata struct {
	FirstSeen        *timestamppb.Timestamp
	LastSeen         *timestamppb.Timestamp
	SourceLabels     map[string]struct{}
	DestLabels       map[string]struct{}
	IngressAllowedBy map[string]*flow.Policy
	EgressAllowedBy  map[string]*flow.Policy
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
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s.%s.%d.%d.%s.%s.%s",
		f.SourceIPAddress,
		f.SourceNamespace,
		f.SourceKind,
		f.SourceName,
		f.DestinationIPAddress,
		f.DestinationNamespace,
		f.DestinationKind,
		f.DestinationName,
		f.SourcePort,
		f.DestinationPort,
		f.Protocol,
		f.Direction,
		f.Verdict)
}

func PolicyKey(p *flow.Policy) string {
	return fmt.Sprintf("%s|%s|%s|%d|%s", p.Namespace, p.Name, p.Kind, p.Revision, strings.Join(p.Labels, ","))
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

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Printf("Purging cache, num flows in cache=%d\n", len(s.FlowKeys))
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
		fmt.Printf("[DEBUG] Sending flow: src=%s/%s dst=%s/%s port=%d count=%d\n",
			flowKey.SourceNamespace, flowKey.SourceName, flowKey.DestinationNamespace, flowKey.DestinationName, flowKey.DestinationPort, flowData.Count)

		select {
		case s.flowChan <- FlowCount{
			FlowKey:      flowKey,
			Count:        flowData.Count,
			Flow:         flowData.Flow,
			FlowMetadata: flowData.FlowMetadata,
		}:
		case <-s.stopCh: // allow graceful shutdown
			return
		}
	}
	// Clear the cache regardless of whether we could send all flows
	s.FlowKeys = make(map[FlowKey]FlowData)
}

func (s *SmartCache) Stop() {
	close(s.stopCh)
}

func (s *SmartCache) AddFlowKey(key FlowKey, f *v1.Flow, flowMetadata *FlowMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get existing data or create new
	fd, exists := s.FlowKeys[key]
	if !exists {
		// Initialize new label sets
		srcLabels := make(map[string]struct{})
		dstLabels := make(map[string]struct{})

		// Add initial labels
		for _, label := range flowMetadata.GetSourceLabelsAsSlice() {
			srcLabels[label] = struct{}{}
		}
		for _, label := range flowMetadata.GetDestLabelsAsSlice() {
			dstLabels[label] = struct{}{}
		}

		// Initialize new network policy maps
		ingressAllowedBy := make(map[string]*flow.Policy)
		egressAllowedBy := make(map[string]*flow.Policy)

		// Add initial network policies that allow the flow, if any
		for _, policy := range flowMetadata.GetIngressAllowedByAsSlice() {
			ingressAllowedBy[PolicyKey(policy)] = policy
		}
		for _, policy := range flowMetadata.GetEgressAllowedByAsSlice() {
			egressAllowedBy[PolicyKey(policy)] = policy
		}

		fd = FlowData{
			Count: 1,
			Flow:  f,
			FlowMetadata: &FlowMetadata{
				FirstSeen:        timestamppb.Now(),
				LastSeen:         timestamppb.Now(),
				SourceLabels:     srcLabels,
				DestLabels:       dstLabels,
				IngressAllowedBy: ingressAllowedBy,
				EgressAllowedBy:  egressAllowedBy,
			},
		}
	} else {
		fd.Count += 1
		fd.Flow = f
		fd.FlowMetadata.LastSeen = timestamppb.Now()

		// Add new labels to existing sets
		for _, label := range flowMetadata.GetSourceLabelsAsSlice() {
			fd.FlowMetadata.SourceLabels[label] = struct{}{}
		}
		for _, label := range flowMetadata.GetDestLabelsAsSlice() {
			fd.FlowMetadata.DestLabels[label] = struct{}{}
		}

		// Add new network policies to existing sets
		for _, policy := range flowMetadata.GetIngressAllowedByAsSlice() {
			fd.FlowMetadata.IngressAllowedBy[PolicyKey(policy)] = policy
		}
		for _, policy := range flowMetadata.GetEgressAllowedByAsSlice() {
			fd.FlowMetadata.EgressAllowedBy[PolicyKey(policy)] = policy
		}
	}
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

// GetFlowCount returns the count for a given flow key and -1 if the key does not exist
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

// GetLabelsAsSlice returns the labels as a slice of strings
func (fm *FlowMetadata) GetSourceLabelsAsSlice() []string {
	labels := make([]string, 0, len(fm.SourceLabels))
	for label := range fm.SourceLabels {
		labels = append(labels, label)
	}
	return labels
}

func (fm *FlowMetadata) GetDestLabelsAsSlice() []string {
	labels := make([]string, 0, len(fm.DestLabels))
	for label := range fm.DestLabels {
		labels = append(labels, label)
	}
	return labels
}

// GetIngressAllowedByAsSlice returns the ingress network policies as a slice of *flow.Policy
func (fm *FlowMetadata) GetIngressAllowedByAsSlice() []*flow.Policy {
	policies := make([]*flow.Policy, 0, len(fm.IngressAllowedBy))
	for _, policy := range fm.IngressAllowedBy {
		policies = append(policies, policy)
	}
	return policies
}

// GetEgressAllowedByAsSlice returns the egress network policies as a slice of *flow.Policy
func (fm *FlowMetadata) GetEgressAllowedByAsSlice() []*flow.Policy {
	policies := make([]*flow.Policy, 0, len(fm.EgressAllowedBy))
	for _, policy := range fm.EgressAllowedBy {
		policies = append(policies, policy)
	}
	return policies
}
