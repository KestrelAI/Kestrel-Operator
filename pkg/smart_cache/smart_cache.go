package smartcache

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// This cache needs to hold flowkeys and flowcount for each flowkey.
type SmartCache struct {
	FlowKeys map[string]int64
	mu       sync.RWMutex
	stopCh   chan struct{}
	flowChan chan FlowKeyCount
}

type FlowKeyCount struct {
	FlowKey string
	Count   int64
}

func InitFlowCache(ctx context.Context, flowChan chan FlowKeyCount) *SmartCache {
	cache := &SmartCache{
		FlowKeys: make(map[string]int64),
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

	ticker := time.NewTicker(1 * time.Minute)
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
	for flowKey, flowCount := range s.FlowKeys {
		s.flowChan <- FlowKeyCount{FlowKey: flowKey, Count: flowCount}
	}
	s.FlowKeys = make(map[string]int64)
}

func (s *SmartCache) Stop() {
	close(s.stopCh)
}

func (s *SmartCache) AddFlowKey(flowKey string, flowCount int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FlowKeys[flowKey] = flowCount
}

func (s *SmartCache) GetFlowKey(flowKey string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count, exists := s.FlowKeys[flowKey]
	return count, exists
}

func (s *SmartCache) RemoveFlowKey(flowKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.FlowKeys, flowKey)
}

func (s *SmartCache) GetAllFlowKeys() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Create a copy of the map to prevent concurrent access issues
	flowKeys := make(map[string]int64, len(s.FlowKeys))
	for k, v := range s.FlowKeys {
		flowKeys[k] = v
	}
	return flowKeys
}

func (s *SmartCache) GetFlowCount(flowKey string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FlowKeys[flowKey]
}

func (s *SmartCache) GetFlowKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	flowKeys := make([]string, 0, len(s.FlowKeys))
	for flowKey := range s.FlowKeys {
		flowKeys = append(flowKeys, flowKey)
	}
	return flowKeys
}
