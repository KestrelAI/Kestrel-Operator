package metrics_store

import (
	"testing"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRingBuffer_Push(t *testing.T) {
	rb := NewRingBuffer(3)

	// Test pushing to empty buffer
	dp1 := &v1.OTelMetricDataPoint{
		Timestamp: timestamppb.New(time.Now()),
		Value:     1.0,
	}
	rb.Push(dp1)

	if rb.Count() != 1 {
		t.Errorf("Expected count 1, got %d", rb.Count())
	}

	// Test pushing more items
	dp2 := &v1.OTelMetricDataPoint{
		Timestamp: timestamppb.New(time.Now().Add(time.Second)),
		Value:     2.0,
	}
	dp3 := &v1.OTelMetricDataPoint{
		Timestamp: timestamppb.New(time.Now().Add(2 * time.Second)),
		Value:     3.0,
	}
	rb.Push(dp2)
	rb.Push(dp3)

	if rb.Count() != 3 {
		t.Errorf("Expected count 3, got %d", rb.Count())
	}
	if !rb.IsFull() {
		t.Error("Buffer should be full")
	}

	// Test overflow - should overwrite oldest
	dp4 := &v1.OTelMetricDataPoint{
		Timestamp: timestamppb.New(time.Now().Add(3 * time.Second)),
		Value:     4.0,
	}
	rb.Push(dp4)

	if rb.Count() != 3 {
		t.Errorf("Expected count 3 after overflow, got %d", rb.Count())
	}

	// Verify oldest is now dp2 (dp1 was overwritten)
	all := rb.All()
	if len(all) != 3 {
		t.Errorf("Expected 3 items, got %d", len(all))
	}
	if all[0].Value != 2.0 {
		t.Errorf("Expected oldest value 2.0, got %f", all[0].Value)
	}
	if all[2].Value != 4.0 {
		t.Errorf("Expected newest value 4.0, got %f", all[2].Value)
	}
}

func TestRingBuffer_Range(t *testing.T) {
	rb := NewRingBuffer(10)

	baseTime := time.Now()
	for i := 0; i < 5; i++ {
		dp := &v1.OTelMetricDataPoint{
			Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute)),
			Value:     float64(i),
		}
		rb.Push(dp)
	}

	// Query range that includes all points
	results := rb.Range(baseTime, baseTime.Add(5*time.Minute))
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Query range that includes subset
	results = rb.Range(baseTime.Add(time.Minute), baseTime.Add(3*time.Minute))
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Query range with no matches
	results = rb.Range(baseTime.Add(10*time.Minute), baseTime.Add(15*time.Minute))
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestRingBuffer_Timestamps(t *testing.T) {
	rb := NewRingBuffer(5)

	if !rb.NewestTimestamp().IsZero() {
		t.Error("Expected zero timestamp for empty buffer")
	}
	if !rb.OldestTimestamp().IsZero() {
		t.Error("Expected zero timestamp for empty buffer")
	}

	baseTime := time.Now()
	for i := 0; i < 3; i++ {
		dp := &v1.OTelMetricDataPoint{
			Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute)),
			Value:     float64(i),
		}
		rb.Push(dp)
	}

	oldest := rb.OldestTimestamp()
	newest := rb.NewestTimestamp()

	if oldest.Sub(baseTime) > time.Second {
		t.Errorf("Oldest timestamp mismatch")
	}
	if newest.Sub(baseTime.Add(2*time.Minute)) > time.Second {
		t.Errorf("Newest timestamp mismatch")
	}
}

func TestRingBuffer_Clear(t *testing.T) {
	rb := NewRingBuffer(5)

	for i := 0; i < 3; i++ {
		dp := &v1.OTelMetricDataPoint{
			Timestamp: timestamppb.New(time.Now()),
			Value:     float64(i),
		}
		rb.Push(dp)
	}

	rb.Clear()

	if rb.Count() != 0 {
		t.Errorf("Expected count 0 after clear, got %d", rb.Count())
	}
	if !rb.IsEmpty() {
		t.Error("Buffer should be empty after clear")
	}
}

func TestRingBuffer_Empty(t *testing.T) {
	rb := NewRingBuffer(5)

	if !rb.IsEmpty() {
		t.Error("New buffer should be empty")
	}
	if rb.IsFull() {
		t.Error("New buffer should not be full")
	}

	results := rb.All()
	if len(results) != 0 {
		t.Errorf("Expected empty results, got %d", len(results))
	}

	results = rb.Range(time.Now(), time.Now().Add(time.Hour))
	if len(results) != 0 {
		t.Errorf("Expected empty results for empty buffer range query, got %d", len(results))
	}
}
