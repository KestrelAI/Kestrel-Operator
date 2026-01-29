package metrics_store

import (
	"time"

	v1 "operator/api/gen/cloud/v1"
)

// RingBuffer is a fixed-size circular buffer for time-series data points.
type RingBuffer struct {
	data  []*v1.OTelMetricDataPoint
	head  int // Index of oldest element
	tail  int // Index where next element will be written
	count int // Current number of elements
	size  int // Maximum capacity
}

// NewRingBuffer creates a new ring buffer with the given capacity.
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		data: make([]*v1.OTelMetricDataPoint, size),
		size: size,
	}
}

// Push adds a data point to the buffer, overwriting the oldest if full.
func (r *RingBuffer) Push(dp *v1.OTelMetricDataPoint) {
	r.data[r.tail] = dp
	r.tail = (r.tail + 1) % r.size

	if r.count < r.size {
		r.count++
	} else {
		// Buffer is full, advance head (overwrite oldest)
		r.head = (r.head + 1) % r.size
	}
}

// Range returns all data points with timestamps in [start, end].
func (r *RingBuffer) Range(start, end time.Time) []*v1.OTelMetricDataPoint {
	if r.count == 0 {
		return nil
	}

	result := make([]*v1.OTelMetricDataPoint, 0, r.count)

	for i := 0; i < r.count; i++ {
		idx := (r.head + i) % r.size
		dp := r.data[idx]
		if dp == nil || dp.Timestamp == nil {
			continue
		}

		t := dp.Timestamp.AsTime()
		if !t.Before(start) && !t.After(end) {
			result = append(result, dp)
		}
	}

	return result
}

// All returns all data points in the buffer, from oldest to newest.
func (r *RingBuffer) All() []*v1.OTelMetricDataPoint {
	if r.count == 0 {
		return nil
	}

	result := make([]*v1.OTelMetricDataPoint, 0, r.count)

	for i := 0; i < r.count; i++ {
		idx := (r.head + i) % r.size
		dp := r.data[idx]
		if dp != nil {
			result = append(result, dp)
		}
	}

	return result
}

// NewestTimestamp returns the timestamp of the most recent data point.
func (r *RingBuffer) NewestTimestamp() time.Time {
	if r.count == 0 {
		return time.Time{}
	}

	// Tail points to next write position, so newest is at tail-1
	idx := (r.tail - 1 + r.size) % r.size
	if r.data[idx] == nil || r.data[idx].Timestamp == nil {
		return time.Time{}
	}
	return r.data[idx].Timestamp.AsTime()
}

// OldestTimestamp returns the timestamp of the oldest data point.
func (r *RingBuffer) OldestTimestamp() time.Time {
	if r.count == 0 {
		return time.Time{}
	}
	if r.data[r.head] == nil || r.data[r.head].Timestamp == nil {
		return time.Time{}
	}
	return r.data[r.head].Timestamp.AsTime()
}

// Count returns the current number of data points in the buffer.
func (r *RingBuffer) Count() int {
	return r.count
}

// Size returns the maximum capacity of the buffer.
func (r *RingBuffer) Size() int {
	return r.size
}

// Clear removes all data points from the buffer.
func (r *RingBuffer) Clear() {
	for i := range r.data {
		r.data[i] = nil
	}
	r.head = 0
	r.tail = 0
	r.count = 0
}

// IsFull returns true if the buffer is at capacity.
func (r *RingBuffer) IsFull() bool {
	return r.count == r.size
}

// IsEmpty returns true if the buffer has no data points.
func (r *RingBuffer) IsEmpty() bool {
	return r.count == 0
}
