package operatorlog

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultBatchSize     = 100
	DefaultFlushInterval = 5 * time.Second
	DefaultEntryChanSize = 5000
	DefaultBatchChanSize = 100
)

// OperatorLogBatcher reads individual LogEntry messages from a channel
// and batches them into PodLogs messages for efficient streaming.
type OperatorLogBatcher struct {
	entryCh       <-chan *v1.LogEntry
	batchCh       chan<- *v1.PodLogs
	batchSize     int
	flushInterval time.Duration
	podName       string
	podNamespace  string
	containerName string
	nodeName      string
	streamOffset  atomic.Int64
}

// NewOperatorLogBatcher creates a new batcher that reads from entryCh and
// writes batched PodLogs to batchCh. Pod identity is read from environment variables.
func NewOperatorLogBatcher(entryCh <-chan *v1.LogEntry, batchCh chan<- *v1.PodLogs) *OperatorLogBatcher {
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		podName = "kestrel-operator"
	}

	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		podNamespace = "kestrel-ai"
	}

	return &OperatorLogBatcher{
		entryCh:       entryCh,
		batchCh:       batchCh,
		batchSize:     DefaultBatchSize,
		flushInterval: DefaultFlushInterval,
		podName:       podName,
		podNamespace:  podNamespace,
		containerName: "kestrel-operator",
		nodeName:      os.Getenv("NODE_NAME"),
	}
}

// Run is the main batching loop. It blocks until ctx is cancelled.
// Entries are flushed when the batch reaches batchSize or every flushInterval.
func (b *OperatorLogBatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*v1.LogEntry, 0, b.batchSize)

	for {
		select {
		case <-ctx.Done():
			// Flush remaining entries before exit
			if len(batch) > 0 {
				b.flush(batch)
			}
			return

		case entry, ok := <-b.entryCh:
			if !ok {
				// Channel closed — flush and exit
				if len(batch) > 0 {
					b.flush(batch)
				}
				return
			}
			batch = append(batch, entry)
			if len(batch) >= b.batchSize {
				b.flush(batch)
				batch = make([]*v1.LogEntry, 0, b.batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = make([]*v1.LogEntry, 0, b.batchSize)
			}
		}
	}
}

// flush sends a batch of log entries as a PodLogs message.
// Non-blocking — if batchCh is full, the batch is silently dropped.
func (b *OperatorLogBatcher) flush(entries []*v1.LogEntry) {
	podLogs := &v1.PodLogs{
		PodName:             b.podName,
		Namespace:           b.podNamespace,
		PodUid:              "", // not available for operator itself
		ContainerName:       b.containerName,
		LogEntries:          entries,
		PreviousContainer:   false,
		CollectionTimestamp: timestamppb.Now(),
		TotalLines:          int64(len(entries)),
		StreamComplete:      false, // continuous stream
		OwnerReferences:     nil, // not available for operator itself
		NodeName:            b.nodeName,
	}

	// Non-blocking send — only advance offset on successful send
	select {
	case b.batchCh <- podLogs:
		podLogs.StreamOffset = b.streamOffset.Add(int64(len(entries)))
	default:
	}
}
