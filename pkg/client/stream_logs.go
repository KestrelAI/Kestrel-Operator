package client

import (
	"sync"

	v1 "operator/api/gen/cloud/v1"
)

// logsStreamSendMu protects concurrent Send calls on the logs stream.
var logsStreamSendMu sync.Mutex

// protectedLogsSend sends a message on the logs stream with mutex protection.
func (s *StreamClient) protectedLogsSend(stream v1.StreamService_StreamLogsClient, msg *v1.StreamLogsRequest) error {
	logsStreamSendMu.Lock()
	defer logsStreamSendMu.Unlock()
	return stream.Send(msg)
}

// sendPodLogsOnLogsStream sends pod logs on the dedicated logs stream.
func (s *StreamClient) sendPodLogsOnLogsStream(logsStream v1.StreamService_StreamLogsClient, podLogs *v1.PodLogs) error {
	return s.protectedLogsSend(logsStream, &v1.StreamLogsRequest{
		Request: &v1.StreamLogsRequest_PodLogs{PodLogs: podLogs},
	})
}
