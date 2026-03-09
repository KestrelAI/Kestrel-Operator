package client

import (
	"sync"

	v1 "operator/api/gen/cloud/v1"
)

// eventsStreamSendMu protects concurrent Send calls on the events stream.
var eventsStreamSendMu sync.Mutex

// protectedEventsSend sends a message on the events stream with mutex protection.
func (s *StreamClient) protectedEventsSend(stream v1.StreamService_StreamEventsClient, msg *v1.StreamEventsRequest) error {
	eventsStreamSendMu.Lock()
	defer eventsStreamSendMu.Unlock()
	return stream.Send(msg)
}

// sendKubernetesEventOnEventsStream sends a K8s event on the dedicated events stream.
func (s *StreamClient) sendKubernetesEventOnEventsStream(eventsStream v1.StreamService_StreamEventsClient, event *v1.KubernetesEvent) error {
	return s.protectedEventsSend(eventsStream, &v1.StreamEventsRequest{
		Request: &v1.StreamEventsRequest_KubernetesEvent{KubernetesEvent: event},
	})
}

// sendPodStatusChangeOnEventsStream sends a pod status change on the dedicated events stream.
func (s *StreamClient) sendPodStatusChangeOnEventsStream(eventsStream v1.StreamService_StreamEventsClient, podStatus *v1.PodStatusChange) error {
	return s.protectedEventsSend(eventsStream, &v1.StreamEventsRequest{
		Request: &v1.StreamEventsRequest_PodStatusChange{PodStatusChange: podStatus},
	})
}

// sendNodeConditionChangeOnEventsStream sends a node condition change on the dedicated events stream.
func (s *StreamClient) sendNodeConditionChangeOnEventsStream(eventsStream v1.StreamService_StreamEventsClient, nodeCondition *v1.NodeConditionChange) error {
	return s.protectedEventsSend(eventsStream, &v1.StreamEventsRequest{
		Request: &v1.StreamEventsRequest_NodeConditionChange{NodeConditionChange: nodeCondition},
	})
}

// sendWorkloadRolloutStatusOnEventsStream sends a workload rollout status on the dedicated events stream.
func (s *StreamClient) sendWorkloadRolloutStatusOnEventsStream(eventsStream v1.StreamService_StreamEventsClient, rolloutStatus *v1.WorkloadRolloutStatus) error {
	return s.protectedEventsSend(eventsStream, &v1.StreamEventsRequest{
		Request: &v1.StreamEventsRequest_WorkloadRolloutStatus{WorkloadRolloutStatus: rolloutStatus},
	})
}
