# Kestrel Operator

An operator for streaming Kubernetes resource metadata and network traffic telemetry over mTLS to Kestrel Cloud.

## Overview

The Kestrel Operator connects to Kestrel Cloud using gRPC bidirectional streaming. It authenticates using OAuth2 credentials, establishes an mTLS HTTP/2 connection, and performs the following functions:

- **Resource Ingestion**: Monitors and streams Kubernetes workloads, services, namespaces, and network policies
- **Network Flow Collection**: Collects L3/L4 network flow data from Cilium Hubble Relay (optional)
- **Network Policy Management**: Receives network policies from Kestrel Cloud to dry-run; applies network policies with explicit approval.
- **API & Command Execution**: Executes Kubernetes and Cilium API calls, and shell commands as requested by AI Agents on Kestrel Cloud.

## Cilium Integration

The Kestrel Operator can optionally integrate with Cilium for L3/L4 network flow collection. This integration is **not required** for basic functionality.

### Without Cilium

The operator will continue to function normally without Cilium installed, providing:
- Kubernetes resource monitoring and ingestion
- Network policy effectiveness analysis and management
- Server communication and command execution

### With Cilium

When Cilium is available, the Kestrel Operator additionally provides:
- Real-time network flow data collection
- Enhanced network policy recommendations based on observed traffic

### Configuration

To disable Cilium integration entirely:

**Via Helm values:**
```yaml
operator:
  cilium:
    disableFlows: true
```

**Via environment variable:**
```bash
DISABLE_CILIUM_FLOWS=true
```

### Installation

To install the Kestrel Operator on your cluster, simply copy the command from the Onboard Cluster page on the Kestrel Platform:

```bash
helm install kestrel-operator oci://ghcr.io/ramanv0/charts/kestrel-operator --version 0.1.0 --namespace kestrel-ai --create-namespace -f kestrel-ai-operator-values-<cluster-name>.yaml
```