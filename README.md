# Auto-NP Operator

A Go operator for connecting to the Auto-NP server and streaming Kubernetes resource data with JWT authentication.

## Overview

This operator connects to the Auto-NP server using gRPC bidirectional streaming. It authenticates using OAuth2 credentials and performs the following functions:

- **Resource Ingestion**: Monitors and streams Kubernetes workloads, namespaces, and network policies
- **Network Flow Collection**: Collects network flow data from Cilium Hubble Relay (optional)
- **Policy Management**: Receives and applies network policies from the server
- **API & Shell Execution**: Executes Kubernetes API calls and shell commands as requested by the server

## Cilium Integration

The operator can optionally integrate with Cilium for network flow collection. This integration is **not required** for basic functionality.

### Without Cilium

The operator will continue to function normally without Cilium installed, providing:
- Kubernetes resource monitoring and ingestion
- Network policy management
- Server communication and command execution

### With Cilium

When Cilium is available, the operator additionally provides:
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
