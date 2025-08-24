# Kestrel Operator

An operator for streaming Kubernetes resource metadata and network traffic telemetry over mTLS to Kestrel Cloud.

## Overview

The Kestrel Operator connects to Kestrel Cloud using gRPC bidirectional streaming. It authenticates using OAuth2 credentials, establishes an mTLS HTTP/2 connection, and performs the following functions:

- **Resource Ingestion**: Monitors and streams Kubernetes workloads, services, namespaces, and network policies
- **Network Flow Collection**: Collects L3/L4 network flow data from Cilium Hubble Relay (optional)
- **L7 Access Log Collection**: Collects L7 access logs from Istio Envoy proxies via a gRPC Access Log Service (optional)
- **Network Policy Management**: Receives network policies from Kestrel Cloud to dry-run; applies network policies with explicit approval.
- **Authorization Policy Management**: Receives Istio authorization policies from Kestrel Cloud to dry-run; applies authorization policies with explicit approval.
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

## Istio Service Mesh Integration

The Kestrel Operator integrates with Istio for L7 access log collection via Envoy's Access Log Service (ALS). This integration provides detailed L7 (HTTP/gRPC) traffic visibility for authorization policy analysis and recommendation.

### Without Istio

The operator will continue to function normally without Istio installed, providing:
- Kubernetes resource monitoring and ingestion
- Network policy effectiveness analysis and management (L3/L4 only)
- Server communication and command execution

### With Istio

When Istio is available and properly configured, the Kestrel Operator additionally provides:
- Real-time L7 access log collection from Envoy proxies
- HTTP request/response analysis including methods, paths, status codes, and headers
- Authorization policy recommendations based on observed L7 traffic patterns
- Authorization policy effectiveness evaluation using L7 traffic patterns

### Prerequisites

For Istio integration to work, the following must be configured:

1. **Istio Service Mesh** must be installed and running
2. **Envoy Access Log Service** must be configured to send logs to the operator
3. **Workloads must be part of the service mesh** (have Envoy sidecars injected)

### Configuration

To enable Istio integration (already done by selecting Istio as the traffic data source at cluster onboarding):

**Via Helm values:**
```yaml
operator:
  istio:
    enabled: true
  cilium:
    disableFlows: true  # Typically disable Cilium when using Istio for flow collection
```

### Istio Configuration

The operator automatically creates the necessary Telemetry resources via its Helm chart, but you **must** configure Istio's mesh configuration to define the Kestrel Operator as an extension provider for access logging.

**Add the following to your Istio mesh configuration:**

```yaml
meshConfig:
  extensionProviders:
    - name: kestrel-operator-als
      envoyHttpAls:
        service: kestrel-operator-als.kestrel-ai.svc.cluster.local
        port: 8080
    - name: kestrel-operator-als-tcp
      envoyTcpAls:
        service: kestrel-operator-als.kestrel-ai.svc.cluster.local
        port: 8080
```

This can be applied:

**Via Helm values when installing/upgrading Istio:**
```bash
helm upgrade istio-base istio/base -n istio-system
helm upgrade istiod istio/istiod -n istio-system --values istio-values.yaml
```

**Via kubectl patch (for existing installations):**
```bash
kubectl patch configmap istio -n istio-system --type merge -p '{"data":{"mesh":"extensionProviders:\n- name: kestrel-operator-als\n  envoyHttpAls:\n    service: kestrel-operator-als.kestrel-ai.svc.cluster.local\n    port: 8080\n- name: kestrel-operator-als-tcp\n  envoyTcpAls:\n    service: kestrel-operator-als.kestrel-ai.svc.cluster.local\n    port: 8080"}}'
```

The operator's Helm chart will automatically create the necessary `Telemetry` resources that reference these extension providers.

### Installation

To install the Kestrel Operator on your cluster, simply copy the command from the Onboard Cluster page on the Kestrel Platform:

```bash
helm install kestrel-operator oci://ghcr.io/ramanv0/charts/kestrel-operator --version 0.1.0 --namespace kestrel-ai --create-namespace -f kestrel-ai-operator-values-<cluster-name>.yaml
```