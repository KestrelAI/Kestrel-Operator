# Kestrel Operator

An operator for streaming Kubernetes resource metadata, events, logs, and network traffic telemetry over mTLS to Kestrel Cloud.

## Overview

The Kestrel Operator connects to Kestrel Cloud using gRPC bidirectional streaming. It authenticates using OAuth2 credentials, establishes an mTLS HTTP/2 connection, and performs the following functions:

- **Event and Log Ingestion**: Collects Kubernetes events, pod logs and statuses, node conditions, etc. for 24/7, real-time incident detection
- **Resource Ingestion**: Monitors and streams Kubernetes workloads, services, namespaces, and network policies
- **Network Flow Collection**: Collects L3/L4 network flow data from Cilium Hubble Relay (optional)
- **L7 Access Log Collection**: Collects L7 access logs from Istio Envoy proxies via a gRPC Access Log Service (optional)
- **API & Command Execution**: Executes Kubernetes and Cilium API calls, and shell commands as requested by AI Agents on Kestrel Cloud.

## Cilium Integration

The Kestrel Operator can optionally integrate with Cilium for L3/L4 network flow collection. This integration is **not required** for basic functionality.

### Without Cilium

The operator will continue to function normally without Cilium installed, providing:
- Kubernetes resource monitoring and ingestion
- Server communication and command execution

### With Cilium

When Cilium is available, the Kestrel Operator additionally provides:
- Real-time network flow data collection

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

The Kestrel Operator integrates with Istio for L7 access log collection via Envoy's Access Log Service (ALS). This integration provides detailed L7 (HTTP/gRPC) traffic visibility.

### Without Istio

The operator will continue to function normally without Istio installed, providing:
- Kubernetes resource monitoring and ingestion
- Server communication and command execution

### With Istio

When Istio is available and properly configured, the Kestrel Operator additionally provides:
- Real-time L7 access log collection from Envoy proxies
- HTTP request/response analysis including methods, paths, status codes, and headers

### Prerequisites

For Istio integration to work, the following must be configured:

1. **Istio Service Mesh** must be installed and running
2. **Istio mesh configuration** must define the Kestrel Operator as an extension provider for access logging
3. **Application namespaces** must be labeled with `istio-injection=enabled` so workloads get Envoy sidecars

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

### Istio Mesh Configuration

The operator automatically creates the necessary Telemetry resources via its Helm chart, but you **must** configure Istio's mesh configuration to define the Kestrel Operator as an extension provider for access logging.

**Required extension providers:**

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

There are several ways to apply this depending on your Istio setup:

#### Option 1: Via Helm `--set` flags (new Istio installations or Helm-managed Istio)

```bash
helm repo add istio https://istio-release.storage.googleapis.com/charts
helm upgrade --install istiod istio/istiod -n istio-system \
  --set 'meshConfig.extensionProviders[0].name=kestrel-operator-als' \
  --set 'meshConfig.extensionProviders[0].envoyHttpAls.service=kestrel-operator-als.kestrel-ai.svc.cluster.local' \
  --set 'meshConfig.extensionProviders[0].envoyHttpAls.port=8080' \
  --set 'meshConfig.extensionProviders[1].name=kestrel-operator-als-tcp' \
  --set 'meshConfig.extensionProviders[1].envoyTcpAls.service=kestrel-operator-als.kestrel-ai.svc.cluster.local' \
  --set 'meshConfig.extensionProviders[1].envoyTcpAls.port=8080'
```

#### Option 2: Via `kubectl patch` (existing installations without custom mesh config)

If your Istio installation doesn't have custom mesh configuration, you can apply the extension providers with a single command:

```bash
kubectl patch configmap istio -n istio-system --type merge -p '{"data":{"mesh":"extensionProviders:\n- name: kestrel-operator-als\n  envoyHttpAls:\n    service: kestrel-operator-als.kestrel-ai.svc.cluster.local\n    port: 8080\n- name: kestrel-operator-als-tcp\n  envoyTcpAls:\n    service: kestrel-operator-als.kestrel-ai.svc.cluster.local\n    port: 8080"}}'
```

After patching, restart Istiod:
```bash
kubectl rollout restart deployment istiod -n istio-system
```

> **⚠️ Warning:** This `--type merge` patch replaces the entire `mesh` key. Only use this if you don't have existing custom mesh configuration. If you do, use Option 3 instead.

#### Option 3: Edit the ConfigMap in place (existing installations with custom mesh config)

If you already have custom mesh configuration (e.g., custom `discoveryAddress`, `trustDomain`, `defaultProviders`), edit the ConfigMap directly to avoid overwriting your existing settings:

```bash
kubectl edit configmap istio -n istio-system
```

Find the `mesh:` key in the `data` section and add the `extensionProviders` block while keeping all existing settings:

```yaml
data:
  mesh: |-
    # ... your existing mesh config (keep all existing settings) ...
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

After saving, restart Istiod to pick up the changes:
```bash
kubectl rollout restart deployment istiod -n istio-system
```

### Namespace Configuration

Application namespaces must have Istio sidecar injection enabled. Label each namespace **before** deploying workloads:

```bash
kubectl label namespace <your-namespace> istio-injection=enabled
```

If workloads are already running, label the namespace and restart the deployments to inject sidecars:

```bash
kubectl label namespace <your-namespace> istio-injection=enabled --overwrite
kubectl rollout restart deployment -n <your-namespace> <deployment-name>
```

Verify sidecars are injected by checking that pods have an `istio-proxy` container:
```bash
kubectl get pods -n <your-namespace> -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.name}{" "}{end}{"\n"}{end}' | grep istio-proxy
```

If sidecars are injected, each pod will show `istio-proxy` in the container list.

The operator's Helm chart will automatically create the necessary `Telemetry` resources that reference these extension providers.

## OpenTelemetry Metrics Integration

The Kestrel Operator can receive and store OTEL metrics locally for incident root cause analysis (RCA). When enabled, the operator exposes an OTLP gRPC receiver (default port 4317) that accepts metrics from customer OpenTelemetry Collectors.

### Configuration

**Via Helm values:**
```yaml
operator:
  otel:
    enabled: true
    receiverPort: 4317
  metricsStore:
    retention: "30m"      # Rolling retention window
    maxSeries: 100000     # Max unique metric series
    ringSize: 60          # Data points per series
```

### Customer OTEL Collector Requirements

**IMPORTANT**: For metrics to include Kubernetes context (namespace, workload, pod names), the customer's OpenTelemetry Collector **MUST** have the `k8sattributes` processor configured.

**Required k8sattributes processor configuration:**

```yaml
processors:
  k8sattributes:
    auth_type: "serviceAccount"
    passthrough: false
    extract:
      metadata:
        - k8s.namespace.name
        - k8s.pod.name
        - k8s.pod.uid
        - k8s.deployment.name
        - k8s.statefulset.name
        - k8s.daemonset.name
        - k8s.replicaset.name
        - k8s.container.name
        - k8s.node.name
    pod_association:
      - sources:
          - from: resource_attribute
            name: k8s.pod.ip
          - from: resource_attribute
            name: ip
          - from: connection
```

**Exporter configuration to send metrics to Kestrel Operator:**

```yaml
exporters:
  otlp/kestrel:
    endpoint: kestrel-operator.kestrel-ai.svc.cluster.local:4317
    tls:
      insecure: true  # In-cluster communication
    # compression: gzip  # Optional, enable for reduced bandwidth

service:
  pipelines:
    metrics:
      receivers: [otlp, prometheus]
      processors: [k8sattributes, batch]
      exporters: [otlp/kestrel]
```

**Note**: The operator supports gzip compression. The OpenTelemetry Collector enables gzip by default, but if using the Go SDK directly, enable it via `OTEL_EXPORTER_OTLP_COMPRESSION=gzip` or the `compression: gzip` exporter option.

### Why k8sattributes is Required

Without the `k8sattributes` processor:
- Metrics will only have basic labels (no Kubernetes context)
- The operator cannot map metrics to workloads
- Queries by namespace/workload/pod will return no results
- RCA agents cannot correlate metrics with Kubernetes resources

### Installation

To install the Kestrel Operator on your cluster, simply copy the command from the Onboard Cluster page on the Kestrel Platform:

```bash
helm install kestrel-operator oci://ghcr.io/kestrelai/charts/kestrel-operator --version 0.1.0 --namespace kestrel-ai --create-namespace -f kestrel-ai-operator-values-<cluster-name>.yaml
```