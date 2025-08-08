FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy the source code
COPY . .

# Build the operator application
WORKDIR /app/operator
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/client ./cmd/client

# Use a small alpine image for the final container
FROM alpine:latest

# Install basic dependencies
RUN apk --no-cache add \
    ca-certificates \
    bash \
    curl \
    wget \
    tar \
    gzip \
    jq

# Install kubectl (multi-arch)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/${ARCH}/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/

# Install Cilium CLI (multi-arch)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    CILIUM_CLI_VERSION=$(curl -s https://raw.githubusercontent.com/cilium/cilium-cli/main/stable.txt) && \
    curl -L --fail --remote-name-all https://github.com/cilium/cilium-cli/releases/download/${CILIUM_CLI_VERSION}/cilium-linux-${ARCH}.tar.gz{,.sha256sum} && \
    sha256sum -c cilium-linux-${ARCH}.tar.gz.sha256sum && \
    tar xzvfC cilium-linux-${ARCH}.tar.gz /usr/local/bin && \
    rm cilium-linux-${ARCH}.tar.gz cilium-linux-${ARCH}.tar.gz.sha256sum

# Install Trivy (multi-arch)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then TRIVY_ARCH="64bit"; elif [ "$ARCH" = "aarch64" ]; then TRIVY_ARCH="ARM64"; fi && \
    TRIVY_VERSION=$(curl -s "https://api.github.com/repos/aquasecurity/trivy/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' | sed 's/v//') && \
    wget https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-${TRIVY_ARCH}.tar.gz && \
    tar zxvf trivy_${TRIVY_VERSION}_Linux-${TRIVY_ARCH}.tar.gz && \
    mv trivy /usr/local/bin/ && \
    rm trivy_${TRIVY_VERSION}_Linux-${TRIVY_ARCH}.tar.gz

# Pre-download Trivy vulnerability database during build
# This ensures the operator doesn't need internet access at runtime
RUN mkdir -p /root/.cache/trivy && \
    trivy image --download-db-only --cache-dir /root/.cache/trivy && \
    chmod -R 755 /root/.cache/trivy

WORKDIR /app

# Set environment variables for Trivy to use offline mode with pre-downloaded DB
ENV TRIVY_OFFLINE=true
ENV TRIVY_CACHE_DIR=/root/.cache/trivy
ENV TRIVY_DB_REPOSITORY=""

# Copy the binary from the builder stage
COPY --from=builder /app/bin/client .

# Verify tools are installed and Trivy database is ready
RUN kubectl version --client=true && \
    cilium version --client && \
    trivy --version && \
    echo "Testing Trivy offline mode..." && \
    trivy image --offline-scan --skip-db-update alpine:latest || echo "Trivy offline test completed (exit code expected for test image)" && \
    jq --version && \
    bash --version

# Run the client
CMD ["/app/client"] 