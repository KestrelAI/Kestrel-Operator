FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy the source code
COPY . .

# Build the operator application
WORKDIR /app
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

# Install kubectl (multi-arch) - pinned to v1.31.0 for reliability
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    curl -LO "https://dl.k8s.io/release/v1.31.0/bin/linux/${ARCH}/kubectl" && \
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

WORKDIR /app

# Create a non-root user for running the operator
RUN addgroup -S app && adduser -S app -G app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/client .
RUN chown app:app /app/client

# Verify tools are installed
RUN kubectl version --client=true && \
    cilium version --client && \
    jq --version && \
    bash --version

USER app

# Run the client
CMD ["/app/client"]