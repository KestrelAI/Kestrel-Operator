FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy the source code
COPY . .

WORKDIR /app/operator

# Build the application
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
    gzip

# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/

# Install Cilium CLI
RUN CILIUM_CLI_VERSION=$(curl -s https://raw.githubusercontent.com/cilium/cilium-cli/main/stable.txt) && \
    curl -L --fail --remote-name-all https://github.com/cilium/cilium-cli/releases/download/${CILIUM_CLI_VERSION}/cilium-linux-amd64.tar.gz{,.sha256sum} && \
    sha256sum -c cilium-linux-amd64.tar.gz.sha256sum && \
    tar xzvfC cilium-linux-amd64.tar.gz /usr/local/bin && \
    rm cilium-linux-amd64.tar.gz cilium-linux-amd64.tar.gz.sha256sum

# Install Trivy
RUN TRIVY_VERSION=$(curl -s "https://api.github.com/repos/aquasecurity/trivy/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' | sed 's/v//') && \
    wget https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz && \
    tar zxvf trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz && \
    mv trivy /usr/local/bin/ && \
    rm trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/client .

# Verify tools are installed
RUN kubectl version --client=true && \
    cilium version --client && \
    trivy --version && \
    bash --version

# Run the client
CMD ["/app/client"] 