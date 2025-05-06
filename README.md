# Auto-NP Client

A Go client for connecting to the Auto-NP server and streaming network flow data with JWT authentication.

## Overview

This client connects to the Auto-NP server using gRPC bidirectional streaming. It authenticates using JWT tokens and sends network flow data to the server. The server processes the flow data and can return auto-generated network policies.

## Configuration

The client can be configured using environment variables:

- `SERVER_HOST`: The hostname of the Auto-NP server (default: `localhost`)
- `SERVER_PORT`: The port of the Auto-NP server (default: `50051`)
- `SERVER_TLS`: Whether to use TLS for the connection (`true` or `false`, default: `false`)
- `JWT_TOKEN`: The JWT token to use for authentication (required)

## Building and Running

### Local Development

```bash
# Build the client
go build -o client ./cmd/client

# Run the client
export JWT_TOKEN="your-jwt-token"
export SERVER_HOST="auto-np-server.example.com"
export SERVER_PORT="50051"
./client
```

### Docker

```bash
# Build the Docker image
docker build -t auto-np-client .

# Run the client in Docker
docker run -e JWT_TOKEN="your-jwt-token" \
           -e SERVER_HOST="auto-np-server.example.com" \
           -e SERVER_PORT="50051" \
           auto-np-client
```

### Kubernetes with Helm

```bash
# Deploy to Kubernetes using Helm
helm install auto-np ./helm/auto-np \
     --set auth.token="your-jwt-token" \
     --set server.host="auto-np-server.example.com"
```

Alternatively, you can create a values file:

```yaml
# values.yaml
server:
  host: auto-np-server.example.com
  port: 50051
  useTLS: true

auth:
  token: "your-jwt-token"
```

And deploy using:

```bash
helm install auto-np ./helm/auto-np -f values.yaml
```

## Using a Secret for JWT Token

For better security in production, you should store your JWT token in a Kubernetes Secret:

```bash
# Create a secret
kubectl create secret generic auto-np-jwt --from-literal=jwt-token="your-jwt-token"

# Deploy using the existing secret
helm install auto-np ./helm/auto-np \
     --set auth.existingSecret="auto-np-jwt"
``` 