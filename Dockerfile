FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy the source code
COPY . .

WORKDIR /app/operator

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/client ./cmd/client

# Use a small alpine image for the final container
FROM alpine:latest

# Install CA certificates for HTTPS/TLS
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/client .

# Run the client
CMD ["/app/client"] 