#!/bin/bash

# deploy-mtls-certs.sh
# Script to deploy mTLS certificates to Kubernetes for Kestrel Operator and Server

set -e

# Configuration
CERT_DIR="${CERT_DIR:-./mtls-certs}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kestrel-ai}"
SERVER_NAMESPACE="${SERVER_NAMESPACE:-autonp-server}"
OPERATOR_RELEASE="${OPERATOR_RELEASE:-kestrel-operator}"
SERVER_RELEASE="${SERVER_RELEASE:-autonp-server}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if certificate directory exists
if [ ! -d "$CERT_DIR" ]; then
    print_error "Certificate directory $CERT_DIR not found!"
    print_warning "Please run generate-mtls-certs.sh first"
    exit 1
fi

# Check if required certificate files exist
REQUIRED_FILES=("ca-cert.pem" "server-cert.pem" "server-key.pem" "client-cert.pem" "client-key.pem")
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$CERT_DIR/$file" ]; then
        print_error "Required certificate file $CERT_DIR/$file not found!"
        exit 1
    fi
done

print_header "Deploying mTLS Certificates to Kubernetes"

# Create namespaces if they don't exist
print_header "Creating Namespaces"
kubectl create namespace "$OPERATOR_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "$SERVER_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

print_success "Namespaces created/verified"

# Deploy server certificates
print_header "Deploying Server Certificates"
kubectl create secret tls "${SERVER_RELEASE}-server-tls" \
    --cert="$CERT_DIR/server-cert.pem" \
    --key="$CERT_DIR/server-key.pem" \
    --namespace="$SERVER_NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

# Deploy CA certificate for server (to verify client certificates)
kubectl create secret generic "${SERVER_RELEASE}-client-ca" \
    --from-file=ca.crt="$CERT_DIR/ca-cert.pem" \
    --namespace="$SERVER_NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

print_success "Server certificates deployed"

# Deploy client certificates for operator
print_header "Deploying Client Certificates for Operator"
kubectl create secret generic "${OPERATOR_RELEASE}-client-tls" \
    --from-file=tls.crt="$CERT_DIR/client-cert.pem" \
    --from-file=tls.key="$CERT_DIR/client-key.pem" \
    --from-file=ca.crt="$CERT_DIR/ca-cert.pem" \
    --namespace="$OPERATOR_NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

print_success "Client certificates deployed"

print_header "Certificate Deployment Summary"
echo ""
print_success "Server certificates deployed to namespace: $SERVER_NAMESPACE"
echo "  - Secret: ${SERVER_RELEASE}-server-tls (server cert & key)"
echo "  - Secret: ${SERVER_RELEASE}-client-ca (CA to verify clients)"
echo ""
print_success "Client certificates deployed to namespace: $OPERATOR_NAMESPACE"
echo "  - Secret: ${OPERATOR_RELEASE}-client-tls (client cert, key & CA)"
echo ""

print_header "Next Steps"
echo "1. Update server Helm values to enable mTLS:"
echo "   tls:"
echo "     useMTLS: true"
echo "     certFile: /tls/tls.crt"
echo "     keyFile: /tls/tls.key"
echo "     clientCACertFile: /tls/client-ca.crt"
echo ""
echo "2. Update operator Helm values to enable mTLS:"
echo "   server:"
echo "     useMTLS: true"
echo ""
echo "3. Redeploy both services with updated values"
echo ""

print_warning "Security Reminders:"
echo "- Certificate secrets contain sensitive private keys"
echo "- Ensure proper RBAC is configured to restrict access"
echo "- Monitor certificate expiration dates"
echo "- Consider using cert-manager for automatic rotation"
