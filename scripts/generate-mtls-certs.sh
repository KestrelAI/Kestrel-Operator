#!/bin/bash

# generate-mtls-certs.sh
# Script to generate mTLS certificates for Kestrel Operator <-> Server communication

set -e

# Configuration
CA_KEY="ca-key.pem"
CA_CERT="ca-cert.pem"
SERVER_KEY="server-key.pem"
SERVER_CERT="server-cert.pem"
CLIENT_KEY="client-key.pem"
CLIENT_CERT="client-cert.pem"

# Certificate validity (2 years)
DAYS=730

# Server configuration
SERVER_NAME="${SERVER_NAME:-grpc.platform.usekestrel.ai}"
COUNTRY="${COUNTRY:-US}"
STATE="${STATE:-CA}"
CITY="${CITY:-San Francisco}"
ORG="${ORG:-Kestrel AI}"
OU="${OU:-Platform}"

echo "Generating mTLS certificates for Kestrel Platform..."
echo "Server Name: $SERVER_NAME"

# Create output directory
OUTPUT_DIR="${OUTPUT_DIR:-./mtls-certs}"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

# 1. Generate CA private key
echo "1. Generating CA private key..."
openssl genrsa -out "$CA_KEY" 4096

# 2. Generate CA certificate
echo "2. Generating CA certificate..."
openssl req -new -x509 -key "$CA_KEY" -sha256 -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=Kestrel-CA" -days $DAYS -out "$CA_CERT"

# 3. Generate server private key
echo "3. Generating server private key..."
openssl genrsa -out "$SERVER_KEY" 4096

# 4. Generate server certificate signing request
echo "4. Generating server CSR..."
openssl req -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=$SERVER_NAME" -sha256 -new -key "$SERVER_KEY" -out server.csr

# 5. Generate server certificate signed by CA
echo "5. Generating server certificate..."
cat > server.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = $SERVER_NAME
DNS.2 = localhost
DNS.3 = grpc.platform.usekestrel.ai
DNS.4 = *.platform.usekestrel.ai
IP.1 = 127.0.0.1
EOF

openssl x509 -req -in server.csr -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$SERVER_CERT" -days $DAYS -extensions v3_req -extfile server.conf

# 6. Generate client private key
echo "6. Generating client private key..."
openssl genrsa -out "$CLIENT_KEY" 4096

# 7. Generate client certificate signing request
echo "7. Generating client CSR..."
openssl req -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=kestrel-operator" -sha256 -new -key "$CLIENT_KEY" -out client.csr

# 8. Generate client certificate signed by CA
echo "8. Generating client certificate..."
cat > client.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client.csr -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$CLIENT_CERT" -days $DAYS -extensions v3_req -extfile client.conf

# Cleanup temporary files
rm -f server.csr client.csr server.conf client.conf

# Set appropriate permissions
chmod 600 "$CA_KEY" "$SERVER_KEY" "$CLIENT_KEY"
chmod 644 "$CA_CERT" "$SERVER_CERT" "$CLIENT_CERT"

echo ""
echo "✅ mTLS certificates generated successfully!"
echo ""
echo "Generated files in $OUTPUT_DIR:"
echo "  📁 CA Certificate Authority:"
echo "    - $CA_KEY (private key - keep secure!)"
echo "    - $CA_CERT (certificate - distribute to clients)"
echo ""
echo "  🖥️  Server certificates:"
echo "    - $SERVER_KEY (private key - deploy to server)"
echo "    - $SERVER_CERT (certificate - deploy to server)"
echo ""
echo "  📱 Client certificates:"
echo "    - $CLIENT_KEY (private key - deploy to operator)"
echo "    - $CLIENT_CERT (certificate - deploy to operator)"
echo ""
echo "🔧 Next steps:"
echo "1. Deploy server certificates to Kestrel Server"
echo "2. Deploy client certificates to Kestrel Operator"
echo "3. Enable mTLS in Helm values:"
echo "   - Server: tls.useMTLS=true"
echo "   - Operator: server.useMTLS=true"
echo ""
echo "⚠️  Security Notes:"
echo "- Keep private keys secure and never commit them to version control"
echo "- Rotate certificates before expiration (valid for $DAYS days)"
echo "- Use proper RBAC to limit access to certificate secrets"
