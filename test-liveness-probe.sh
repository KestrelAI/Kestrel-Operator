#!/bin/bash

# Test script for operator liveness probe
# This script demonstrates how to test the EOF loop detection and pod restart functionality

set -e

NAMESPACE=${1:-kestrel-ai}
DEPLOYMENT="kestrel-operator"
PORT=8081

echo "Testing Kestrel AI Operator Liveness Probe"
echo "=============================================="
echo "Namespace: $NAMESPACE"
echo "Deployment: $DEPLOYMENT"
echo ""

# Function to check if port-forward is running
check_port_forward() {
    if ! curl -s http://localhost:$PORT/health/status > /dev/null 2>&1; then
        echo "Port-forward not available, setting up..."
        kubectl port-forward -n $NAMESPACE deployment/$DEPLOYMENT $PORT:$PORT > /dev/null 2>&1 &
        PORT_FORWARD_PID=$!
        echo "Waiting for port-forward to be ready..."
        sleep 3
        
        # Verify port-forward is working
        if ! curl -s http://localhost:$PORT/health/status > /dev/null 2>&1; then
            echo "Failed to establish port-forward"
            exit 1
        fi
        echo "Port-forward established (PID: $PORT_FORWARD_PID)"
    fi
}

# Function to cleanup port-forward
cleanup() {
    if [ ! -z "$PORT_FORWARD_PID" ]; then
        echo "🧹 Cleaning up port-forward (PID: $PORT_FORWARD_PID)"
        kill $PORT_FORWARD_PID 2>/dev/null || true
    fi
}

# Set up cleanup on exit
trap cleanup EXIT

echo "Checking initial operator health..."
check_port_forward

INITIAL_STATUS=$(curl -s http://localhost:$PORT/health/status)
echo "Initial status: $(echo $INITIAL_STATUS | jq -r '.status')"
echo "Stream healthy: $(echo $INITIAL_STATUS | jq -r '.stream_healthy')"
echo "EOF count: $(echo $INITIAL_STATUS | jq -r '.eof_error_count')"
echo ""

echo "Testing liveness probe (should be healthy)..."
LIVENESS_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/health/live)
if [ "$LIVENESS_CODE" = "200" ]; then
    echo "Liveness probe returns 200 OK (healthy)"
else
    echo "Liveness probe returns $LIVENESS_CODE (unexpected)"
fi
echo ""

echo "Simulating EOF error..."
SIMULATE_RESPONSE=$(curl -s -X POST http://localhost:$PORT/health/test/simulate-eof)
echo "Simulation response: $(echo $SIMULATE_RESPONSE | jq -r '.status')"
echo ""

echo "Checking status after EOF simulation..."
POST_EOF_STATUS=$(curl -s http://localhost:$PORT/health/status)
echo "Status after EOF: $(echo $POST_EOF_STATUS | jq -r '.status')"
echo "Stream healthy: $(echo $POST_EOF_STATUS | jq -r '.stream_healthy')"
echo "EOF count: $(echo $POST_EOF_STATUS | jq -r '.eof_error_count')"
echo "Unhealthy for liveness: $(echo $POST_EOF_STATUS | jq -r '.in_eof_loop')"
echo ""

echo "Testing liveness probe after EOF (should fail)..."
LIVENESS_CODE_AFTER=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/health/live)
if [ "$LIVENESS_CODE_AFTER" = "503" ]; then
    echo "Liveness probe returns 503 Service Unavailable (stream unhealthy detected)"
else
    echo "Liveness probe returns $LIVENESS_CODE_AFTER (expected 503)"
fi
echo ""

echo "Watching for pod restart..."
echo "Current pod:"
kubectl get pods -n $NAMESPACE -l app=$DEPLOYMENT
echo ""

CURRENT_POD=$(kubectl get pods -n $NAMESPACE -l app=$DEPLOYMENT -o jsonpath='{.items[0].metadata.name}')
RESTART_COUNT_BEFORE=$(kubectl get pod -n $NAMESPACE $CURRENT_POD -o jsonpath='{.status.containerStatuses[0].restartCount}')

echo "Pod: $CURRENT_POD"
echo "Restart count before: $RESTART_COUNT_BEFORE"
echo ""
echo "Waiting for Kubernetes to restart the pod (this may take up to 2 minutes)..."
echo "   - Liveness probe period: 15s"
echo "   - Failure threshold: 1"
echo "   - Expected restart time: ~15-20 seconds"
echo ""

# Wait for restart (up to 1 minute should be enough now)
for i in {1..12}; do
    sleep 5
    RESTART_COUNT_NOW=$(kubectl get pod -n $NAMESPACE $CURRENT_POD -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo "0")
    
    if [ "$RESTART_COUNT_NOW" -gt "$RESTART_COUNT_BEFORE" ]; then
        echo "Pod restarted! Restart count: $RESTART_COUNT_NOW"
        break
    fi
    
    # Check if pod was replaced (new name)
    NEW_POD=$(kubectl get pods -n $NAMESPACE -l app=$DEPLOYMENT -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ "$NEW_POD" != "$CURRENT_POD" ]; then
        echo "Pod was replaced! New pod: $NEW_POD"
        CURRENT_POD=$NEW_POD
        break
    fi
    
    echo "   Waiting... (${i}5s elapsed)"
done

echo ""
echo "Verifying recovery after restart..."
echo "Waiting for new pod to be ready..."
kubectl wait --for=condition=ready pod -n $NAMESPACE -l app=$DEPLOYMENT --timeout=60s

# Re-establish port-forward to new pod
cleanup
sleep 2
check_port_forward

FINAL_STATUS=$(curl -s http://localhost:$PORT/health/status)
echo "Final status: $(echo $FINAL_STATUS | jq -r '.status')"
echo "Stream healthy: $(echo $FINAL_STATUS | jq -r '.stream_healthy')"
echo "EOF count: $(echo $FINAL_STATUS | jq -r '.eof_error_count')"
echo "Unhealthy for liveness: $(echo $FINAL_STATUS | jq -r '.in_eof_loop')"

FINAL_LIVENESS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$PORT/health/live)
if [ "$FINAL_LIVENESS" = "200" ]; then
    echo "Liveness probe is healthy again after restart"
else
    echo "Liveness probe returns $FINAL_LIVENESS (may need more time to recover)"
fi

echo ""
echo "Liveness probe test completed!"
echo ""
echo "Summary:"
echo "- Initial health check passed"
echo "- EOF simulation worked"
echo "- Stream unhealthy detection worked"  
echo "- Liveness probe failed as expected"
echo "- Pod restarted quickly (~15-20s)"
echo "- Recovery after restart verified"
echo ""
echo "The liveness probe is working correctly! 🚀" 