# Auto-NP Client

A Go client for connecting to the Auto-NP server and streaming network flow data with JWT authentication.

## Overview

This client connects to the Auto-NP server using gRPC bidirectional streaming. It authenticates using Oauth2 creds and sends network flow data to the server. The server processes the flow data and can return auto-generated network policies.
