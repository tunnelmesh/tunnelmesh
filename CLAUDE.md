# TunnelMesh - Claude Code Instructions

## Project Overview

TunnelMesh is a P2P mesh networking tool written in Go that creates encrypted tunnels between nodes. It uses the Noise protocol (IKpsk2) with ChaCha20-Poly1305 encryption for secure communication.

**This is a greenfield project** - breaking changes are allowed and encouraged when they improve the codebase. Backwards compatibility is not a constraint during active development.

**Admin Access:**
- Configured via `admin_peers` field in coordinator config
- Supports peer names OR peer IDs (SHA256 of SSH key, first 8 bytes = 16 hex chars)
- Peer IDs are preferred for security (immutable, tied to SSH key)
- Matched peers are added to "admins" group on first join
- Network/routing functionality works without admin access
- Admin access enables peer stats, mesh/data plane configuration

**Key components:**
- **Coordinator**: Central server for peer discovery, IP allocation, DNS, and relay fallback
- **Peer**: Mesh peer with TUN interface for transparent IP routing
- **Transports**: UDP (primary, low-latency), SSH (fallback), WebSocket relay (last resort)

## Common Commands

```bash
# Build
make build              # Build binary
make release-all        # Cross-platform release builds

# Test
make test               # Run tests with -short (skips slow simulator stress tests)
make test-full          # Run ALL tests including slow simulator (used in CI merge jobs)
make test-verbose       # Verbose test output (with -short)
make test-race          # Run with race detector (with -short, ONLY for debugging concurrency)

# Lint
golangci-lint run       # Run linter
make fmt                # Format code

# Frontend Linting (requires Node.js 20+)
npm install             # Install dependencies (stylelint, biome)
npx stylelint "internal/coord/web/css/**/*.css"  # Lint CSS
npx biome check internal/coord/web/js/           # Lint JavaScript

# Docker
make docker-up          # Start full stack (server + clients + monitoring)
make docker-down        # Stop containers
make docker-logs        # Follow logs

# Deploy (Terraform/DigitalOcean)
make deploy-plan        # Preview changes
make deploy             # Apply infrastructure
make deploy-update      # Update binaries on nodes
```

## Directory Structure

```
cmd/                    # CLI entrypoints
  tunnelmesh/           # Main binary
internal/               # Core packages
  transport/            # SSH, UDP, relay transports
    udp/                # Noise protocol, encryption, handshake
  routing/              # Packet router, filter
  peer/                 # Peer peer logic, connection FSM
  coord/                # Coordinator server, API, relay
  tun/                  # TUN device (platform-specific)
  dns/                  # Mesh DNS resolver
  portmap/              # NAT-PMP, PCP, UPnP
  metrics/              # Prometheus metrics
pkg/proto/              # Protocol message definitions
terraform/              # DigitalOcean infrastructure
docker/                 # Container deployment
monitoring/             # Prometheus, Grafana, Loki configs
```

## Coding Conventions

### Go Patterns
- **Error wrapping**: Use `fmt.Errorf("context: %w", err)` for wrapped errors
- **Context propagation**: Pass `context.Context` through call chains, never store in structs
- **Resource cleanup**: Always use `defer` for cleanup; ensure goroutines have termination conditions
- **Interfaces**: Keep small, define at point of use (consumer-defined)

### Concurrency
- **Lock-free hot paths**: Use `atomic.Pointer` with copy-on-write for high-frequency reads (see `routing/router.go`)
- **Synchronization**: Prefer `sync.RWMutex` for read-heavy access, channels for coordination
- **Goroutine lifecycle**: Track with `sync.WaitGroup`, cancel via `context.Context`

### Testing
- **Coverage threshold**: 40% minimum (enforced in CI)
- **Race detection**: Always run `go test -race` before committing
- **Table-driven tests**: Preferred for exhaustive case coverage
- **Mocks**: Use interface-based mocks, not concrete type mocking

### Crypto (in transport/udp/)
- **Noise IKpsk2**: Initiator sends e, es, s, ss; responder sends e, ee, se
- **Nonce management**: 12-byte nonce from 64-bit counter, must never repeat
- **Constant-time ops**: Use `hmac.Equal` for MAC comparison
- **Key zeroing**: Zero ephemeral private keys after handshake

## Workflow

After implementing a feature or fix:

1. **Run tests**: `make test` (ensure all pass)
2. **Run linter**: `golangci-lint run` (fix any issues)
3. **Commit**: Create a commit with descriptive message
4. **Create branch**: Push to a feature branch
5. **Create PR**: Open pull request against `main`

## UI Panel System

The web dashboard uses an extensible panel system with RBAC-based visibility control.

### Panel Architecture

**Backend (`internal/auth/panel.go`)**:
- `PanelRegistry`: Dynamic panel registration with 13 built-in panels
- `PanelDefinition`: Panel metadata (id, name, tab, category, public, external)
- Panels can be marked as **public** (visible without authentication)
- External plugins can register panels via the API

**Frontend (`internal/coord/web/js/lib/panel.js`)**:
- UMD module for browser + Bun test compatibility
- Permission loading via `/api/user/permissions`
- Fail-secure: API errors result in no panel access
- External panel support via iframe + postMessage protocol

### Panel Permissions

Access control via existing RBAC system:
1. **Public flag**: If `panel.Public == true`, anyone can view
2. **Admin role**: Full access to all panels
3. **RoleBinding**: `role=panel-viewer, panel_scope=<panelID>`
4. **GroupBinding**: Same pattern, applied to groups

**Default groups**:
- `everyone`: visualizer, map, s3, shares
- `all_admin_users`: peers, logs, filter, dns, users, groups, bindings, docker

### Built-in Panel IDs

| Tab  | Panels                                                |
|------|-------------------------------------------------------|
| mesh | visualizer, map, peers, logs, filter, dns |
| data | s3, shares, users, groups, bindings, docker           |

### CSS Design Tokens

All styling uses CSS variables in `internal/coord/web/css/style.css`:

```css
--panel-border-radius: 0px;   /* Sharp corners throughout */
--btn-border-radius: 0px;
--modal-border-radius: 0px;
--input-border-radius: 0px;

/* Visualizer colors (for canvas rendering) */
--viz-node-online, --viz-node-offline, --viz-node-coordinator
--viz-edge-online, --viz-edge-offline, --viz-label-text
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/user/permissions` | Current user's accessible panels |
| GET | `/api/panels` | List all panels (filter: `?external=true`) |
| POST | `/api/panels` | Register external panel (admin only) |
| PATCH | `/api/panels/{id}` | Update panel metadata (admin only) |
| DELETE | `/api/panels/{id}` | Unregister external panel (admin only) |

### External Panel Development

External panels are loaded via iframe with postMessage:

```javascript
// Plugin -> Dashboard
window.parent.postMessage({
    type: 'tunnelmesh:panel:ready',
    panelId: 'my-plugin'
}, '*');

// Dashboard -> Plugin (on load)
{
    type: 'tunnelmesh:init',
    panelId: 'my-plugin',
    theme: 'dark',
    user: { id: '...', isAdmin: false }
}
```

## Docker Orchestration

TunnelMesh coordinators automatically detect and integrate with Docker when the Docker socket is available. No explicit configuration required.

### Configuration

Docker integration is **automatically enabled** when:
- The coordinator joins the mesh (`join_mesh` configured)
- Docker socket exists at `/var/run/docker.sock` (default)

**Optional configuration:**

```yaml
# coordinator.yaml - only if you need non-default settings
docker:
  socket: "unix:///var/run/docker.sock"  # Custom socket path
  auto_port_forward: false                # Disable automatic port forwarding
```

Join coordinator with token (server URL and auth token are CLI-only):
```bash
tunnelmesh join --token your-token --config coordinator.yaml
```

**Security Note**: Docker socket access grants root-equivalent privileges. Use with caution. Consider Docker rootless mode for enhanced security.

### Automatic Port Forwarding

When a container starts with published ports on a bridge network:
1. TunnelMesh creates temporary filter rules allowing access to those ports
2. Rules expire after 24 hours (container lifetime-based)
3. Rules are shown as "Temporary" in the filter panel
4. Host network containers are skipped (already have direct access)
5. **Event-driven**: Automatically detects containers started via any method (docker run, docker-compose, restart policies)

**Example:**
```bash
# Container with published port
docker run -d -p 8080:80 nginx

# TunnelMesh automatically creates:
# Port: 8080, Protocol: TCP, Action: Allow, Expires: <24h from now>
# This happens automatically via Docker events API - no manual refresh needed
```

### Docker Panel

The Docker panel (data tab) displays:
- **Container list**: Name, image, status, uptime, ports, network mode
- **Status badges**: Running (green), exited (red), other states
- **Control actions**: Start, stop, restart buttons (admin-only)
- **Port mappings**: Shows published host:container ports

**Panel visibility:**
- Hidden if Docker socket not found
- Admin-only by default
- Grantable via: `role=panel-viewer, panel_scope=docker`

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/docker/containers` | List all containers (query: `?status=running`) |
| GET | `/api/docker/containers/{id}` | Inspect specific container |
| POST | `/api/docker/containers/{id}/control` | Control container (`action: start\|stop\|restart`) |

**Control request:**
```json
POST /api/docker/containers/abc123/control
{
  "action": "restart"
}
```

### Prometheus Metrics

Docker container metrics are automatically exposed for Prometheus scraping:

```
docker_container_cpu_percent{peer, container_id, container_name, image}
docker_container_memory_bytes{peer, container_id, container_name, image}
docker_container_memory_percent{peer, container_id, container_name, image}
docker_container_disk_bytes{peer, container_id, container_name, image}
docker_container_status{peer, container_id, container_name, image}  # 1=running, 0=stopped
docker_container_info{peer, container_id, container_name, image, status, network_mode}
```

### Implementation Details

**Files:**
- `internal/docker/` - Docker manager, events watcher, port forwarding, metrics, stats persistence
- `internal/coord/docker.go` - Coordinator API handlers
- `internal/coord/web/js/docker.js` - Frontend panel implementation
- `internal/coord/s3/system.go` - S3 persistence for Docker stats

**Architecture:**
- Each coordinator/peer monitors its own local Docker daemon (no cross-peer aggregation)
- Event watcher streams Docker events API in real-time
- Automatically syncs port forwards on container start events (bridge networks only)
- Debounces rapid events (100ms window) to prevent duplicate processing
- Filter rules expire naturally via TTL mechanism (24h)
- **Stats persistence**: Collects comprehensive Docker stats every 30 seconds to S3

**Stats Persistence:**
- **Collection interval**:
  - Docker stats: 30 seconds
  - Network stats: 10 seconds (via heartbeat)
- **Storage location**: S3 system bucket at `stats/{peer_name}.{function}.json`
- **Naming convention**:
  - `stats/{peer_name}.docker.json` - Docker container stats (peer-side collection)
  - `stats/{peer_name}.network.json` - Network transmission stats (coordinator-side collection)
- **Data collected**:
  - **Docker stats**: Full `docker inspect` output, runtime stats (CPU%, memory, disk), Docker networks
  - **Network stats**: Bytes/packets sent/received rates, active tunnels, dropped packets
- **Format**: JSON with checksum validation for corruption detection
- **Architecture**:
  - Docker stats: Collected by each peer with Docker installed
  - Network stats: Collected by coordinator from all peer heartbeats

**Testing:**
- 20+ unit tests with mock Docker client
- TDD approach: tests written before implementation
- 100% coverage on core Docker manager functionality

## S3Bench - Story-Driven S3 Testing Tool

`tunnelmesh-s3bench` is a narrative-driven S3 stress testing tool that generates realistic workloads based on story scenarios.

### Modes

**Standalone Mode (default):**
- Documents stored locally in temporary directory
- Tests S3 store, deduplication, versioning, RBAC without network

**Mesh Mode (opt-in via `--coordinator`):**
- Registers as lightweight peer with coordinator
- Uploads documents to coordinator's S3 API via mesh network
- Documents viewable in Objects browser web UI
- No TUN device, SSH server, or UDP transport needed

### Common Commands

```bash
# List available scenarios
tunnelmesh-s3bench list

# Describe scenario details
tunnelmesh-s3bench describe alien_invasion

# Run scenario (standalone mode)
tunnelmesh-s3bench run alien_invasion --time-scale 100

# Run scenario (mesh mode)
tunnelmesh-s3bench run alien_invasion \
  --coordinator https://localhost:8443 \
  --time-scale 100 \
  --json results.json

# Quick test (1 minute)
tunnelmesh-s3bench run alien_invasion --time-scale 4320

# Stress test (5 minutes)
tunnelmesh-s3bench run alien_invasion --time-scale 864 --enable-adversary

# Realistic demo (2 hours)
tunnelmesh-s3bench run alien_invasion --time-scale 36
```

### Key Flags

- `--coordinator <url>` - Enable mesh mode, upload to coordinator (e.g., `https://coord.example.com:8443`)
- `--ssh-key <path>` - SSH key for peer identity (default: `~/.tunnelmesh/s3bench_key`)
- `--insecure-tls` - Skip TLS verification (default: true for self-signed certs)
- `--time-scale <N>` - Time scaling factor (1.0=realtime, 100.0=100x faster)
- `--concurrency <N>` - Number of parallel users (default: 3)
- `--json <file>` - Write results to JSON file
- `--enable-adversary` - Enable adversarial simulation (default: true)
- `--enable-workflows` - Enable workflow tests (default: true)

### Mesh Integration Architecture

1. **Registration** - s3bench calls `/api/v1/register` (public endpoint)
   - Sends: name, public key, IPs (not listening on any ports)
   - Receives: mesh IP, coordinator mesh IP, peer ID

2. **Credential Derivation** - S3 credentials derived from SSH public key using HKDF
   - Access key: 20 character hex string
   - Secret key: 40 character hex string

3. **Bucket Creation** - Creates buckets on coordinator for each story department
   - Endpoint: `POST https://{coord_mesh_ip}:443/api/s3/buckets`
   - Auth: HTTP Basic Auth with derived credentials

4. **Document Upload** - Uploads documents with metadata preservation
   - Endpoint: `PUT https://{coord_mesh_ip}:443/api/s3/buckets/{bucket}/objects/{key}`
   - Metadata: author, doc_type, phase, clearance, version (as `x-amz-meta-*` headers)

### Implementation Files

- `cmd/tunnelmesh-s3bench/` - CLI entrypoint
- `internal/s3bench/mesh/` - Mesh integration (registration, credentials, API client)
- `internal/s3bench/simulator/` - Workload generation and execution
- `internal/s3bench/story/` - Story scenarios and characters
- `internal/s3bench/documents/` - Document generation

## Skills

Three specialized skill files are available in `.claude/skills/`:
- `senior-network-engineer.md` - NAT traversal, transports, crypto protocols
- `sre.md` - Terraform, Docker, Prometheus/Grafana/Loki
- `senior-developer.md` - Go patterns, testing, security, UI
