# Peer Configuration

exo requires manual peer configuration to provide explicit control over cluster membership. There are two modes:

1. **Orchestrator Mode (Recommended)**: Nodes register with a central orchestrator server that manages peer discovery
2. **File Mode**: Nodes read peer configuration from a local JSON file

## Orchestrator Mode (Recommended)

Orchestrator mode enables dynamic peer discovery through a centralized server. This is ideal for production deployments where nodes can dynamically join and leave the cluster.

### How It Works

1. Each node registers itself with the orchestrator server on startup
2. The orchestrator responds with a list of all other peers in the cluster
3. Nodes connect to their peers using the provided addresses

### Orchestrator API

The orchestrator must implement the following API:

**Endpoint**: `POST /register`

**Request Body**:
```json
{
  "name": "My Provider",
  "ip": "192.168.1.100",
  "port": 52415
}
```

**Response Body**:
```json
{
  "peers": [
    {"ip": "192.168.1.101", "port": 52415},
    {"ip": "192.168.1.102", "port": 52415}
  ]
}
```

### CLI Usage

```bash
# Basic usage - auto-detect node's IP and use hostname as name
uv run exo --orchestrator 10.0.0.100:8080

# Specify custom node name
uv run exo --orchestrator 10.0.0.100:8080 --node-name "GPU-Node-1"

# Specify custom node IP (useful when auto-detection fails or for specific network interfaces)
uv run exo --orchestrator 10.0.0.100:8080 --node-host 192.168.1.100

# Full example with all options
uv run exo \
  --orchestrator 10.0.0.100:8080 \
  --node-name "GPU-Node-1" \
  --node-host 192.168.1.100 \
  --api-port 52415
```

### Example Setup

#### 1. Start Orchestrator Server

First, run your orchestrator server (example implementation not included):

```bash
# Your orchestrator implementation
python orchestrator_server.py --port 8080
```

#### 2. Start Worker Nodes

```bash
# Node 1
uv run exo --orchestrator localhost:8080 --node-name "Worker-1"

# Node 2
uv run exo --orchestrator localhost:8080 --node-name "Worker-2"

# Node 3
uv run exo --orchestrator localhost:8080 --node-name "Worker-3"
```

Each node will automatically:
- Detect its own IP address
- Register with the orchestrator
- Receive a list of other peers
- Connect to all peers in the cluster

### Orchestrator Benefits

- **Dynamic Discovery**: Nodes automatically find each other without manual configuration
- **Centralized Management**: Single point to manage cluster membership
- **Easy Scaling**: Add new nodes without updating existing configurations
- **Health Monitoring**: Orchestrator can track which nodes are active
- **Cross-Network Support**: Works across different subnets and networks

## File Mode (Alternative)

For simpler deployments or when an orchestrator is not available, you can use file-based configuration.

### Configuration File

Create a `peers.json` file in your exo config directory:
- **Linux**: `~/.config/exo/peers.json`
- **macOS**: `~/.exo/peers.json`
- **Custom**: Set via `EXO_HOME` environment variable

### Format

```json
{
  "peers": [
    {"ip": "192.168.1.100", "port": 45123},
    {"ip": "192.168.1.101", "port": 45123},
    {"ip": "10.0.0.50", "port": 45123}
  ]
}
```

**Important notes:**
- `ip`: IPv4 or IPv6 address of the peer
- `port`: TCP port the peer is listening on (1-65535)
- At least one peer must be specified
- Peer IDs are discovered automatically during handshake

## Finding Peer Ports

When a node starts, it logs its listening port:

```
INFO Starting node 12D3KooW...
INFO Listening on /ip4/0.0.0.0/tcp/45123
```

Use this port in other nodes' peer configuration files.

### File Mode CLI Usage

```bash
# Use default config location (~/.config/exo/peers.json or ~/.exo/peers.json)
uv run exo

# Specify a custom config file path
uv run exo --peers-file /path/to/peers.json
```

### Example: Two-Node Setup

### Node 1 (192.168.1.100)

1. Start the first node to get its port:
   ```bash
   uv run exo
   # Output: INFO Listening on /ip4/0.0.0.0/tcp/45123
   ```

2. Create `~/.config/exo/peers.json` for Node 2:
   ```json
   {
     "peers": [
       {"ip": "192.168.1.100", "port": 45123}
     ]
   }
   ```

### Node 2 (192.168.1.101)

1. Start Node 2 to get its port:
   ```bash
   uv run exo
   # Output: INFO Listening on /ip4/0.0.0.0/tcp/45124
   ```

2. Update Node 1's `~/.config/exo/peers.json`:
   ```json
   {
     "peers": [
       {"ip": "192.168.1.101", "port": 45124}
     ]
   }
   ```

3. Restart Node 1 for changes to take effect

Both nodes should now connect to each other.

## Cross-Subnet Configuration

Manual peer configuration enables cross-subnet clustering, which wasn't possible with mDNS:

```json
{
  "peers": [
    {"ip": "10.0.1.100", "port": 45123},   # Subnet A
    {"ip": "10.0.2.100", "port": 45123},   # Subnet B
    {"ip": "192.168.1.50", "port": 45123}  # Subnet C
  ]
}
```

Ensure firewall rules allow TCP connections on the specified ports.

## Choosing Between Orchestrator and File Mode

### Use Orchestrator Mode When:
- You have many nodes that frequently join/leave the cluster
- You want centralized management and monitoring
- You're deploying in production or cloud environments
- You want to avoid manual configuration updates
- Nodes are distributed across different networks

### Use File Mode When:
- You have a small, stable cluster (2-5 nodes)
- All nodes are on the same local network
- You want minimal dependencies (no orchestrator server needed)
- You prefer explicit, auditable configuration
- You're testing or developing locally

## Migration from mDNS

**Breaking Change:** Automatic discovery has been removed. You have two migration options:

### Option 1: Orchestrator Mode (Recommended for production)

1. **Deploy an orchestrator server**: Implement or deploy an orchestrator that handles the `/register` endpoint
2. **Start nodes with orchestrator**: Use `--orchestrator <ip:port>` when starting each node
3. **Nodes auto-discover**: Each node will automatically register and receive peer list

### Option 2: File Mode (Simpler for small clusters)

1. **Identify your peers**: Note the IP addresses of all devices in your cluster
2. **Start each node**: Run each node to discover its listening port
3. **Create config files**: For each node, create a `peers.json` listing the other nodes
4. **Restart all nodes**: Apply the configuration by restarting exo on all devices

## Troubleshooting

### Orchestrator Mode Issues

#### Error: "Failed to register with orchestrator"

**Solution**:
- Verify orchestrator server is running and accessible
- Check orchestrator address format: `ip:port`
- Ensure network connectivity: `curl -X POST http://<orchestrator-ip>:<port>/register`
- Check orchestrator logs for errors

#### Error: "Orchestrator address must be in format 'ip:port'"

**Solution**: Use the correct format, e.g., `--orchestrator 10.0.0.100:8080`

#### Node IP auto-detection fails

**Solution**: Manually specify node IP with `--node-host`:
```bash
uv run exo --orchestrator 10.0.0.100:8080 --node-host 192.168.1.100
```

### File Mode Issues

#### Error: "No peer configuration file found"

**Solution**: Create a `peers.json` file in the default location or specify `--peers-file`.

#### Error: "Invalid JSON in peer config"

**Solution**: Validate your JSON syntax. Common issues:
- Missing commas between objects
- Trailing commas after the last item
- Unquoted strings

### Error: "Invalid IP address"

**Solution**: Ensure IP addresses are valid IPv4 (e.g., `192.168.1.100`) or IPv6 format.

### Error: "Port must be between 1 and 65535"

**Solution**: Use a valid TCP port number within the allowed range.

### Nodes not connecting

**Checklist**:
1. Verify peer IPs are reachable: `ping <peer-ip>`
2. Verify ports are open: `nc -zv <peer-ip> <peer-port>`
3. Check firewall rules on both nodes
4. Ensure both nodes have each other in their configs
5. Check logs for connection errors: `uv run exo -v`

## Security Considerations

- **Explicit control**: Only configured peers can join the cluster
- **Firewall protection**: Restrict TCP port access to known peer IPs
- **Private networks**: Use VPNs or private networks for cross-internet clusters
- **Network encryption**: libp2p provides encrypted communication between peers

## Advanced Configuration

### Dynamic Port Assignment

If you don't want to manually track ports, you can:
1. Use a fixed port by binding to a specific port (requires code modification)
2. Use a service discovery system to publish ports
3. Use a configuration management tool to distribute peer configs

### Large Clusters

For large clusters (10+ nodes):
- Use a centralized configuration management system
- Consider mesh vs. star topology tradeoffs
- Monitor connection health via logs
