# Distributed Hash Table

This project implements a Distributed Hash Table (DHT) based on the **Chord** protocol. It supports dynamic node joins, departures, and crash recovery, utilizing consistent hashing to manage key-value pairs across a cluster of nodes.

## Prerequisites

This system is designed to run on the **UiT Clusters**. Before execution, ensure all shell scripts have the necessary execution permissions:

```bash
chmod +x run join.sh leave.sh
```

## Deployment

### 1. Launching the Network

To deploy a set of nodes on the cluster, use the `run` script. Each node starts in a "loner" state (a ring of size 1).

```bash
./run <amount_of_nodes>
```

**Note:** When deploying a high number of nodes (e.g., 32+), please wait until the script outputs all node addresses.

- A `nodes.txt` file will be generated automatically.
- This file contains the `IP:PORT` addresses of all active nodes and is used by the testing scripts.

### 2. Joining Nodes to the Ring

Once the nodes are deployed, they must be linked to form the Chord ring.

**Automatic Join:**
Use the provided script to read `nodes.txt` and join all nodes to a single bootstrap node:

```bash
./join.sh
```

**Manual Join (CURL):**
To join a specific "loner" node to an existing network, send a POST request to the node you want to move:

```bash
curl -X POST "http://<node_address>/join?nprime=<target_network_address>"
```

### 3. Leaving the Network

To gracefully remove nodes from the ring and return them to a single-node state:

```bash
./leave.sh
```

### 4. Handling Crashes & Cleanup

If the cluster becomes unresponsive or nodes fail to join correctly, you can force-kill all active processes:

```bash
python3 kill.py
```

---

## API Specification

The following HTTP endpoints:

### Storage

- `PUT /storage/<key>`: Stores the message body at the specific key using consistent hashing.
- `GET /storage/<key>`: Retrieves the value associated with the key.

### Node Management

- `GET /node-info`: Returns a JSON object containing the node's hash, successor, and other known neighbors (finger table).
- `POST /join?nprime=HOST:PORT`: Instructs the node to join the network containing `nprime`.
- `POST /leave`: Instructs the node to gracefully exit the network.
- `POST /sim-crash`: Simulates a node failure. The node will stop responding to all requests except `sim-recover`.
- `POST /sim-recover`: Restores a "crashed" node to an active state.
