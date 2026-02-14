import argparse
import time
import http.client
import json
import logging
import random

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_node_info(node_address):
    """Fetch the node information including node_hash, successor, and predecessor."""
    try:
        conn = http.client.HTTPConnection(node_address, timeout=5)
        conn.request("GET", "/node-info")
        res = conn.getresponse()
        data = json.loads(res.read())
        conn.close()
        return data
    except Exception as e:
        logging.error(f"Error fetching node info from {node_address}: {e}")
        return None

def crash_node(node_address):
    """Send a crash signal to a node."""
    try:
        conn = http.client.HTTPConnection(node_address, timeout=5)
        conn.request("POST", "/sim-crash")
        res = conn.getresponse()
        if res.status == 200:
            logging.info(f"Node {node_address} successfully crashed.")
        else:
            logging.error(f"Failed to crash node {node_address}. Status: {res.status}")
        conn.close()
    except Exception as e:
        logging.error(f"Error crashing node {node_address}: {e}")

def check_network_stability(nodes):
    """Check if the network is stable by ensuring each node has a valid successor and predecessor."""
    stable = True
    for node in nodes:
        node_info = get_node_info(node)
        if node_info is None:
            logging.error(f"Node {node} is unresponsive, might be crashed.")
            stable = False
            continue
        if not node_info.get('successor') or not node_info.get('predecessor'):
            logging.warning(f"Node {node} has invalid successor or predecessor: {node_info}")
            stable = False
        else:
            logging.info(f"Node {node} has valid connections (Successor: {node_info['successor']}, Predecessor: {node_info['predecessor']})")
    return stable

def simulate_burst_crashes(nodes, burst_size, crashed_nodes):
    """Simulate a burst of crashes, avoiding already crashed nodes."""
    available_nodes = [node for node in nodes if node not in crashed_nodes]
    
    if burst_size > len(available_nodes):
        logging.error(f"Not enough nodes to crash {burst_size} nodes. Available nodes: {available_nodes}")
        return []
    
    # Randomly select nodes to crash from the available ones
    nodes_to_crash = random.sample(available_nodes, burst_size)
    logging.info(f"Simulating crash for {burst_size} node(s): {nodes_to_crash}")
    
    for node in nodes_to_crash:
        crash_node(node)
    
    # Add crashed nodes to the list
    crashed_nodes.extend(nodes_to_crash)
    
    return nodes_to_crash

def main():
    parser = argparse.ArgumentParser(
        description="Simulate node crashes in a DHT network."
    )
    parser.add_argument(
        "nodes", type=str, nargs='+', help="List of node addresses"
    )
    args = parser.parse_args()

    nodes = args.nodes
    crashed_nodes = []  # Track crashed nodes

    # Start simulating bursts of crashes
    burst_size = 20
    while burst_size <= len(nodes):
        logging.info(f"\n--- Starting burst of {burst_size} node crash(es) ---")
        crashed_this_round = simulate_burst_crashes(nodes, burst_size, crashed_nodes)
        
        if not crashed_this_round:
            logging.error("No more nodes to crash or error in crashing. Ending test.")
            break

        # Wait for some time to let the network adjust
        time.sleep(200)

        # Check if the network is still stable with the remaining nodes
        logging.info(f"Checking network stability after crashing {burst_size} nodes...")
        stable = check_network_stability([node for node in nodes if node not in crashed_nodes])

        if stable:
            logging.info(f"Network stabilized after crashing {burst_size} node(s).")
        else:
            logging.error(f"Network failed to stabilize after crashing {burst_size} node(s). Stopping test.")
            break

        # Increment burst size for the next round
        # burst_size += 8

    logging.info("Test completed.")

if __name__ == "__main__":
    main()

