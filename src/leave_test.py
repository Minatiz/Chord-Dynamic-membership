import argparse
import time
import http.client
import json
import logging
import random

# Set up logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def leave_node(node_address):
    """Send a request to a node to leave the Chord ring."""
    try:
        logging.info(f"Node {node_address} is leaving the network.")
        conn = http.client.HTTPConnection(node_address, timeout=5)
        conn.request("POST", "/leave")
        res = conn.getresponse()

        if res.status == 200:
            logging.info(f"Node {node_address} successfully left the network.")
        else:
            logging.error(f"Failed to remove node {node_address}. Status: {res.status}")

        conn.close()
    except Exception as e:
        logging.error(f"Error while removing node {node_address}: {e}")


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

def check_others_change(nodes, length, required_stable_duration=15, interval=4):
    """
    Check if the 'others' field in the node-info changes.
    The function runs indefinitely until all nodes have no changes in their 'others' field for the required duration.
    """
    if length >= 4 and length < 16:
        required_stable_duration = 20
    elif length >= 16 and length < 24: 
        required_stable_duration = 40
        interval = 7
    elif length >= 24: 
        required_stable_duration = 60 
        interval = 10

    last_others = {node: get_node_info(node)['others'] for node in nodes}
    unchanged_time = {node: 0 for node in nodes}

    while True:
        change_detected = False

        for node in nodes:
            current_info = get_node_info(node)
            if current_info:
                current_others = current_info['others']

                if current_others != last_others[node]:
                    logging.info(f"'others' for node {node} changed.")
                    last_others[node] = current_others
                    change_detected = True

        if change_detected:
            logging.info(f"Resetting time for all nodes due to a change.")
            unchanged_time = {node: 0 for node in nodes}
        else:
            for node in nodes:
                unchanged_time[node] += interval

            if all(unchanged_time[node] >= required_stable_duration for node in nodes):
                logging.info(f"No change in 'others' for any node in the last {required_stable_duration} seconds.")
                return True  

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Remove nodes."
    )
    parser.add_argument(
        "nodes", type=str, nargs='+', help="List of node addresses"
    )
    args = parser.parse_args()

    nodes = args.nodes

    if len(nodes) < 2:
        logging.error("At least 2 nodes are required.")
        return
    # Start measuring time
    start_time_e = time.time()

    # Determine how many nodes should leave (half of the total nodes)
    num_nodes_to_leave = len(nodes) // 2
    nodes_to_leave = random.sample(nodes, num_nodes_to_leave)  # Randomly select half of the nodes to leave


    # Leave the network for each selected node
    for node in nodes_to_leave:
        leave_node(node)
        time.sleep(4)# Not to overflow the requests with leaves. 
    
    # Check for changes in 'others' field for the remaining nodes
    length_list_node = len(nodes)
    others_test_passed = check_others_change(nodes, length_list_node)

    if others_test_passed:
        logging.info("Others field test passed.")
        # Measure the total time
        end_time_e = time.time()
        total_time_e = end_time_e - start_time_e
        logging.info(f"Total time for stable leave operation: {total_time_e:.2f} seconds")


if __name__ == "__main__":
    main()

