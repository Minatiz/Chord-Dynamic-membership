import argparse
import time
import http.client
import json
import logging



# Set up logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def join_node(node_address, bootstrap_node_address):
    """Join a node to the Chord ring using a bootstrap node."""
    try:
        logging.info(
            f"Joining node {node_address} to network via {bootstrap_node_address}")
        conn = http.client.HTTPConnection(node_address, timeout=5)
        conn.request("POST", f"/join?nprime={bootstrap_node_address}")
        res = conn.getresponse()

        if res.status == 200:
            logging.info(
                f"Node {node_address} successfully joined the network")
        else:
            logging.error(
                f"Failed to join node {node_address}. Status: {res.status}")

        conn.close()
    except Exception as e:
        logging.error(f"Error while joining node {node_address}: {e}")


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


def verify_ring_structure(nodes_info):
    """Verify the ring structure based on node_hash values."""
    # Sort nodes by their node_hash
    sorted_nodes = sorted(nodes_info, key=lambda x: x['node_hash'])
    num_nodes = len(sorted_nodes)

    for i, node in enumerate(sorted_nodes):
        expected_successor = sorted_nodes[(i + 1) % num_nodes]
        expected_predecessor = sorted_nodes[(i - 1) % num_nodes]

        if node['successor'] != expected_successor['node_address']:
            logging.error(f"Node {node['node_address']} has incorrect successor. "
                          f"Expected: {expected_successor['node_address']}, Got: {node['successor']}")
            return False

        if node['predecessor'] != expected_predecessor['node_address']:
            logging.error(f"Node {node['node_address']} has incorrect predecessor. "
                          f"Expected: {expected_predecessor['node_address']}, Got: {node['predecessor']}")
            return False

    logging.info("Ring structure is correct.")
    return True



def check_others_change(nodes, length ,required_stable_duration=15, interval=4):
    """
    Check if the 'others' field in the node-info changes.
    The function runs indefinitely until all nodes have no changes in their 'others' field for the required duration.
    """
    # print(f'length: {length}\n')
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

                # If the 'others' field has changed for any node, set change_detected to True
                if current_others != last_others[node]:
                    logging.info(f"'others' for node {node} changed.")
                    last_others[node] = current_others
                    change_detected = True

        if change_detected:
            # Reset the unchanged time for all nodes if any change is detected
            logging.info(f"Resetting time for all nodes due to a change.")
            unchanged_time = {node: 0 for node in nodes}
        else:
            # Increment the time since the last change for all nodes
            for node in nodes:
                unchanged_time[node] += interval

            # Check if all nodes have had no changes for the required_stable_duration
            if all(unchanged_time[node] >= required_stable_duration for node in nodes):
                logging.info(f"No change in 'others' for any node in the last {required_stable_duration} seconds.")
                return True  # The test is considered complete

        # Sleep for the interval before checking again
        time.sleep(interval)

def main():
    parser = argparse.ArgumentParser(
        description="Connect nodes and verify the ring structure.")
    parser.add_argument(
        "nodes", type=str, nargs='+', help="List of node addresses"
    )
    args = parser.parse_args()

    nodes = args.nodes

    if len(nodes) < 2:
        logging.error("At least 2 nodes are needed to form a network.")
        return

    # First node is the bootstrap node
    bootstrap_node = nodes[0]

    # Starting measuring time
    start_time_e = time.time()

    # Join the rest of the nodes to the network
    for node in nodes[1:]:
        join_node(node, bootstrap_node)
    
    length_list_node = len(nodes)
    # Verify connections and the ring structure
    nodes_info = []
    for node in nodes:
        node_info = get_node_info(node)
        if node_info:
            nodes_info.append(node_info)

    if len(nodes_info) != len(nodes):
        logging.error("Not all nodes returned valid information.")
        return

    # Wait until all nodes have proper predecessor and successor
    all_connected = False
    while not all_connected:
        all_connected = all(
            node['predecessor'] is not None and node['successor'] is not None for node in nodes_info)

        if all_connected:
            logging.info("All nodes have valid predecessors and successors.")
        else:
            logging.info(
                "Waiting for all nodes to have predecessors and successors...")
            time.sleep(2)
            nodes_info = [get_node_info(node) for node in nodes]

    # Verify the correct ring structure
    ring_correct = verify_ring_structure(nodes_info)

    if ring_correct:
        logging.info("Ring structure verification passed.")
        logging.info(
                f"All nodes successfully connected via bootstrap {bootstrap_node}.") 
        logging.info("Next test: finger table entries updating (Fix finger action).")


        # Check for changes in 'others' field for each node
        others_test_passed = check_others_change(nodes, length_list_node)

        if others_test_passed:
            logging.info("Others field test passed")
            # Measure the total time
            end_time_e = time.time()
            total_time_e = end_time_e - start_time_e
            logging.info("Now use ./get.sh, and next test to run is ./put_test")
            logging.info(
                f"Total time for startup: {total_time_e:.2f} seconds. Amount nodes {len(nodes)}")
            

    else:
        logging.error("Ring structure verification failed.")
        print("Remember only this can be executed once, kill all server and rerun\n")


if __name__ == "__main__":
    main()
