from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
import time
import argparse
import http.client
import hashlib
import requests
import socket

M = 16  # Indentifier.
HASH_SPACE = 2**M


# SHA1 hashing, for consistent hashing. Used for hashing nodes and keys.
def hash_sha1(value: str) -> int:
    return int(hashlib.sha1(value.encode()).hexdigest(), 16) % HASH_SPACE


# The node class.
class Node:
    """
    Represents a node in a distributed hash table.

    Attributes:
        address (str): The address of the node.
        finger_table (list): The finger table for routing.
        node_id (int): The unique identifier of the node.
        data (dict): The data stored in the nodes hash table.
        successor (Node): The successor node.
        predecessor (Node): The predecessor node.
        next (int): The next index for stabilization.
        crashed (bool): Marks if the node has crashed.
        has_left (bool): Marks if the node has left the network.
        joined_via_node(int): Address of the bootstrap node it joined via
        backup(int): Backup of the predecessor address. 
    """

    def __init__(self, address):
        self.address = address
        self.finger_table = [self] * M
        self.node_id = hash_sha1(address)
        self.data = {}
        self.successor = self
        self.predecessor = None
        self.next = 0
        self.crashed = False
        self.has_left = False
        self.joined_via_node = None
        self.backup = None 

    def create(self):
        self.successor = self
        self.predecessor = None

    # Initializing the finger table
    def init_finger_table(self):
        for i in range(M):
        # finger[k] logic from Chord paper (n + 2^K-1) mod 2^m and 1<= k <= m.
            if 1 <= i <= M:
                s = (self.node_id + 2**(i-1)) % HASH_SPACE
                # Find successor and append to the nodes finger table.
                successor = self.find_successor(s)
                self.finger_table[i] = successor
                print(
                    f"Entry: {i} Node + s: {self.address} + {s}, Successor: {successor.address}, Node_ID: {successor.node_id}\n")

    # Finding the successor node based on the given hashed key(ID).
    def find_successor(self, hashed_key):

        # If the hashed_key is in the range (node_id, successor.node_id], this is the successor and return it
        if self.node_id < hashed_key <= self.successor.node_id:
            return self.successor
        else:
            # This handles the wrap-around case in the chord ring where node_id > successor.node_id. And when the key is > nodeid or <= succesor nodeid.
            if self.node_id > self.successor.node_id and (hashed_key > self.node_id or hashed_key <= self.successor.node_id):
                # print(f"Successor is: {self.successor.address}")
                return self.successor

            # Use the finger table to find the closest preceding node
            closest_preceding = self._closest_preceding_node(hashed_key)
            print(f"Closest preceding node: {closest_preceding.address}")

            # If the closets node is it self then we return the successor, avoid infinite recursive calls.
            if closest_preceding == self:
                return self.successor

            # Sending POST request to the closest preceding node to find the successor with the id.
            response = requests.post(
                f'http://{closest_preceding.address}/find_successor',
                json={'hashed_key': hashed_key}, timeout=10
            )
            if response.status_code == 200:
                successor_data = response.json()
                print(
                    f"POST Successor found: {successor_data['node_address']}")
                return Node(successor_data['node_address'])
            else:
                return self.successor

    def _closest_preceding_node(self, hashed_key):
        # Searches in the finger table in reverse for the highest node that precedes hashed_key
        # print(f"Finding closest preceding node for key: {hashed_key}")
        for finger in reversed(self.finger_table):
            # print(
            #     f"Checking finger: {finger.address} with ID: {finger.node_id}")
            if self.node_id < finger.node_id < hashed_key:
                # print(
                #     f"Finger {finger.address} is between {self.node_id} and {hashed_key}")
                return finger
            elif self.node_id > hashed_key and (finger.node_id > self.node_id or finger.node_id < hashed_key):
                # print(
                #     f"Finger {finger.address} wraps around and is between {self.node_id} and {hashed_key}")
                return finger
        print("No suitable finger found, returning self")
        return self

    # Join when node joins network.
    def join(self, join_address):
        self.has_left = False
        self.joined_via_node = join_address
        # Not smart to join itself.
        if self.address == join_address:
            print("Node is attempting to join itself; no action taken.")
            return

        # Get the node object and use it for joining to the correct network.
        try:
            connection = http.client.HTTPConnection(join_address)
            connection.request("GET", "/node-info")
            response = connection.getresponse()

            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                joined_node = Node(data['node_address'])
                self.successor = joined_node.find_successor(self.node_id)
                self.predecessor = None

                if self.successor.node_id == self.node_id:
                    print("Successor cannot be the same as current node.")
                    return

                # Notify successor and stabilize and intialize finger table. Node has now joined network. Populates finger table with correct entries.
                self._notify_successor()
                self.stabilize()
                self.init_finger_table()
            else:
                print(f"Join request failed with status {response.status}")
        except Exception as e:
            print(f"Error during join: {str(e)}")

    def _notify_successor(self):
        requests.post(f'http://{self.successor.address}/notify', json={
            'node': {'node_id': self.node_id, 'node_address': self.address}
        }, timeout=10)

    # This is called periodcally for checking if the predecessor or the successor is the right one for the node.
    # And then updates it to correct successor and predecessor. It keeps the chord ring circular.
    def notify(self, node):
        incoming_node = Node(node['node_address'])
        # print(f"Notify called with node: {incoming_node.address}")

        # Check and update predecessor (also checks wrap-around case)
        if self.predecessor is None or (
            (self.predecessor.node_id < incoming_node.node_id < self.node_id) or
            (self.predecessor.node_id > self.node_id and
             (incoming_node.node_id < self.node_id or incoming_node.node_id > self.predecessor.node_id))
        ):
            # print(f"Updating predecessor to: {incoming_node.address}")
            self.predecessor = incoming_node

        # Updates successor if necessary (also checks wrap-around case)
        if self.successor.node_id == self.node_id or (
            incoming_node.node_id < self.successor.node_id and
            incoming_node.node_id > self.node_id
        ) or (
            self.node_id > self.successor.node_id and (
                incoming_node.node_id > self.node_id or incoming_node.node_id < self.successor.node_id)
        ):
            # print(f"Updating successor to: {incoming_node.address}")
            self.successor = incoming_node

    # This is called periodcally and manually to update the sucessor for each node with information.
    def stabilize(self):
        try:
            # Checking if the current successor is alive or if it has left network.
            if not self._ping_alive(self.successor.address) or self.successor.has_left is True:
                print(
                    f"Successor {self.successor.address} is not responding, updating successor.")
                # Find the next available node in the finger table or reset to itself
                self.successor = self._find_next_active_node()

            if self.successor.address != self.address:
                # Get details from the sucessor predecessor its id and address to determiner if close neighbour
                response = requests.get(
                    f'http://{self.successor.address}/predecessor', timeout=10)
                if response.status_code == 200:
                    pred_data = response.json()
                    if pred_data:
                        x = Node(pred_data['node_address'])
                        # Check if the predecessor of the successor is closer
                        if (self.node_id < x.node_id < self.successor.node_id) or (
                            self.node_id > self.successor.node_id and (
                                x.node_id > self.node_id or x.node_id < self.successor.node_id)
                        ):  
                            # Set it as the successor when passed.
                            self.successor = x
                # Notify the successor
                self._notify_successor()
        except requests.exceptions.RequestException as e:
            print(f"Error during stabilization: {e}")

    def _find_next_active_node(self):
        # Attempt to find the next active node from the finger table
        for finger in self.finger_table:
            if self._ping_alive(finger.address) and not finger.has_left:
                return finger
        # If no active node is found, the node points to itself
        return self

    # Follows chord paper
    # This method is called periodcally by every node to fix their finger tables entries. 
    # This is because when node join or leave network they should update their finger table for correct routing. 
    def fix_fingers(self):
        # If node is crashed, we cant do anything with that. 
        if self.crashed is not True:
            self.next += 1
            if self.next > (M):
                self.next = 1
            finger_index = (self.node_id + 2 ** (self.next-1)) % HASH_SPACE # finding the correct index in the finger table
            new_successor = self.find_successor(finger_index) # Find the correct sucessor 
            # If the new sucessor is a node that has left or is crshed we need to remove it from entries, we don't want other nodes to have it in the finger table. 
            if new_successor.has_left or new_successor.crashed:
                return
            # print(
            #     f"Node {self.address} updating finger table entry {self.next - 1} with successor: {new_successor.address}")
            # Inserting it to finger table and calling to stabilize. 
            self.finger_table[self.next - 1] = new_successor
            self.stabilize()
    
    # Follows chord paper. 
    # This method is called periodcally by every node to check if their predecessor is alive, if not it should be set to none. 
    def check_predecessor(self):
        if self.predecessor is None:
            return
        if not self._ping_alive(self.predecessor.address):
            print(
                f"Predecessor {self.predecessor.address} is not responding, clearing predecessor.")
            self.predecessor = None

    # Used for checking if the node is alive. 
    def _ping_alive(self, address):
        try:
            print(f"Ping check {address} if online")
            response = requests.get(f'http://{address}/ping', timeout=10)
            if response.status_code == 200:
                return True
            else:
                return False

        except requests.ConnectionError:
            return False
    # Updates info about the node in node-info call 
    def _set_others(self):
        others = set() # Not interested in duplicate values using set first. 
        if self.predecessor:
            others.add(self.predecessor.address)
        for finger in self.finger_table:
            others.add(finger.address)
        return list(others)

    # Information the node provides. 
    def get_node_info(self):
        node_info = {
            "node_address": self.address,
            "node_hash": self.node_id,
            "others": self._set_others(),
            "predecessor": self.predecessor.address if self.predecessor else None,
            "successor": self.successor.address if self.successor else None,
        }
        return node_info

    # Node leaves network and goes to loner state, before doing so it notifies its predecessor and sucessor to update their neighbours. 
    def leave(self):
        self.backup = self.predecessor.address # In case a left node did crash. And wanna rejoin to network.  
        self.has_left = True
        print(f"Node {self.address} is leaving the network.")

        # Tell predecessor to update its successor
        if self.predecessor:
            requests.post(f'http://{self.predecessor.address}/update_successor', json={
                'successor': self.successor.address if self.successor != self else None
            }, timeout=10)
           
        # Tell successor to update its predecessor
        if self.successor:
            requests.post(f'http://{self.successor.address}/update_predecessor', json={
                'predecessor': self.predecessor.address if self.predecessor else None
            },  timeout=10)
         
        self.predecessor = None
        self.successor = self
        self.finger_table = [self] * M
        print(
            f"Node {self.address} has left the network and reset its state.")

    # Crashes node and sets it to loner state. Doesn't notify anyone,since it cant perform any calls to fix finger or stabilize itself it will be unreachable also in the api calls. 
    def crash_node(self):
        print(f"Simulating crash for node: {self.address}")
        # Right in the crash the node just saves the predecessor address it was connected to. This is to simulate when a node crashes it back ups its data to a file, this just do it in the program. 
        if self.backup: 
            self.backup = self.predecessor.address
        self.crashed = True
        self.predecessor = None
        self.successor = self
        self.finger_table = [self] * M

    # Crashed node recovers, either it recovers by rejoining the bootstrap node, but what happens if that was crashed? no longer desentralized system, so it has also the backup address of it predecessor to join via. 
    def recover_node(self):
        print(f"Recovering node: {self.address}")
        self.crashed = False
        if self._ping_alive(self.joined_via_node): # Via joined node (bootstrap node)
            print(f'Node: {self.address} recovering via {self.joined_via_node}')
            self.join(self.joined_via_node)
            return True
        else: 
            print(f'Node: {self.address} recovering via {self.backup}')
            if self._ping_alive(self.backup) and self.backup is not None: 
                self.join(self.backup) # Or via the backup. 
                return True
            else: 
                print(f'Completely failure on recover')
                return False
             

    # Inserting value based of its hashed id to the correct node id
    def put_action(self, key, value):
        hashed_key = hash_sha1(key)
        # Finding the correct successor to forward the the value to 
        correct_node = self.find_successor(hashed_key)

        # When found match. 
        if correct_node.node_id == self.node_id:
            print(f"Storing key: {key} and value on node: {self.node_id}")
            self.data[key] = value
        # If isnt found means we need to forward it in the network to the correct node and insert it. 
        else:
            try:
                print(
                    f"Forwarding PUT to: {correct_node.address} with key: {key}")
                connection = http.client.HTTPConnection(
                    correct_node.address, timeout=8)
                headers = {'Content-Type': 'text/plain'}
                connection.request(
                    "PUT", f"/storage/{key}", body=value.encode('utf-8'), headers=headers)
                response = connection.getresponse()
                if response.status == 200:
                    print(
                        f"PUT request successfully forwarded to {correct_node.address}")
                else:
                    print(
                        f"Failed to PUT key: {key} to node {correct_node.address} with status {response.status}")
            except (http.client.HTTPException, socket.timeout) as e:
                print(f"Error forwarding PUT request: {e}")
            finally:
                connection.close()
    
    # Retrieving value based of its hased it from the correct node. 
    def get_action(self, key):
        hashed_key = hash_sha1(key)

        correct_node = self.find_successor(hashed_key)

        if correct_node.node_id == self.node_id:
            return self.data.get(key)
        else:
            try:
                print(
                    f"Forwarding GET request to: {correct_node.address} for key: {key}")
                connection = http.client.HTTPConnection(
                    correct_node.address, timeout=8)
                connection.request("GET", f"/storage/{key}")
                response = connection.getresponse()
                response_body = response.read().decode("utf-8")
                if response.status == 200:
                    return response_body
                else:
                    print(
                        f"GET request failed with status {response.status} on node {correct_node.address}")
                    return None
            except (http.client.HTTPException, socket.timeout) as e:
                print(f"Error forwarding GET request: {e}")
                return None
            finally:
                connection.close()


# HTTP request handler for the DHT.
class DHTHandler(BaseHTTPRequestHandler):
    def send_error(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"error": message}).encode())

    def do_PUT(self):
        # If node is crashed it can't perform any put requests. 
        if self.server.node.crashed:
            self.send_error(503, "Service Unavailable - Node is crashed")
            return
        if self.path.startswith('/storage/'):
            key = self.path.split('/storage/')[1]
            value = self.rfile.read(
                int(self.headers.get('Content-length'))).decode('utf-8')
            print(f"PUT request received for key: {key}")
            self.server.node.put_action(key, value)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_error(404, "Not Found - This API doesn't exist")

    def do_POST(self):
        # If node is crashed it can only perform POST recover call, rest POST calls is blocked. 
        if self.server.node.crashed and self.path != '/sim-recover':
            self.send_error(503, "Service Unavailable - Node is crashed")
            return
        # Join call
        if self.path.startswith("/join"):
            node_url = self.path.split("nprime=")
            if node_url:
                print(
                    f"\nNode: {self.server.node.address} joining network via {node_url[1]}")
                self.server.node.join(node_url[1])
                response_message = (
                    f"Node: {self.server.node.address} joined {node_url[1]} network successfully")
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(response_message.encode())
            else:
                self.send_error(400, "Bad Request - /join")
        # Notify call, mostly used inside the methods to update. 
        elif self.path.startswith("/notify"):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            new_node = json.loads(post_data.decode('utf-8')).get('node')
            if new_node is not None:
                self.server.node.notify(new_node)
                response_message = {"status": "success"}
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_message).encode())
            else:
                self.send_error(
                    400, "Bad Request - /notify Error: Invalid node data")
        # Leave the network 
        elif self.path == "/leave":
            if self.server.node.has_left is not True: 
                print(f"\nNode: {self.server.node.address} leaving the network")
                self.server.node.leave()

                response_message = {
                    "message": f"Node: {self.server.node.address} has left the network"
                }
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(json.dumps(response_message).encode())
            else: 
                self.send_error(400, "Bad request - Node has already left network")
        # Call to update the nodes sucessor in the network 
        elif self.path == "/update_successor":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            self.server.node.successor = Node(data['successor'])
            response_message = {"status": "success"}
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_message).encode())
        # Call to update the nodes predecessor in the network 
        elif self.path == "/update_predecessor":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            self.server.node.predecessor = Node(
                data['predecessor']) if data['predecessor'] else None
            response_message = {"status": "success"}
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_message).encode())
        # Call for forward it to the right sucessor. 
        elif self.path.startswith('/find_successor'):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            hashed_key = data['hashed_key']
            successor = self.server.node.find_successor(hashed_key)
            successor_info = {
                'node_id': successor.node_id,
                'node_address': successor.address
            }
            json_data = json.dumps(successor_info).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-length', len(json_data))
            self.end_headers()
            self.wfile.write(json_data)
        # Simulates a crash of a node. 
        elif self.path == "/sim-crash":
            self.server.node.crash_node()
            response_message = {
                "status": "success", "message": f"Node {self.server.node.address} simulated crash."}
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_message).encode())
        # Recovers the crashed node. 
        elif self.path == "/sim-recover":
            if self.server.node.crashed:
                status = self.server.node.recover_node()
                response_message = {
                    "status": "success", "message": f"Node {self.server.node.address} recovered from crash."}
                if status is False: 
                    response_message = {
                    "status": "failed", "message": f"Node {self.server.node.address} Failed via backup and bootstrap."}
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_message).encode())
            else:
                self.send_error(400, "Bad Request - Node is not crashed")

        else:
            self.send_error(404, "Not Found - This API doesn't exist")
            return

    # Do GET for network and storage
    def do_GET(self):
        # If node is crashed it cant perform any GET calls. 
        if self.server.node.crashed:
            self.send_error(503, "Service Unavailable - Node is crashed")
            return
        # Retrieve info about the node. 
        if self.path == "/node-info":
            node_info = self.server.node.get_node_info()
            json_data = json.dumps(node_info).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Content-length", len(json_data))
            self.end_headers()
            self.wfile.write(json_data)
        # Used to get the sucessor predecessor. 
        elif self.path.startswith('/predecessor'):
            if self.server.node.predecessor:
                predecessor_info = {
                    'node_id': self.server.node.predecessor.node_id,
                    'node_address': self.server.node.predecessor.address
                }
                json_data = json.dumps(predecessor_info).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Content-length', len(json_data))
                self.end_headers()
                self.wfile.write(json_data)
            else:
                print("No predecessor found.")
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({}).encode('utf-8'))
        # Retrieve value from node hash table. 
        elif self.path.startswith("/storage/"):
            key = self.path.split("/storage/")[1]
            value = self.server.node.get_action(key)
            if value:
                self.send_response(200)
                self.send_header(
                    "Content-type", "text/plain; charset=utf-8")
                self.send_header("Content-length",
                                 len(value.encode("utf-8")))
                self.end_headers()
                self.wfile.write(value.encode("utf-8"))
            else:
                self.send_error(
                    404, f"Not Found - /storage Key: {key} not found")
        # Ping to check if node is alive. 
        elif self.path.startswith('/ping'):
            self.send_response(200)
            self.end_headers()

        else:
            self.send_error(404, "Not Found - This API doesn't exist")


def run_server(node):
    host, port = node.address.split(":")
    port = int(port)
    server = HTTPServer((host, port), DHTHandler)
    server.node = node
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Node {node.address} hashed {node.node_id} is running...")

    # Task that are called periodcally, follows Chord paper logic
    def task_stabilize():
        while True:
            time.sleep(7)
            node.stabilize()

    def task_fix_finger():
        while True:
            time.sleep(3)
            node.fix_fingers()

    def task_check_pred():
        while True:
            time.sleep(5)
            node.check_predecessor()

    threading.Thread(target=task_stabilize, daemon=True).start()
    threading.Thread(target=task_fix_finger, daemon=True).start()
    threading.Thread(target=task_check_pred, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down the server")
        server.shutdown()


def arg_parser():
    parser = argparse.ArgumentParser(
        prog="server", description="DHT server/client")
    parser.add_argument("current_node", type=str,
                        help="address (host:port) of this node")
    return parser


def main(args):
    current_node_addr = args.current_node
    node = Node(current_node_addr)
    node.create()
    run_server(node)


if __name__ == "__main__":
    parser = arg_parser()
    args = parser.parse_args()
    main(args)
