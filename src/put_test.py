import requests
import unittest
import sys
import random
import string

random.seed(None)


class Test(unittest.TestCase):
    def setUp(self):
        if len(sys.argv) < 2:
            print("Run: python3 2_test.py <node1> <node2> ...")
            sys.exit(1)

        self.nodes = sys.argv[1:]

    def do_get_storage(self, node, key):
        url = f"http://{node}/storage/{key}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f"GET failed on {node}, Status code: {
                      response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"GET failed on {node}, Exception: {e}")
            return None

    def do_put_storage(self, node, key, value):
        url = f"http://{node}/storage/{key}"
        try:
            response = requests.put(url, data=value)
            if response.status_code == 200:
                print(f"PUT successful on {node}")
            else:
                print(f"PUT failed on {node}, Status code: {
                      response.status_code}")
        except requests.RequestException as e:
            print(f"PUT failed on {node}, Exception: {e}")

    def do_ping(self, node):
        url = f"http://{node}/ping"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"Ping successful on {node}")
                return True
            else:
                print(f"Ping failed on {node}, Status code: {
                      response.status_code}")
                return False
        except requests.RequestException as e:
            print(f"Ping failed on {node}, Exception: {e}")
            return False

    # Test PUT and GET storage
    def test_put_and_get(self):
        key = ''.join(random.choices(
            string.ascii_lowercase + string.digits, k=16))
        value = ''.join(random.choices(
            string.ascii_lowercase + string.digits, k=18))

        # Attempt to find a node that responds to ping for PUT
        put_node = None
        for _ in range(len(self.nodes)):
            candidate_node = random.choice(self.nodes)
            print(f"Attempting to ping {candidate_node} for PUT operation.")
            if self.do_ping(candidate_node):
                put_node = candidate_node
                break
            else:
                print(f"Ping failed on {candidate_node}, trying another node.")

        if put_node:
            print(f"Performing PUT on {put_node} with key: {
                  key} and value: {value}")
            self.do_put_storage(put_node, key, value)
        else:
            print("No available nodes responded to ping for PUT operation.")
            return

        # GET from all nodes, even the ones that might have crashed
        for node in self.nodes:
            print(f"Attempting GET on {node} for key: {key}")
            retrieved_value = self.do_get_storage(node, key)
            if retrieved_value == value:
                print(f"GET successful on {node}, value: {retrieved_value}")
            else:
                print(f"GET failed on {node} or value mismatch.")


if __name__ == '__main__':
    unittest.main(argv=sys.argv[:1])
