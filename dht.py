"""
A distributed hash table (DHT) is a decentralized system for storing and retrieving data in a network.
"""

__AUTHOR__ = "Farshid Ashouri"

import hashlib
import random
import socket
import threading
import time


class DHT:
    def __init__(self, node_id, host="localhost", port=5000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data = {}

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.host, self.port))
        print(f"Node {self.node_id} listening on {self.host}:{self.port}")
        while True:
            data, addr = sock.recvfrom(1024)
            cmd, *args = data.decode().split()
            if cmd == "get":
                key = args[0]
                value = self.get(key)
                sock.sendto(value.encode(), addr)
            elif cmd == "put":
                key, value = args
                self.put(key, value)
            elif cmd == "join":
                peer = (args[0], int(args[1]))
                self.join(peer)
            elif cmd == "ping":
                sock.sendto(b"pong", addr)
            else:
                print(f"Unknown command: {cmd}")

    def get(self, key):
        if key in self.data:
            return self.data[key]
        else:
            return ""

    def put(self, key, value):
        self.data[key] = value

    def join(self, peer):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto("ping".encode(), peer)
        data, addr = sock.recvfrom(1024)
        if data.decode() == "pong":
            print(f"Node {peer[0]}:{peer[1]} is alive")
        else:
            print(f"Node {peer[0]}:{peer[1]} did not respond to ping")
            return

        # Calculate the hash of the peer's address and port
        peer_id = hashlib.sha1(f"{peer[0]}:{peer[1]}".encode()).hexdigest()

        # If the peer's ID is less than ours, it is our predecessor
        if peer_id < self.node_id:
            print(f"Node {peer[0]}:{peer[1]} is our predecessor")
            # Send our data to the peer
            for key, value in self.data.items():
                sock.sendto(f"put {key} {value}".encode(), peer)

        # If the peer's ID is greater than ours, it is our successor
        else:
            print(f"Node {peer[0]}:{peer[1]} is our successor")
            # Request the peer's data
            sock.sendto("get_all".encode(), peer)
            data, addr = sock.recvfrom(1024)
            if data.decode() == "":
                return
            else:
                # Parse the peer's data and add it to our own
                for line in data.decode().split("\n"):
                    key, value = line.split()
                    self.data[key] = value


if __name__ == "__main__":
    # Create 3 nodes with different IDs and ports
    node1 = DHT(hashlib.sha1(b"node1").hexdigest(), port=5000)
    node2 = DHT(hashlib.sha1(b"node2").hexdigest(), port=5001)
    node3 = DHT(hashlib.sha1(b"node3").hexdigest(), port=5002)

    # Start each node in a separate thread
    threads = []
    for node in [node1, node2, node3]:
        t = threading.Thread(target=node.run)
        t.daemon = True
        t.start()
        threads.append(t)

    # Wait a bit for the nodes to start listening
    time.sleep(1)

    # Put some data into the network
    for i in range(10):
        key = f"key{i}"
        value = f"value{i}"
        node = random.choice([node1, node2, node3])  # choose a random node to put the data
        node.put(key, value)
        print(f"Put {key}={value} in node {node.node_id}")

    # Wait a bit for the data to propagate
    time.sleep(1)

    # Get the data back from the network
    for i in range(10):
        key = f"key{i}"
        node = random.choice([node1, node2, node3])  # choose a random node to get the data
        value = node.get(key)
        print(f"Get {key}={value} from node {node.node_id}")
