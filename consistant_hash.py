import hashlib


class ConsistentHash:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        self.nodes = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node

    def remove_node(self, node):
        self.nodes.discard(node)
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring.pop(key)

    def get_node(self, key):
        if not self.ring:
            return None

        hash_key = self._hash(key)
        for node_key in sorted(self.ring.keys()):
            if hash_key <= node_key:
                return self.ring[node_key]
        return self.ring[min(self.ring.keys())]

    def _hash(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)


if __name__ == "__main__":
    # create a new instance of ConsistentHash
    from pprint import pprint

    ch = ConsistentHash(nodes=["node1", "node2", "node3"])

    # add a key-value pair to the ring
    key = "key1"
    value = "value1"
    node = ch.get_node(key)
    print(f"Adding key-value pair ({key}, {value}) to node {node}")
    # add the key-value pair to the node using your preferred method
    # for example, if the nodes are Redis instances, you can use the following:
    # redis_instance[node].set(key, value)

    # add another key-value pair
    key2 = "key2"
    value2 = "value2"
    node2 = ch.get_node(key2)
    print(f"Adding key-value pair ({key2}, {value2}) to node {node2}")

    pprint(ch.ring)
