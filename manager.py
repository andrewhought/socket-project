import socket
import json

class DHT:
    def __init__(self):
        self.peers = []
        self.leader = None
        self.initialized = False
        self.n = None
        self.year = None

    def register_peer(self, peer):
        self.peers.insert(0, peer)
        print(f"Registered peer: {peer}")

    def setup_dht(self, peer):
        if (self.n < 3):
            print("FAILURE: DHT needs to have a size of 3 or greater")
            return False
        if (len(self.peers) < self.n):
            print("FAILURE: Not enough peers to setup DHT")
            return False
        self.leader = peer
        self.initialized = True
        print(f"DHT setup completed with leader: {self.leader}")
        return True

def manager():
    dht = DHT()
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_socket.bind(("127.0.0.1", 5000))

    print("Manager started")

    while True:
        data, addr = m_socket.recvfrom(1024)
        message = json.loads(data.decode())
        
        if (message["command"] == "register"):
            dht.register_peer(message["peer"])
            response = {"status": "registered"}
            m_socket.sendto(json.dumps(response).encode(), addr)

        elif (message["command"] == "setup"):
            peer = message["peer"]
            dht.n = message["size"]
            dht.year = message["year"]
            success = dht.setup_dht(peer)
    # If success, gather n 3-tuples: (peerName, ip, port)
    # For example:
    if success:
        # Suppose the FIRST peer in dht.peers is the leader 
        # followed by the next n-1 in the list. (This is simplistic!)
        # Weou might want a better random selection or something that truly picks n distinct peers.
        # For now, let's just take the first n from dht.peers:
        ring_peers = dht.peers[:dht.n]
        peer_list = []
        for p in ring_peers:
            peer_list.append({
                "name": p["name"],
                "ip": p["ip"],
                "port": p["port"]
            })
        response = {
            "status": "setup",
            "result": True,
            "ring_peers": peer_list
        }
    else:
        response = {"status": "setup", "result": False}

    m_socket.sendto(json.dumps(response).encode(), addr)

        elif (message["command"] == "complete"):
            if (addr[1] == dht.leader["port"]):
                print("DHT setup completed by leader")
                response = {"status": "complete"}
                m_socket.sendto(json.dumps(response).encode(), addr)
            else:
                response = {"status": "FAILURE", "message": "Only the leader can complete the DHT"}
                m_socket.sendto(json.dumps(response).encode(), addr)

elif msg["command"] == "store":
    record = msg["record"]
    target_id = msg["target_id"]
    if this_peer.ring_id == target_id:
        # store locally
        print(f"{this_peer.name} storing event_id={record['event_id']} locally!")
    else:
        # forward to next neighbor
        forward_msg = {
            "command": "store",
            "record": record,
            "target_id": target_id
        }
        p_socket.sendto(json.dumps(forward_msg).encode(),
                        (this_peer.right_neighbor_ip, this_peer.right_neighbor_port))
        print(f"{this_peer.name} forwarding 'store' for event {record['event_id']} to ring neighbor.")

if (__name__ == "__main__"):
    manager()
