import socket
import json
import sys

class Peer:
    def __init__(self, name, ip, m_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port

    def register(self, m_socket):
        message = {
            "command": "register",
            "peer": {
                "name": self.name,
                "ip": self.ip,
                "port": self.m_port
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (self.ip, 5000))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

    ddef setup_dht(self, m_socket):
    size = int(input("Size: "))
    year = int(input("Year: "))
    message = {
        "command": "setup",
        "peer": {
            "name": self.name,
            "ip": self.ip,
            "port": self.m_port
        },
        "size": size,
        "year": year
    }
    m_socket.sendto(json.dumps(message).encode(), (self.ip, 5000))
    response, _ = m_socket.recvfrom(1024)
    resp_dict = json.loads(response.decode())
    print(resp_dict)

    # If "result" is True, the manager should also provide "ring_peers".
    if resp_dict["result"] == True:
        ring_peers = resp_dict["ring_peers"]
        # If I'm the first in ring_peers, I'm the leader.
        if ring_peers[0]["name"] == self.name:
            print("I am the leader. Assigning IDs to other peers...")

            # i) set my local ring_id = 0
            self.ring_id = 0
            self.ring_size = len(ring_peers)

            # ii) For i in 1..(n-1), send 'set-id'
            for i in range(1, self.ring_size):
                target = ring_peers[i]
                self.send_set_id_command(
                    p_socket=m_socket,
                    target_ip=target["ip"],
                    target_port=target["port"],
                    assigned_id=i,
                    ring_size=self.ring_size,
                    all_peers=ring_peers
                )
        else:
            print("I am not the leader. Waiting for set-id from the leader...")


    def dht_complete(self, m_socket):
        message = {"command": "complete"}
        m_socket.sendto(json.dumps(message).encode(), (self.ip, 5000))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

def leader_populate_dht(self, m_socket):
    """
    Example: The leader reads a CSV file, for each record, decides which ID to send it to.
    For now, we'll just do a dummy example with one event record.
    """
    if self.ring_id != 0:
        print("Error: Only the leader (id=0) should populate the DHT.")
        return

    # TODO: read CSV and figure out total_records, compute prime s, etc.
    # For demonstration, let's assume we have 1 record
    dummy_record = {
        "event_id": 12345,
        "state": "ARIZONA",
        "year": 2000,
        "month_name": "March",
        # etc...
    }

    # Suppose we've computed pos=some_value, id=some_value 
    # For demonstration:
    ring_size = self.ring_size  # from earlier
    assigned_id = 1  # pretend it belongs to peer with ID=1

    if assigned_id == 0:
        # store locally
        print("Storing event_id=12345 locally (leader).")
        # you'd store in a local data structure
    else:
        # send a "store" command to our right neighbor
        store_msg = {
            "command": "store",
            "record": dummy_record,
            "target_id": assigned_id
        }
        # we send to right_neighbor if assigned_id != 0
        # The ring-based approach says you always forward to the next neighbor
        m_socket.sendto(json.dumps(store_msg).encode(), (self.right_neighbor_ip, self.right_neighbor_port))
        print(f"Forwarding store for event_id=12345 to neighbor at {self.right_neighbor_ip}:{self.right_neighbor_port}")

def peer():
    name = input("Peer name: ")
    ip = "127.0.0.1"
    m_port = int(sys.argv[1])
    peer = Peer(name, ip, m_port)

    print(f"{peer.name} started")

    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_socket.bind((ip, m_port))
    
while True:
        # 1. Non-blocking or blocking receive
        #    For example, do a quick input check or use select() to see if there's data
        try:
            p_socket.settimeout(0.5)  # half-second
            data, addr = p_socket.recvfrom(1024)
            if data:
                msg = json.loads(data.decode())
                if msg["command"] == "set-id":
                    assigned_id = msg["assigned_id"]
                    ring_size = msg["ring_size"]
                    ring_peers = msg["ring_peers"]
                    this_peer.ring_id = assigned_id
                    this_peer.ring_size = ring_size

                    # compute your right neighbor from ring_peers
                    # (i+1) mod n
                    neighbor_index = (assigned_id + 1) % ring_size
                    neighbor = ring_peers[neighbor_index]
                    this_peer.right_neighbor_ip = neighbor["ip"]
                    this_peer.right_neighbor_port = neighbor["port"]

                    print(f"{this_peer.name} set-id = {assigned_id}, neighbor is {neighbor}")

                elif msg["command"] == "store": 
                    print("Received store command:", msg["record"])
                    # either store locally or forward to next neighbor
                    # ...
                else:
                    # handle other commands...
                    pass
        except socket.timeout:
            pass

        # 2. Then read user input in the same loop, or do a separate approach:
        command = input("Enter command (register/setup/complete/exit): ").strip()
        if (command == "exit"):
            break
        elif (command == "register"):
            peer.register(m_socket)
        elif (command == "setup"):
            peer.setup_dht(m_socket)
        elif (command == "complete"):
            peer.dht_complete(m_socket)
        else:
            print("Invalid")
    
if (__name__ == "__main__"):
    peer()
