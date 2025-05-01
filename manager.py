import argparse
import socket
import json

from time import sleep

KEEP_RUNNING = True


class DHT:
    def __init__(self):
        # List of all registered peers
        self.peers = []
        # Peers currently in the DHT
        self.dht_peers = []
        self.leader = None  # Leader peer is a dictionary contain field name, ...
        self.initialized = False
        self.n = None
        self.year = None

    def register_peer(self, peer):
        # Check if peer name already exists
        for p in self.peers:
            if p["name"] == peer["name"]:
                return False, "Peer name already registered"

        # Check if ports are already in use by another peer on the same IP
        for p in self.peers:
            if p["ip"] == peer["ip"]:
                if p["m_port"] == peer["m_port"] or p["p_port"] == peer["p_port"]:
                    return False, "Port already in use"

        # If all checks pass, register the peer
        self.peers.append(peer)
        print(f"Registered peer: {peer['name']} at {peer['ip']}:{peer['m_port']}/{peer['p_port']}")
        return True, "SUCCESS"

    def setup_dht(self, peer_name, size, year):
        # Check if DHT already initialized
        if self.initialized:
            return False, "DHT already set up"

        # Check if size is at least 3
        if size < 3:
            return False, "DHT size must be at least 3"

        # Check if enough peers are registered
        if len(self.peers) < size:
            return False, "Not enough peers registered"

        # Find the requesting peer
        leader_peer = None
        for p in self.peers:
            if p["name"] == peer_name:
                leader_peer = p
                print(f"leader_peer: {leader_peer}")
                break

        if not leader_peer:
            return False, "Requesting peer not found"

        # Set up DHT
        self.n = size
        self.year = year
        self.leader = leader_peer
        self.initialized = True

        # Select n-1 random peers
        self.dht_peers = [leader_peer]

        count = 1
        for p in self.peers:
            if p["name"] != leader_peer["name"] and count < size:
                self.dht_peers.append(p)
                count += 1
            if count >= size:
                break
        self.peers = [p for p in self.peers if p not in self.dht_peers]
        print(f"DHT setup with size {size} for year {year}, leader: {leader_peer['name']}")
        print(f"dht peers: {self.dht_peers}")
        print(f"peers: {self.peers}")
        return True, self.dht_peers

    def teardown_dht(self, peer):
        if not self.initialized:
            return False, "DHT not initialized"
        if peer['name'] != self.leader['name']:
            return False, "Only the leader can tear down the DHT"

        return True, "DHT torn down"

    def teardown_complete(self, peer):
        if not self.initialized:
            return False, "DHT not initialized"
        if peer['name'] != self.leader['name']:
            return False, "Only the leader can tear down the DHT"

        for p in self.dht_peers:
            self.peers.append(p)
        self.dht_peers.clear()

        print(f"dht peers: {self.dht_peers}")
        print(f"peers: {self.peers}")

        self.initialized = False
        self.leader = None
        return True, "DHT completely torn"

    def leave_dht(self, peer_name):
        if not self.initialized:
            return False, "DHT does not exist"

        if peer_name not in [peer["name"] for peer in self.dht_peers]:
            return False, "Peer is not part of the DHT"

        print(f"Peer {peer_name} is leaving the DHT")
        # Simulate teardown and renumbering of the DHT
        peer_remove = self.dht_peers.remove(next(p for p in self.dht_peers if p["name"] == peer_name))
        self.n -= 1

        if self.dht_peers:
            new_leader = self.dht_peers[0]
            self.leader = new_leader

        self.peers.append(peer_remove)

        return True, f"{peer_name} successfully left the DHT. New leader is {self.leader['name'] if self.leader else 'None'}"

    def join_dht(self, peer_name):
        if not self.initialized:
            return False, "DHT does not exist"

        # Check if the peer is free
        if peer_name in [peer["name"] for peer in self.dht_peers]:
            return False, "Peer is already in the DHT"

        # Add the peer to DHT
        new_peer = {"name": peer_name}
        self.dht_peers.append(new_peer)
        self.n += 1

        return True, self.leader["name"]

    def dht_rebuilt(self, peer_name, new_leader):
        if peer_name not in [peer["name"] for peer in self.dht_peers]:
            return False, "Peer not recognized in the DHT"

        # Update the leader if necessary
        self.leader = next(p for p in self.dht_peers if p["name"] == new_leader)

        print(f"Peer {peer_name} has rebuilt the DHT. New leader is {self.leader['name']}")
        print(f"current free peers: {self.peers}")
        print(f"current dht peers: {self.dht_peers}")

        return True, "SUCCESS"

    def query_dht(self, peer_name):
        if not self.initialized:
            return False, "DHT is not initialized or not completed"
        
        requesting_peer = None

        for p in self.peers:
            if p["name"] == peer_name:
                requesting_peer = p
                break
        if not requesting_peer:
            return False, "Peer not registered"

        if not self.dht_peers:
            return False, "No peers in DHT"

        import random
        random_peer = random.choice(self.dht_peers)  # Return the random peer's 3-tuple
        return True, {
            "name": random_peer["name"],
            "ip": random_peer["ip"],
            "p_port": random_peer["p_port"]
        }

    def deregister_peer(self, peer_name):
        dereg_peer = None  # Find the peer in self.peers
        for p in self.peers:
            if p["name"] == peer_name:
                dereg_peer = p
                break
        if not dereg_peer:
            return False, "Peer not registered"

        if dereg_peer in self.dht_peers:  # Check if that peer is in dht_peers or is leader
            return False, "Peer is currently in the DHT"

        if self.leader and self.leader["name"] == peer_name:
            return False, "Cannot deregister the leader"

        self.peers.remove(dereg_peer)  # Remove from self.peers
        print(f"Peer {peer_name} deregistered successfully.")
        return True, "SUCCESS"


def manager_main():
    parser = argparse.ArgumentParser(description="Start a DHT manager")
    parser.add_argument("--port", type=int, default=30000, help="Port for manager communication (default: 30000)")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode with localhost")

    args = parser.parse_args()

    dht = DHT()
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    if args.debug:
        ip_addr = "127.0.0.1"
    else:
        ip_addr = socket.gethostbyname(socket.gethostname())

    port = args.port
    m_socket.bind((ip_addr, port))

    print(f"Manager started at {ip_addr}:{port}")

    receive_msg = True

    while KEEP_RUNNING:
        data, addr = m_socket.recvfrom(1024)
        message = json.loads(data.decode())
        print(message)
        command = message["command"]

        if not receive_msg:
            if command != "dht-rebuilt":
                print("not receiving messages anymore except dht-rebuilt")
                continue
            else:
                peer_name = message["peer"]["name"]
                new_leader = message["new_leader"]
                success, result = dht.dht_rebuilt(peer_name, new_leader)
                response = {"status": "SUCCESS" if success else "FAILURE", "message": result}
                m_socket.sendto(json.dumps(response).encode(), addr)
                receive_msg = True
                print("receiving messages again except dht-rebuilt")

        if command == "register":
            peer = message["peer"]
            success, msg = dht.register_peer(peer)
            if success:
                response = {"status": "SUCCESS", "message": msg}
            else:
                response = {"status": "FAILURE", "message": msg}
            m_socket.sendto(json.dumps(response).encode(), addr)

        elif command == "setup-dht":
            peer = message["peer"]
            size = message["size"]
            year = message["year"]

            success, result = dht.setup_dht(peer["name"], size, year)

            if success:
                # Convert DHT peers to the format expected by client
                ring_peers = []
                for p in result:
                    ring_peers.append({
                        "name": p["name"],
                        "ip": p["ip"],
                        "p_port": p["p_port"]
                    })

                response = {
                    "status": "SUCCESS",
                    "result": True,
                    "ring_peers": ring_peers
                }
            else:
                response = {
                    "status": "FAILURE",
                    "result": False,
                    "message": result
                }

            m_socket.sendto(json.dumps(response).encode(), addr)

        elif command == "dht-complete":
            # Check if the request is from the leader
            if dht.leader and addr[0] == dht.leader["ip"] and addr[1] == dht.leader["m_port"]:
                print("DHT setup completed by leader")
                dht.setup_complete = True
                response = {"status": "SUCCESS"}
            else:
                response = {"status": "FAILURE", "message": "Only the leader can complete the DHT"}

            m_socket.sendto(json.dumps(response).encode(), addr)

        elif command == "leave-dht":
            peer_name = message["peer"]["name"]
            success, result = dht.leave_dht(peer_name)
            response = {"status": "SUCCESS" if success else "FAILURE", "message": result}
            m_socket.sendto(json.dumps(response).encode(), addr)
            receive_msg = False
            print("not receiving messages anymore except dht-rebuilt")

        elif command == "join-dht":
            peer_name = message["peer"]["name"]
            success, result = dht.join_dht(peer_name)
            response = {"status": "SUCCESS" if success else "FAILURE", "message": result}
            m_socket.sendto(json.dumps(response).encode(), addr)
            receive_msg = False
            print("not receiving messages anymore except dht-rebuilt")

        elif command == "query-dht":
            peer_name = message["peer"]["name"]
            ok, res = dht.query_dht(peer_name)
            if ok:
                response = {
                    "status": "SUCCESS",
                    "random_peer": res
                }
            else:
                response = {
                    "status": "FAILURE",
                    "message": res
                }
            m_socket.sendto(json.dumps(response).encode(), addr)

        elif command == "deregister":
            peer_name = message["peer"]["name"]
            ok, res = dht.deregister_peer(peer_name)
            if ok:
                response = {"status": "SUCCESS", "message": res}
            else:
                response = {"status": "FAILURE", "message": res}
            m_socket.sendto(json.dumps(response).encode(), addr)

        elif command == "teardown-dht":
            peer = message["peer"]
            success, msg = dht.teardown_dht(peer)
            if success:
                response = {"status": "SUCCESS", "message": msg}
            else:
                response = {"status": "FAILURE", "message": msg}
            m_socket.sendto(json.dumps(response).encode(), addr)
        elif command == "teardown-complete":
            peer = message["peer"]
            success, msg = dht.teardown_complete(peer)
            if success:
                response = {"status": "SUCCESS", "message": msg}
            else:
                response = {"status": "FAILURE", "message": msg}
            m_socket.sendto(json.dumps(response).encode(), addr)
        else:
            response = {"status": "FAILURE", "message": "Unknown command"}
            m_socket.sendto(json.dumps(response).encode(), addr)


if __name__ == "__main__":
    try:
        manager_main()
    except KeyboardInterrupt:
        print("Shutting down manager")
    finally:
        KEEP_RUNNING = False
        sleep(1)
        print("Finish cleaning")
