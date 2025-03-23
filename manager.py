import argparse
import socket
import json
import sys
from time import sleep

KEEP_RUNNING = True


class DHT:
    def __init__(self):
        self.peers = []  # List of all registered peers
        self.dht_peers = []  # Peers currently in the DHT
        self.leader = None
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
                break

        if not leader_peer:
            return False, "Requesting peer not found"

        # Set up DHT
        self.n = size
        self.year = year
        self.leader = leader_peer
        self.initialized = True

        # Select n-1 random peers (simplistic approach: take first n peers including leader)
        # In a real implementation, you would want to randomly select from free peers
        self.dht_peers = [leader_peer]

        count = 1
        for p in self.peers:
            if p["name"] != leader_peer["name"] and count < size:
                self.dht_peers.append(p)
                count += 1
                if count >= size:
                    break

        print(f"DHT setup with size {size} for year {year}, leader: {leader_peer['name']}")
        print(self.dht_peers)
        return True, self.dht_peers


def manager_main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Start a DHT manager")
    parser.add_argument("--port", type=int, default=30000, help="Port for manager communication (default: 30000)")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode with localhost")

    # Parse arguments
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

    while KEEP_RUNNING:
        data, addr = m_socket.recvfrom(1024)
        message = json.loads(data.decode())
        print(message)
        command = message["command"]

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
                        "p_port": p["p_port"]  # Note: using p_port for peer-to-peer communication
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
        print('finish cleaning')
