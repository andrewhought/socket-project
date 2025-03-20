import socket
from enum import Enum

class State(Enum):
    Free = 1
    Leader = 2
    InDHT = 3

class Peer:
    def __init__(self, name, state, ip, m_port, p_port):
        self.name = name
        self.state = state
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port

def send_dht_complete(manager_ip, manager_port, peer_name):
    """ Sends 'dht-complete' message from the DHT leader to the manager """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    message = f"dht-complete {peer_name}"
    
    try:
        sock.sendto(message.encode(), (manager_ip, manager_port))
        response, _ = sock.recvfrom(1024)
        response = response.decode()
        
        if response == "SUCCESS":
            print(f"DHT setup completed successfully by leader {peer_name}.")
        else:
            print(f"DHT completion failed for {peer_name}.")
    
    finally:
        sock.close()

# Test function to validate dht-complete
if __name__ == "__main__":
    manager_ip = "127.0.0.1"  # Localhost for testing
    manager_port = 5000  # Example port
    peer_name = "Leader1"  # Replace with actual leader name
    send_dht_complete(manager_ip, manager_port, peer_name)
