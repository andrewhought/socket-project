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

    def setup_dht(self, m_socket):
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
        print(json.loads(response.decode()))

    def dht_complete(self, m_socket):
        message = {"command": "complete"}
        m_socket.sendto(json.dumps(message).encode(), (self.ip, 5000))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

def peer():
    name = input("Peer name: ")
    ip = "127.0.0.1"
    m_port = int(sys.argv[1])
    peer = Peer(name, ip, m_port)

    print(f"{peer.name} started")

    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_socket.bind((ip, m_port))

    while True:
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
