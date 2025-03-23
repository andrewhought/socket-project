import socket
import json
import sys
import threading
import zlib
from time import sleep

import pandas as pd
import argparse

from peer_utils import send_set_id_command, send_peer_command

MANAGER_PORT: int = 30000
MANAGER_IP_ADDR: str = "127.0.0.1"  # testing at general.asu.edu, this need to be changed
BIG_PRIME = 7017224779

KEEP_RUNNING = True  # Flag to control all the while loop


class Peer:
    def __init__(self, name, ip, m_port, p_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port  # Port for manager communication
        self.p_port = p_port  # Port for peer-to-peer communication
        self.ring_id = None  # ID in the ring (0 for leader)
        self.ring_size = None  # Total size of the ring
        self.right_neighbor_ip = None
        self.right_neighbor_port = None
        self.local_hashtable = {}  # For storing DHT records

    def register(self, m_socket):
        message = {
            "command": "register",
            "peer": {
                "name": self.name,
                "ip": self.ip,
                "m_port": self.m_port,
                "p_port": self.p_port
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

    def construct_local_hashtable(self, year, df=None):
        if self.ring_id == 0 or df is None:
            df = pd.read_csv("storm_data_search_results.csv", parse_dates=["BEGIN_DATE"], dayfirst=False)

            # Ensure BEGIN_DATE is in datetime format
        df["BEGIN_DATE"] = pd.to_datetime(df["BEGIN_DATE"], format="%m/%d/%Y", errors="coerce")

        # Filter rows for the given year
        df_filtered = df[df["BEGIN_DATE"].dt.year == year].copy()  # Copy to avoid modifying original df

        # List to store indices of rows to remove
        rows_to_remove = []

        for index, event in df_filtered.iterrows():
            event_id = int(event["EVENT_ID"])
            pos = hash(event_id) % BIG_PRIME
            id = pos % self.ring_size

            if id == self.ring_id:
                self.local_hashtable[event_id] = event.to_dict()
                rows_to_remove.append(index)  # Mark row for removal

        # Remove processed rows
        df_filtered.drop(rows_to_remove, inplace=True)

        return df_filtered  # Return updated DataFrame

    def setup_dht(self, m_socket):
        size = int(input("Size: "))
        year = int(input("Year: "))
        message = {
            "command": "setup-dht",
            "peer": {
                "name": self.name,
                "ip": self.ip,
                "m_port": self.m_port,
                "p_port": self.p_port
            },
            "size": size,
            "year": year
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        resp_dict = json.loads(response.decode())
        print(resp_dict)

        # If "result" is True, the manager should also provide "ring_peers"
        if resp_dict.get("result"):
            ring_peers = resp_dict["ring_peers"]
            # If I'm the first in ring_peers, I'm the leader
            if ring_peers[0]["name"] == self.name:
                print("I am the leader. Assigning IDs to other peers...")

                # i) set my local ring_id = 0
                self.ring_id = 0
                self.ring_size = len(ring_peers)

                # ii) For i in 1..(n-1), send 'set-id'
                for i in range(1, self.ring_size):
                    target = ring_peers[i]
                    send_set_id_command(
                        target_ip=target["ip"],
                        target_port=target["p_port"],
                        assigned_id=i,
                        ring_size=self.ring_size,
                        all_peers=ring_peers
                    )
                self.right_neighbor_ip = ring_peers[1]["ip"]
                self.right_neighbor_port = ring_peers[1]["p_port"]

                # Now the leader should populate the DHT
                # For milestone: just report the number of records at each node
                sleep(5)  # Wait for other peers to set their IDs
                print(f"sleep for 5 seconds to ensure all peers are set their IDs")
                print(f"Leader would populate DHT with data from year {year}")
                remain_df = self.construct_local_hashtable(year)

                print(f"current length of hash table: {len(self.local_hashtable)}")

                send_peer_command(self.right_neighbor_ip, self.right_neighbor_port, remain_df, year)
                # Send dht-complete to manager
                self.dht_complete(m_socket)
            else:
                print("I am not the leader.")

    def dht_complete(self, m_socket):
        message = {
            "command": "dht-complete",
            "peer": {
                "name": self.name
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))


def listen_for_peers(peer, p_socket):
    p_socket.listen(5)  # Start listening for incoming connections
    print(f"Listening for peer connections on port {peer.p_port}")

    while KEEP_RUNNING:
        try:
            # Accept a connection if it's a new peer connection
            connection, addr = p_socket.accept()
            print(f"Connection established with {addr}")

            # Start a new thread to handle this connection
            handler_thread = threading.Thread(
                target=handle_peer_connection,
                args=(peer, connection, addr),
                daemon=True
            )
            handler_thread.start()

        except Exception as e:
            print(f"Error accepting connection: {e}")

    p_socket.close()
def handle_peer_connection(peer, connection, addr):
    """Handle a single peer connection in its own thread."""
    received_chunks = {}
    buffer = b""

    try:
        while KEEP_RUNNING:
            data = connection.recv(4096)  # Larger buffer for TCP
            if not data:
                break  # Connection was closed by peer

            buffer += data

            # Process complete messages from the buffer
            while buffer:
                try:
                    # Try to parse a complete JSON message
                    message_str = buffer.decode()

                    # Try to find the position of the first valid JSON object
                    brace_count = 0
                    message_end = 0

                    for i, char in enumerate(message_str):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                message_end = i + 1
                                break

                    if message_end == 0:
                        # No complete JSON object found
                        break

                    # Extract the complete JSON message
                    json_message = message_str[:message_end]
                    msg = json.loads(json_message)

                    # Remove the processed message from the buffer
                    buffer = buffer[message_end:]

                    # Process the message based on command
                    if msg.get("command") == "set-id":
                        assigned_id = msg["assigned_id"]
                        ring_size = msg["ring_size"]
                        ring_peers = msg["ring_peers"]
                        peer.ring_id = assigned_id
                        peer.ring_size = ring_size
                        neighbor_index = (assigned_id + 1) % ring_size
                        neighbor = ring_peers[neighbor_index]
                        peer.right_neighbor_ip = neighbor["ip"]
                        peer.right_neighbor_port = neighbor["p_port"]
                        print(f"{peer.name} set-id = {assigned_id}, neighbor is {neighbor}")
                        # Send acknowledgment
                        ack = json.dumps({"status": "id-set-success"}).encode()
                        connection.sendall(ack)

                    elif msg.get("command") == "store":
                        chunk_id = msg["chunk_id"]
                        total_chunks = msg["total_chunks"]
                        compressed_data = bytes.fromhex(msg["data"])
                        received_chunks[chunk_id] = compressed_data

                        if len(received_chunks) == total_chunks:
                            full_data = b''.join(received_chunks[i] for i in sorted(received_chunks.keys()))
                            decompressed_data = zlib.decompress(full_data).decode()
                            df = pd.read_json(decompressed_data)
                            print(f"{peer.name} reconstructed full DataFrame. Length: {len(df)}")

                            df = peer.construct_local_hashtable(msg['year'], df)
                            print(f"current length of hash table: {len(peer.local_hashtable)}")

                            if len(df) > 0:  # Only forward if there's data left to process
                                send_peer_command(peer.right_neighbor_ip, peer.right_neighbor_port, df, msg['year'])
                                print(f"{peer.name} forwarding 'store' to neighbor.")
                            else:
                                print(f"{peer.name} has no more data to forward.")
                            received_chunks.clear()

                            # Send acknowledgment
                            ack = json.dumps({"status": "store-success"}).encode()
                            connection.sendall(ack)

                except json.JSONDecodeError:
                    # If we can't parse the beginning of the buffer, it might be incomplete
                    # Wait for more data
                    if len(buffer) > 100000:  # Safety check to avoid infinite buffer growth
                        print("Buffer too large without valid JSON, clearing buffer.")
                        buffer = b""
                    break
                except UnicodeDecodeError:
                    # Incomplete UTF-8 sequence, wait for more data
                    break
                except Exception as e:
                    print(f"Error processing message: {e}")
                    buffer = b""  # Clear buffer on error
                    break

    except Exception as e:
        print(f"Error in connection handler: {e}")
    finally:
        connection.close()
        print(f"Connection with {addr} closed")

def handle_user_commands(peer, m_socket):
    while KEEP_RUNNING:
        command = input("\nEnter command (register/setup-dht/dht-complete/exit): ").strip()
        if command == "exit":
            break
        elif command == "register":
            peer.register(m_socket)
        elif command == "setup-dht":
            peer.setup_dht(m_socket)
        elif command == "dht-complete":
            peer.dht_complete(m_socket)
        else:
            print("Invalid command")


def peer_main():
    parser = argparse.ArgumentParser(description="Start a peer in the DHT network")
    parser.add_argument("m_port", type=int, help="Port for manager communication")
    parser.add_argument("p_port", type=int, help="Port for peer-to-peer communication")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode with localhost")
    args = parser.parse_args()

    try:
        name = input("Peer name: ")
        ip_addr = "127.0.0.1" if args.debug else socket.gethostbyname(socket.gethostname())

        peer = Peer(name, ip_addr, args.m_port, args.p_port)
        print(f"{peer.name} started")

        # UDP socket for manager communication
        m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        m_socket.bind((ip_addr, args.m_port))

        # TCP socket for peer-to-peer communication
        p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p_socket.bind((ip_addr, args.p_port))

        listen_thread = threading.Thread(target=listen_for_peers, args=(peer, p_socket), daemon=True)
        listen_thread.start()

        handle_user_commands(peer, m_socket)

        print("Shutting down peer.")
        p_socket.close()
        m_socket.close()

    except KeyboardInterrupt:
        print("User signal down the program.")
    finally:
        KEEP_RUNNING = False
        sleep(1)
        print('finish cleaning')


if __name__ == "__main__":
    peer_main()