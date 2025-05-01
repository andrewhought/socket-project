import socket
import json
import threading
import zlib
import random

import pandas as pd
import argparse

from time import sleep, time
from utils import send_peer_set_id_command, send_peer_storm_data, send_peer_tear_down_command

MANAGER_PORT: int = 30000
# TODO: Testing at general.asu.edu
MANAGER_IP_ADDR: str = "127.0.0.1"
BIG_PRIME = 7017224779

# Flag to control while loops
KEEP_RUNNING = True


class Peer:
    def __init__(self, name, ip, m_port, p_port):
        self.name = name
        self.ip = ip
        # Port for manager communication
        self.m_port = m_port
        # Port for peer-to-peer communication
        self.p_port = p_port
        # ID in the ring (0 for leader)
        self.ring_id = None
        # Total size of the ring
        self.ring_size = None
        self.right_neighbor_ip = None
        self.right_neighbor_port = None
        # For storing DHT records. Key is event ID, value is the event data store in storm_data_search_results.csv
        self.local_hashtable = {}
        # Save state of teardown. True if teardown is in progress
        self.is_teardown = False
        # Socket for manager communication
        self.m_socket = None

        # List of peers in the ring, having name and id and port
        self.ring_peers = []

        # Peer that manager returns for query-dht
        self.last_query_dht_peer = None
        # For storing the response of find-event comman
        self.find_event_response = None

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
            df = pd.read_csv("storm_data_search_results.csv", dtype=str,
                             index_col=False)  # Read as string for inspection
            print(len(df), "rows in the original DataFrame")

            # Print first few values of BEGIN_DATE before conversion
            print("BEGIN_DATE before conversion:")
            print(df["BEGIN_DATE"].head(10))

            # Try converting BEGIN_DATE
            df["BEGIN_DATE"] = pd.to_datetime(df["BEGIN_DATE"], errors="coerce")

            # Check for NaT values
            num_nat = df["BEGIN_DATE"].isna().sum()
            print(f"Number of NaT values after conversion: {num_nat}")

            # Print unique years in BEGIN_DATE after conversion
            print("Unique years in BEGIN_DATE:", df["BEGIN_DATE"].dt.year.dropna().unique())

            # Check if the provided year exists in data
            if year not in df["BEGIN_DATE"].dt.year.dropna().unique():
                print(f"Warning: No records found for year {year}")

            # Filter rows for the given year
            df_filtered = df[df["BEGIN_DATE"].dt.year == year].copy()

            # Print number of rows after filtering
            print(f"Filtered {len(df_filtered)} rows for year {year}")

            # Show some filtered data
            print(df_filtered.head(10))  # Copy to avoid modifying original df
        else:
            print(f"df provided, using the one in the class, len = {len(df)}")
            df_filtered = df.copy()
        # List to store indices of rows to remove
        rows_to_remove = []

        for index, event in df_filtered.iterrows():
            event_id = int(event["EVENT_ID"])
            pos = event_id % BIG_PRIME
            id_data = pos % self.ring_size
            if id_data == self.ring_id:
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

        if resp_dict.get("result"):
            ring_peers = resp_dict["ring_peers"]
            self.ring_peers = ring_peers
            if ring_peers[0]["name"] == self.name:
                print("Leader set, assigning IDs to other peers")

                self.ring_id = 0
                self.ring_size = len(ring_peers)

                for i in range(1, self.ring_size):
                    target = ring_peers[i]
                    send_peer_set_id_command(
                        target_ip=target["ip"],
                        target_port=target["p_port"],
                        assigned_id=i,
                        ring_size=self.ring_size,
                        all_peers=ring_peers
                    )
                self.right_neighbor_ip = ring_peers[1]["ip"]
                self.right_neighbor_port = ring_peers[1]["p_port"]

                # Now the leader populates the DHT
                sleep(5)
                print(f"Leader populates DHT with data from year: {year}")
                remain_df = self.construct_local_hashtable(year)

                print(f"current length of hash table: {len(self.local_hashtable)}")

                send_peer_storm_data(self.right_neighbor_ip, self.right_neighbor_port, remain_df, year)
                # Send dht-complete to manager
                self.dht_complete(m_socket)
            else:
                print("Not the leader")

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

    def listen_for_peers(self, p_socket, m_socket):
        p_socket.listen(5)
        print(f"Listening for peer connections on port {self.p_port}")

        while KEEP_RUNNING:
            try:
                # Accept a connection if new peer connection
                connection, addr = p_socket.accept()
                print(f"Connection established with {addr}")

                # Start a new thread to handle this connection
                handler_thread = threading.Thread(
                    target=self.handle_peer_connection,
                    args=(connection, addr, m_socket),
                    daemon=True
                )
                handler_thread.start()

            except Exception as e:
                print(f"Error accepting connection: {e}")

        p_socket.close()

    def handle_peer_connection(self, connection, addr, m_socket):
        """
        Handle a connection with a peer. This function will run in a separate thread.
        m_socket is the manager socket, create for convenience
        :param connection:
        :param addr:
        :param m_socket:
        :return:
        """

        # This part is due to large data sent via TCP
        received_chunks = {}
        buffer = b""

        try:
            while KEEP_RUNNING:
                # Larger buffer for TCP
                data = connection.recv(4096)
                if not data:
                    break

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
                            if char == "{":
                                brace_count += 1
                            elif char == "}":
                                brace_count -= 1
                                if brace_count == 0:
                                    message_end = i + 1
                                    break

                        if message_end == 0:
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
                            self.ring_peers = ring_peers
                            self.ring_id = assigned_id
                            self.ring_size = ring_size
                            neighbor_index = (assigned_id + 1) % ring_size
                            neighbor = ring_peers[neighbor_index]
                            self.right_neighbor_ip = neighbor["ip"]
                            self.right_neighbor_port = neighbor["p_port"]
                            print(f"{self.name} set-id = {assigned_id}, neighbor is {neighbor}")
                            # Send acknowledgment
                            ack = json.dumps({"status": "id-set-success"}).encode()
                            connection.sendall(ack)

                        elif msg.get("command") == "store":
                            chunk_id = msg["chunk_id"]
                            total_chunks = msg["total_chunks"]
                            compressed_data = bytes.fromhex(msg["data"])
                            received_chunks[chunk_id] = compressed_data

                            if len(received_chunks) == total_chunks:
                                full_data = b"".join(received_chunks[i] for i in sorted(received_chunks.keys()))
                                decompressed_data = zlib.decompress(full_data).decode()
                                df = pd.read_json(decompressed_data)
                                print(f"{self.name} reconstructed DataFrame with length: {len(df)}")

                                df = self.construct_local_hashtable(msg["year"], df)
                                print(f"Current length of hash table: {len(self.local_hashtable)}")

                                if len(df) > 0:
                                    send_peer_storm_data(self.right_neighbor_ip, self.right_neighbor_port, df,
                                                         msg["year"])
                                    print(f"{self.name} forwarding store to neighbor")
                                else:
                                    print(f"{self.name} has no more data to forward")
                                received_chunks.clear()

                                # Send acknowledgment
                                ack = json.dumps({"status": "store-success"}).encode()
                                connection.sendall(ack)

                        elif msg.get("command") == "teardown-dht-peer":
                            if self.ring_id != 0:
                                self.teardown_dht()
                            if self.ring_id == 0:
                                print("Leader sent teardown command to all peers")
                                self.teardown_complete(m_socket)

                        elif msg.get("command") == "find-event":
                            event_id = msg["event_id"]
                            sender = msg["sender"]
                            id_seq = msg.get("id_seq", [])

                            print(f'received find-event from {sender["name"]} for event ID: {event_id}')

                            # Calculate position and node ID for this event
                            pos = event_id % BIG_PRIME
                            id_data = pos % self.ring_size

                            # Add current node to the sequence
                            id_seq.append(self.ring_id)

                            # If this is the node that should have the event
                            if id_data == self.ring_id:
                                # Check if event exists in local hash table
                                # print(self.local_hashtable)
                                if event_id in self.local_hashtable.keys():
                                    print(f"Found event {event_id} in local hashtable, sending to {sender['name']}")
                                    # Found the event, send success response directly to sender
                                    # Here fix nan and pd.Timestamp (cannot json serialized)
                                    record_copy = self.local_hashtable[event_id].copy()
                                    if isinstance(record_copy['BEGIN_DATE'],pd.Timestamp):
                                        record_copy['BEGIN_DATE'] = record_copy['BEGIN_DATE'].isoformat()

                                    for key, value in record_copy.items():
                                        if pd.isna(value):
                                            record_copy[key] = None

                                    print(f"data to sent: {self.local_hashtable[event_id]}")
                                    response = {
                                        "command": "find-event-response",
                                        "status": "SUCCESS",
                                        "record": self.local_hashtable[event_id],
                                        "id_seq": id_seq
                                    }
                                    # Sent result to sender
                                    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    try:
                                        sender_socket.connect((sender["ip"], sender["port"]))
                                        print(f"trying to send to {sender['ip']}:{sender['port']}")
                                        sender_socket.sendall(json.dumps(response).encode())
                                    finally:
                                        sender_socket.close()
                                else:
                                    print(f"Event {event_id} not found in local hashtable, forwarding to next peer")
                                    # This node should have it but doesn't, start hot potato routing
                                    self.forward_find_event(event_id, sender, id_seq, id_data)
                            else:
                                # This is not the right node, forward using hot potato routing
                                print(f"Event {event_id} not found in local hashtable, forwarding to next peer")
                                self.forward_find_event(event_id, sender, id_seq, id_data)

                        elif msg.get("command") == "find-event-response":
                            print(f"Received find-event response: {msg['status']}")
                            # Store the response for the waiting find_event_command
                            self.find_event_response = msg

                    except json.JSONDecodeError:
                        # Safety check to avoid infinite buffer growth
                        if len(buffer) > 100000:
                            print("Buffer too large without valid JSON, clearing buffer")
                            buffer = b""
                        break
                    except UnicodeDecodeError:
                        # Incomplete UTF-8 sequence, wait for more data
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        buffer = b""
                        break

        except Exception as e:
            print(f"Error in connection handler: {e}")
        finally:
            connection.close()
            print(f"Connection with {addr} closed")

    def forward_find_event(self, event_id, sender, id_seq, current_id):
        """
        Implement hot potato routing for find-event
        """
        # Create set of all possible IDs except current_id
        all_ids = set(range(self.ring_size))
        remaining_ids = list(all_ids - set(id_seq))

        print(f"Remaining IDs: {remaining_ids}, current ID: {current_id}, sender ID: {sender['name']}")

        # If we've checked all nodes and still haven't found it
        if not remaining_ids:
            # Send failure response directly to the sender
            response = {
                "status": "FAILURE",
                "id_seq": id_seq
            }
            sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sender_socket.connect((sender["ip"], sender["port"]))
                sender_socket.sendall(json.dumps(response).encode())
            finally:
                sender_socket.close()
            return

        next_id = int(random.choice(remaining_ids))

        # Find the peer info with this ID
        next_peer = self.ring_peers[next_id]

        if next_peer is None:
            raise ValueError(f"No peer found with ID {next_id}")

        next_ip = next_peer["ip"]
        next_port = next_peer["p_port"]

        # Forward the find-event message
        forward_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            forward_socket.connect((next_ip, next_port))
            message = {
                "command": "find-event",
                "event_id": event_id,
                "sender": sender,
                "id_seq": id_seq
            }
            forward_socket.sendall(json.dumps(message).encode())
        except Exception as e:
            print(f"Error forwarding find-event: {e}")
        finally:
            forward_socket.close()

    def query_dht(self, m_socket):
        """
        Send query-dht command to manager and get a peer from the DHT
        """
        message = {
            "command": "query-dht",
            "peer": {
                "name": self.name,
                "ip": self.ip,
                "m_port": self.m_port,
                "p_port": self.p_port
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        resp_dict = json.loads(response.decode())
        print(resp_dict)

        if resp_dict.get("status") == "SUCCESS":
            # Save the peer info for find_event_command to use
            self.last_query_dht_peer = resp_dict.get("random_peer")
            return True
        return False

    def find_event_command(self):
        """
        After getting a peer from query_dht, send find-event to that peer
        """
        if not self.last_query_dht_peer:
            print("No peer information available. Run query-dht first.")
            return

        event_id = input("Enter storm event ID to search for: ")
        try:
            event_id = int(event_id)
        except ValueError:
            print("Event ID must be an integer.")
            return

        # Create a TCP socket to connect to the peer
        find_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            target_ip = self.last_query_dht_peer["ip"]
            target_port = self.last_query_dht_peer["p_port"]
            find_socket.connect((target_ip, target_port))

            # Create and send the find-event message
            message = {
                "command": "find-event",
                "event_id": event_id,
                "sender": {
                    "name": self.name,
                    "ip": self.ip,
                    "port": self.p_port
                },
                "id_seq": []  # Initialize empty ID sequence
            }

            find_socket.sendall(json.dumps(message).encode())

            print(f"Sending find-event for event ID {event_id} to {target_ip}:{target_port}")
            find_socket.sendall(json.dumps(message).encode())
            print(f"Find-event message sent, waiting for response...")

            # Set a timeout for the search
            timeout = 10  # seconds
            start_time = time()

            # Create a flag to track if response was received
            self.find_event_response = None

            # Wait for response to arrive through the peer listener
            while time() - start_time < timeout:
                if self.find_event_response:
                    response = self.find_event_response
                    self.find_event_response = None  # Reset for next query

                    if response.get("status") == "SUCCESS":
                        print(f"\nStorm event {event_id} found!")
                        print("\nEvent details:")
                        for field, value in response.get("record", {}).items():
                            print(f"{field}: {value}")
                        print("\nNodes probed:", " --→ ".join(map(str, response.get("id_seq", []))))
                    elif response.get("status") == "FAILURE":
                        print(f"Storm event {event_id} not found in the DHT.")
                    break

                # Sleep
                sleep(0.22)

            if time() - start_time >= timeout:
                print("Timeout waiting for response. No peer responded.")

        except Exception as e:
            print(f"Error find event: {e}")
        finally:
            find_socket.close()

    def handle_user_commands(self, m_socket):
        self.m_socket = m_socket
        while KEEP_RUNNING:
            command = input(
                "\nEnter command (register/setup-dht/dht-complete/leave-dht/join-dht/query-dht/deregiste/exit): ").strip()
            if command == "exit":
                break
            elif command == "register":
                self.register(m_socket)
            elif command == "setup-dht":
                self.setup_dht(m_socket)
            elif command == "dht-complete":
                self.dht_complete(m_socket)
            elif command == "teardown-dht":
                if self.send_manager_teardown(m_socket):
                    self.teardown_dht()
            elif command == "leave-dht":
                self.leave_dht(m_socket)
            elif command == "join-dht":
                self.join_dht(m_socket)
            elif command == "query-dht":
                success = self.query_dht(m_socket)
                if success:
                    self.find_event_command()
                else:
                    print("Query failed")

            elif command == "deregister":
                self.deregister(m_socket)
            elif command == "dht-rebuilt":
                self.dht_rebuilt(m_socket, input("Enter new leader name: "))
            else:
                print("Invalid command")

    def leave_dht(self, m_socket):
        message = {
            "command": "leave-dht",
            "peer": {
                "name": self.name
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

    def join_dht(self, m_socket):
        message = {
            "command": "join-dht",
            "peer": {
                "name": self.name
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

    def dht_rebuilt(self, m_socket, new_leader):
        message = {
            "command": "dht-rebuilt",
            "peer": {
                "name": self.name
            },
            "new_leader": new_leader
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        response, _ = m_socket.recvfrom(1024)
        print(json.loads(response.decode()))

    def deregister(self, m_socket):
        message = {
            "command": "deregister",
            "peer": {
                "name": self.name
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        resp_data, _ = m_socket.recvfrom(1024)
        resp = json.loads(resp_data.decode())
        print("Manager response:", resp)
        # If success, you might close this peer. Up to your design.

    def teardown_dht(self):
        # empty the local hashtable and send teardown command to the right neighbor
        self.local_hashtable = {}
        send_peer_tear_down_command(self.right_neighbor_ip, self.right_neighbor_port)
        print(f"{self.name} sent teardown command to neighbor at {self.right_neighbor_ip}:{self.right_neighbor_port}")

    def send_manager_teardown(self, m_socket):
        message = {
            "command": "teardown-dht",
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
        return json.loads(response.decode())['status'] == "SUCCESS"

    def teardown_complete(self, m_socket):

        message = {
            "command": "teardown-complete",
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

        listen_thread = threading.Thread(target=peer.listen_for_peers, args=(p_socket, m_socket), daemon=True)
        listen_thread.start()

        peer.handle_user_commands(m_socket)

        print("Shutting down peer")
        p_socket.close()
        m_socket.close()

    except KeyboardInterrupt:
        print("Shutting down program")
    finally:
        KEEP_RUNNING = False
        sleep(1)
        print("Finish cleaning")


if __name__ == "__main__":
    peer_main()
