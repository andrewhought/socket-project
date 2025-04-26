from __future__ import annotations
import socket
import json
import threading
import random
import zlib
import pandas as pd
import argparse
from time import sleep
from utils import send_set_id_command, send_peer_command
from io import StringIO
from time import sleep
from typing import Any, Dict, List, Optional
from utils import send_peer_command, send_set_id_command

MANAGER_PORT: int = 30000
# TODO: Testing at general.asu.edu
MANAGER_IP_ADDR: str = "127.0.0.1"
BIG_PRIME = 7017224779
# Flag to control while loops
KEEP_RUNNING = True

def next_prime(n):
    def is_prime(num):
        if num < 2:
            return False
        for i in range(2, int(num ** 0.5) + 1):
            if num % i == 0:
                return False
        return True
    num = n + 1
    while not is_prime(num):
        num += 1
    return num

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
        # Ring info filled in later
        self.ring_id: Optional[int] = None
        self.ring_size: Optional[int] = None
        self.ring_peers: List[Dict[str, Any]] = []
        self.right_neighbor_ip: Optional[str] = None
        self.right_neighbor_port: Optional[int] = None

        # Local hash‑table {event_id: record‑dict}
        self.local_hashtable: Dict[int, Dict[str, Any]] = {}
        self.s: Optional[int] = None  # hash‑table size (prime > 2×events)

        # Book‑keeping for find‑event
        self.last_query_dht_peer: Optional[Dict[str, Any]] = None

    def register(self, m_socket):
        msg = {
            "command": "register",
            "peer": {
                "name": self.name,
                "ip": self.ip,
                "m_port": self.m_port,
                "p_port": self.p_port
            }
        }
        m_socket.sendto(json.dumps(msg).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        print(json.loads(m_socket.recvfrom(1024)[0].decode()))

    def construct_local_hashtable(self, year, df=None):
        record = self.local_hashtable.get(event_id) if self.local_hashtable else None
        if df is None:  # leader call
            df = pd.read_csv("storm_data_search_results.csv", na_values=["", " "])
            df["BEGIN_DATE"] = pd.to_datetime(df["BEGIN_DATE"], format="%m/%d/%Y", errors="coerce")
            df = df[df["BEGIN_DATE"].dt.year == year].copy()
            print(f"{self.name} loaded {len(df)} events for {year}")
            self.s = next_prime(2 * len(df))
            df["_s"] = self.s  # stash so followers know *s*
        else:  # follower side – s was embedded in received df
            self.s = int(df["_s"].iloc[0])

        remaining: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            event_id = int(row["EVENT_ID"])
            pos = event_id % self.s
            target_id = pos % self.ring_size  # type: ignore[arg-type]
            if target_id == self.ring_id:
                self.local_hashtable[event_id] = row.drop("_s").to_dict()
            else:
                remaining.append(row.to_dict())

        print(f"{self.name} stored {len(self.local_hashtable)} events")
        print(f"{self.name} first 5 ids: {list(self.local_hashtable.keys())[:5]}")
        return pd.DataFrame(remaining)
                
    def hash_event_id(event_id, size):
        return event_id % size  # Example hash function

    def listen_for_peers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.ip, self.p_port))
            sock.listen()
            print(f"{self.name} listening for peer connections on port {self.p_port}")
            while KEEP_RUNNING:
                try:
                    conn, addr = sock.accept()
                    print(f"{self.name} connection established with {addr}")
                    threading.Thread(target=self.handle_connection, args=(conn, addr)).start()
                except Exception as e:
                    print(f"{self.name} error accepting connection: {e}")
    
    def process_received_data(data):
        try:
            decompressed_data = decompress_data(data)  # Custom decompress function
            if decompressed_data:
                df = pd.read_json(StringIO(decompressed_data))
                print(f"{peer_name} reconstructed DataFrame with length: {len(df)}")
                store_events(df)
            else:
                print(f"{peer_name} received empty data")
        except Exception as e:
            print(f"Error processing message: {e}")

    # Include find_event_command, handle_find_event, etc., from previous responses
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
            if ring_peers[0]["name"] == self.name:
                print("Leader set, assigning IDs to other peers")
                self.ring_id = 0
                self.ring_size = len(ring_peers)
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
                # Now the leader populates the DHT
                sleep(5)
                print(f"Leader populates DHT with data from year: {year}")
                remain_df = self.construct_local_hashtable(year)
                print(f"current length of hash table: {len(self.local_hashtable)}")
                send_peer_command(self.right_neighbor_ip, self.right_neighbor_port, remain_df, year)
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

    def listen_for_peers(self, p_socket):
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
                    args=(connection, addr),
                    daemon=True
                )
                handler_thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")
        p_socket.close()

    def handle_peer_connection(self, connection, addr):
        received_chunks: dict[int, bytes] = {}
        buffer = b""
        try:
            while KEEP_RUNNING:
                data = connection.recv(4096)
                if not data:
                    break
                buffer += data
                while buffer:
                    try:
                        text = buffer.decode()
                        brace = depth = end = 0
                        for i, ch in enumerate(text):
                            if ch == "{":
                                depth += 1
                                brace = 1
                            elif ch == "}":
                                depth -= 1
                                if depth == 0 and brace:
                                    end = i + 1
                                    break
                        if end == 0:
                            break
                        msg = json.loads(text[:end])
                        buffer = buffer[end:]  # remove processed bytes
                    except (UnicodeDecodeError, json.JSONDecodeError):  
                        break
                    cmd = msg.get("command")
                    if cmd == "set-id":
                        self.handle_set_id(msg, connection)
                    elif cmd == "store":
                        cid = msg["chunk_id"]
                        total = msg["total_chunks"]
                        received_chunks[cid] = bytes.fromhex(msg["data"])
                        if len(received_chunks) == total:
                            full = b"".join(received_chunks[i]
                                            for i in sorted(received_chunks))
                            try:
                                df_str = zlib.decompress(full).decode()
                                if not df_str.strip():
                                    connection.sendall(
                                        json.dumps({"status": "store-failure",
                                                    "reason": "empty payload"}).encode())
                                    received_chunks.clear()
                                    continue
                                df = pd.read_json(StringIO(df_str))
                                print(f"{self.name} reconstructed DF len={len(df)}")
                                remain = self.construct_local_hashtable(msg["year"], df)
                                print(f"Current hash‑table size: {len(self.local_hashtable)}")
                                if remain is not None and not remain.empty:
                                    send_peer_command(self.right_neighbor_ip,
                                                        self.right_neighbor_port,
                                                        remain, msg["year"])
                                    print(f"{self.name} forwarded {len(remain)} rows")
                                else:
                                    print(f"{self.name} nothing to forward")
                                connection.sendall(
                                    json.dumps({"status": "store-success"}).encode())
                                received_chunks.clear()
                            except Exception as exc:
                                print(f"{self.name} store error: {exc}")
                                connection.sendall(
                                    json.dumps({"status": "store-failure",
                                                "reason": str(exc)}).encode())
                                received_chunks.clear()
                    elif cmd == "find-event":
                        self.handle_find_event(msg)
                    elif cmd == "find-event-result":
                        self.handle_find_event_result(msg)
                    else:
                        print(f"{self.name} unknown command: {cmd}")
        except Exception as exc:
            print(f"{self.name} handler error from {addr}: {exc}")
        finally:
            connection.close()
            print(f"Connection with {addr} closed")

    def handle_set_id(self, msg: Dict[str, Any], conn: socket.socket) -> None:
        self.ring_peers = msg["ring_peers"]
        self.ring_id = msg["assigned_id"]
        self.ring_size = msg["ring_size"]
        nbr = self.ring_peers[(self.ring_id + 1) % self.ring_size]
        self.right_neighbor_ip, self.right_neighbor_port = nbr["ip"], nbr["p_port"]
        print(f"{self.name}: set‑id={self.ring_id}, right={nbr['name']}")
        conn.sendall(json.dumps({"status": "id-set-success"}).encode())

    def handle_store(self, msg: Dict[str, Any], conn: socket.socket, received_chunks: Dict[int, bytes]) -> None:
        cid, total = msg["chunk_id"], msg["total_chunks"]
        received_chunks[cid] = bytes.fromhex(msg["data"])
        if len(received_chunks) < total:
            return  # wait for more
        data = b"".join(received_chunks[i] for i in sorted(received_chunks))
        df = pd.read_json(StringIO(zlib.decompress(data).decode()))
        year = msg["year"]
        remaining = self.construct_local_hashtable(year, df)
        if not remaining.empty and self.right_neighbor_ip:
            send_peer_command(self.right_neighbor_ip, self.right_neighbor_port, remaining, year)
            print(f"{self.name} forwarded {len(remaining)} rows to neighbor")
        conn.sendall(json.dumps({"status": "store-success"}).encode())
        received_chunks.clear()

    def handle_find_event(self, msg: Dict[str, Any]) -> None:
        sender = msg["original_sender"]
        event_id = msg["event_id"]
        correct_id = msg["correct_id"]
        I: List[int] = msg["I"]
        id_seq: List[int] = msg["id_seq"] + [self.ring_id]

        # If we are the target id
        if self.ring_id == correct_id: 
            record = self.local_hashtable.get(event_id)
            if record:
                self.send_find_event_result(sender, True, event_id, id_seq, record)
            else:
                self.send_find_event_result(sender, False, event_id, id_seq,
                                             reason=f"Event {event_id} not in local table")
            return
            
        if not I:   # Not target – forward hot‑potato
            self.send_find_event_result(sender, False, event_id, id_seq, reason="I empty")
            return
        next_id = random.choice(I)
        I.remove(next_id)
        nxt = self.ring_peers[next_id]
        msg.update({"I": I, "id_seq": id_seq})
        self._send_tcp(nxt["ip"], nxt["p_port"], msg)

    def send_response(sender, response):
        # Send response back to sender (name, ip, p_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((sender['ip'], sender['p_port']))
            s.sendall(json.dumps(response).encode())
    
    def process_message(self, data):
        try:
            if not data:
                print(f"{self.name} received empty message")
                return
            msg = json.loads(data.decode())
            if msg["command"] == "find-event":
                self.handle_find_event(msg)
            elif msg["command"] == "find-event-result":
                self.handle_find_event_result(msg)
            else:
                print(f"{self.name} received unknown command: {msg['command']}")
        except json.JSONDecodeError as e:
            print(f"{self.name} error decoding message: {e}")
        except Exception as e:
            print(f"{self.name} error processing message: {e}")

    def send_find_event_result(self, sender: Dict[str, Any], success: bool, event_id: int,
                               id_seq: List[int], record: Optional[Dict[str, Any]] = None,
                               reason: str = "") -> None:
        res = {
            "command": "find-event-result",
            "success": success,
            "event_id": event_id,
            "id_seq": id_seq,
        }
        self._send_tcp(sender["ip"], sender["p_port"], res)
        if success:
            res["record"] = record
        else:
            res["reason"] = reason

    def handle_find_event_result(self, msg: Dict[str, Any]) -> None:
        eid = msg["event_id"]
        id_seq = msg["id_seq"]
        if msg["success"]:
            print(f"\n[find-event-result] SUCCESS: Found event {eid}")
            for k, v in msg["record"].items():
                print(f"{k}: {v}")
            print("id-seq:", id_seq)
        else:
            print(f"\n[find-event-result] FAILURE: {msg.get('reason','')}")
            print("id-seq:", id_seq)
    
    def _send_tcp(self, ip: str, port: int, message: Dict[str, Any]) -> None:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.sendall(json.dumps(message).encode())
            print(f"{self.name} sent {message['command']} to {ip}:{port}")
        except Exception as e:
            print(f"{self.name} TCP send error: {e}")
    
    def handle_find_event_response(response):
        if response['status'] == 'SUCCESS':
            print(f"[find-event-result] SUCCESS: Found event {response['event_id']}")
            for key, value in response['record'].items():
                print(f"{key}: {value}")
            print(f"id-seq: {response['id_seq']}")
        else:
            print(f"[find-event-result] FAILURE: {response['message']}")
            print(f"id-seq: {response['id_seq']}")

    def process_query_dht(peer_name, p_port, event_id):
        # Get random peer from manager
        manager_response = query_manager_for_random_peer()
        random_peer = manager_response['random_peer']
        print(f"{peer_name} sending find-event {event_id} to {random_peer['name']}")
        # Send find-event query
        message = {
            'command': 'find-event',
            'event_id': event_id,
            'sender': {'name': peer_name, 'ip': '127.0.0.1', 'p_port': p_port},
            'id_seq': []
        }
        send_to_peer(random_peer, message)
        # Listen for response
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', p_port))
            s.listen()
            conn, addr = s.accept()
            with conn:
                data = conn.recv(4096).decode()
                response = json.loads(data)
                handle_find_event_response(response)

    def handle_user_commands(self, m_socket):
        while KEEP_RUNNING:
            command = input("\nEnter command (register/setup-dht/dht-complete/leave-dht/join-dht/query-dht/deregiste/exit): ").strip()
            if command == "exit":
                break
            elif command == "register":
                self.register(m_socket)
            elif command == "setup-dht":
                self.setup_dht(m_socket)
            elif command == "dht-complete":
                self.dht_complete(m_socket)
            elif command == "leave-dht":
                self.leave_dht(m_socket)
            elif command == "join-dht":
                self.join_dht(m_socket)
            elif command == "query-dht":
                self.query_dht(m_socket)
            elif command == "deregister":
                self.deregister(m_socket)
            elif command == "find-event":
                self.find_event_command()
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

    def query_dht(self, m_socket):
        message = {
            "command": "query-dht",
            "peer": {
                "name": self.name
            }
        }
        m_socket.sendto(json.dumps(message).encode(), (MANAGER_IP_ADDR, MANAGER_PORT))
        resp_data, _ = m_socket.recvfrom(1024)
        resp = json.loads(resp_data.decode())
        print("Manager response:", resp)

        if resp["status"] == "SUCCESS":
            rp = resp["random_peer"]
            self.last_query_dht_peer = rp
            print(f"Random DHT peer => {rp}")
        else:
            print(f"Query-DHT failed: {resp.get('message', '')}")

    def find_event_command(self):
        if not self.last_query_dht_peer:
            print("No random peer found. Please do 'query-dht' first.")
            return
        if self.s is None or self.ring_size is None:
            print("DHT not initialized. Please run 'setup-dht' first.")
            return
        try:
            event_id = int(input("Enter event_id to search: "))
        except ValueError:
            print("Invalid event_id. Must be an integer.")
            return
        pos = event_id % self.s
        correct_id = pos % self.ring_size
        all_ids = set(range(self.ring_size))
        all_ids.discard(correct_id)
        I = list(all_ids)
        msg = {
            "command": "find-event",
            "original_sender": {
                "ip": self.ip,
                "p_port": self.p_port,
                "name": self.name
            },
            "event_id": event_id,
            "pos": pos,
            "correct_id": correct_id,
            "I": I,
            "id_seq": []
        }
        print(f"{self.name} sending find-event {event_id} to {self.last_query_dht_peer['name']} (pos={pos}, correct_id={correct_id})")
        self.send_hot_potato_message(self.last_query_dht_peer["ip"], self.last_query_dht_peer["p_port"], msg)

    def send_hot_potato_message(self, ip, port, message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)  # Set 5-second timeout
            sock.connect((ip, port))
            encoded_message = json.dumps(message).encode('utf-8')
            sock.sendall(encoded_message)
            print(f"{self.name} sent message to {ip}:{port}: {message['command']}")
        except socket.timeout:
            print(f"{self.name} timeout sending message to {ip}:{port}")
        except ConnectionRefusedError:
            print(f"{self.name} connection refused by {ip}:{port}")
        except json.JSONEncodeError as e:
            print(f"{self.name} error encoding message: {e}")
        except Exception as e:
            print(f"{self.name} error sending message to {ip}:{port}: {e}")
        finally:
            try:
                sock.close()
            except:
                pass  # Ignore errors during socket closure

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
        listen_thread = threading.Thread(target=peer.listen_for_peers, args=(p_socket,), daemon=True)
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