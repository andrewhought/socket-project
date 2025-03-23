import socket
import json
import pandas as pd
import zlib  # Compression library

MAX_PACKET_SIZE = 65536  # TCP can handle larger packets than UDP


def send_set_id_command(target_ip, target_port, assigned_id, ring_size, all_peers):
    # Create a TCP socket for peer-to-peer communication
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        p_socket.connect((target_ip, int(target_port)))

        message = {
            "command": "set-id",
            "assigned_id": assigned_id,
            "ring_size": ring_size,
            "ring_peers": all_peers
        }

        p_socket.sendall(json.dumps(message).encode())
        print(f"Sent set-id command to peer at {target_ip}:{target_port}, assigned ID={assigned_id}")
    except Exception as e:
        print(f"Error sending set-id command: {e}")
    finally:
        p_socket.close()


def send_peer_command(target_ip, target_port, data, year):
    # Create a TCP socket for peer-to-peer communication
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        p_socket.connect((target_ip, int(target_port)))

        # Convert DataFrame to JSON string
        if isinstance(data, pd.DataFrame):
            data_json = data.to_json(orient="records")
        else:
            data_json = json.dumps(data)

        # Compress the data
        compressed_data = zlib.compress(data_json.encode())

        # Split into chunks if needed for easier processing
        chunks = [compressed_data[i:i + MAX_PACKET_SIZE] for i in range(0, len(compressed_data), MAX_PACKET_SIZE)]

        # Send each chunk
        for i, chunk in enumerate(chunks):
            message = {
                "command": "store",
                "data": chunk.hex(),
                "year": year,
                "chunk_id": i,
                "total_chunks": len(chunks),
            }

            # Send the message
            p_socket.sendall(json.dumps(message).encode())

        print(f"Sent data to peer at {target_ip}:{target_port} in {len(chunks)} chunks")
    except Exception as e:
        print(f"Error sending peer command: {e}")
    finally:
        p_socket.close()