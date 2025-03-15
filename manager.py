# - peer-name is alphabetic string of length no greater than 15 characters
# - On receipt, the manager stores name, IPv4, and ports associated with peer
# - m-port is for communication between peer and manager
# - p-port is for communication between peers
# - Each peer-name can only be registered once
# - IPv4 does not need to be unique for each peer, ports must be unique
def register(peer_name, ip, m_port, p_port):
    return

# - n must be greater than or equal to 3
# - Initiates construction of DHT of size n using data from year YYYY, with peer-name as "Leader"
# - Command results in FAILURE for the following conditionals:
#     - peer-name is not registered
#     - n is not at least 3
#     - Fewer than n users are registered with the manager
#     - DHT has already been set up
# - Manager sets state of peer-name to "Leader" and selects n-1 "Free" users from registered and updates state to "InDHT"
# - The n peers are given by 3-tuples consisting of peer-name, IPv4, and p-port, "Leader" is the first 3-tuple
# - After SUCCESS from "setup-dht," manager waits for "dht-complete"
def setup_dht(peer_name, n, year):
    return

# - Receipt of "dht-complete" indicates leader has completed all steps required to set up DHT
# - If peer-name is not the leader, FAILURE response
# - Manager may now process any other commands besides "setup-dht"
def dht_complete(peer_name):
    return