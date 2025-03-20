from peer import State, Peer

class DHT():
    def __init__(self, leader, peers, initialized, year, n):
        self.leader = leader
        self.peers = peers
        self.initialized = initialized
        self.year = year
        self.n = n

    def not_registered_check(self, peer_name):
        return all(peer.name != peer_name for peer in self.peers)

dht = DHT(None, [], False, None, None)

def register(peer_name, ip, m_port, p_port):
    success = True

    if (len(peer_name) > 15):
        print("FAILURE")
        return not success
    
    for peer in dht.peers:
        if (peer.m_port == m_port or peer.p_port == p_port or peer.name == peer_name):
            print("FAILURE")
            return not success

    peer = Peer(peer_name, State.Free, ip, m_port, p_port)

    dht.peers.append(peer)

    print(f"Successfully registered {peer_name}")
    return success

def setup_dht(peer_name, n, year):
    success = True

    if (dht.not_registered_check(peer_name)):
        print("FAILURE")
        return not success

    if (n < 3 or len(dht.peers) < n or dht.initialized == True):
        print("FAILURE")
        return not success
    
    dht.n = n
    dht.year = year
    
    for peer in dht.peers:
        if (peer.name == peer_name):
            peer.state = State.Leader
            dht.leader = peer

    leader = dht.leader
        
    remaining_peers = [peer for peer in dht.peers if peer.name != leader.name]
    remaining_peers = remaining_peers[:n - 1]

    for peer in remaining_peers:
        peer.state = State.InDHT

    dht.initialized = True

    print(f"Successfully set up DHT with leader {peer_name}")
    return success

# - Receipt of "dht-complete" indicates leader has completed all steps required to set up DHT
# - If peer-name is not the leader, FAILURE response
# - Manager may now process any other commands besides "setup-dht"
def dht_complete(peer_name):
    return