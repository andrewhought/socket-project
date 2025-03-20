from enum import Enum

class State(Enum):
    Free = 1
    Leader = 2
    InDHT = 3

class Peer():
    def __init__(self, name, state, ip, m_port, p_port):
        self.name = name
        self.state = state
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port