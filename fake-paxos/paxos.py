#!/usr/bin/env python3
import sys
import socket
import struct
import time
from collections import defaultdict
import threading
import json
import math

# Message class with JSON encoding/decoding
class Message:

    PHASE1A = 'Phase1A'
    PHASE1B = 'Phase1B'
    PHASE2A = 'Phase2A'
    PHASE2B = 'Phase2B'
    CLIENT = 'Client'
    DECIDE = 'Decide'

    @staticmethod
    def phase_client(value, phase):
        msg = {
            'value': value,
            'phase': phase
        }
        return json.dumps(msg)

    @staticmethod
    def phase_1a(c_rnd, phase, key):
        msg = {
            'c_rnd_1': c_rnd[0],
            'c_rnd_2': c_rnd[1],
            'phase': phase,
            'key_1': key[0],
            'key_2': key[1]
        }
        return json.dumps(msg)

    @staticmethod
    def phase_1b(rnd, v_rnd, v_val, phase, key):
        msg = {
            'rnd_1': rnd[0],
            'rnd_2': rnd[1],
            'v_rnd_0': v_rnd[0],
            'v_rnd_1': v_rnd[1],
            'v_val': v_val,
            'phase': phase,
            'key_1': key[0],
            'key_2': key[1]
        }
        return json.dumps(msg)

    @staticmethod
    def phase_2a(c_rnd, c_val, phase, key):
        msg = {
            'c_rnd_1': c_rnd[0],
            'c_rnd_2': c_rnd[1],
            'c_val': c_val,
            'phase': phase,
            'key_1': key[0],
            'key_2': key[1]
        }
        return json.dumps(msg)

    @staticmethod
    def phase_2b(v_rnd, v_val, phase, key):
        msg = {
            'v_rnd_1': v_rnd[0],
            'v_rnd_2': v_rnd[1],
            'v_val': v_val,
            'phase': phase,
            'key_1': key[0],
            'key_2': key[1]
        }
        return json.dumps(msg)

    @staticmethod
    def decision(v_val, phase, key):
        msg = {
            'v_val': v_val,
            'phase': phase,
            'key_1': key[0],
            'key_2': key[1]
        }
        return json.dumps(msg)

    @staticmethod
    def handle_proposer_received_message(data):
        try:
            msg = json.loads(data)
            if 'phase' in msg:
                if msg['phase'] == Message.PHASE2B:
                    print("Received Phase2B message")
                    return Message.PHASE2B, msg
                elif msg['phase'] == Message.CLIENT:
                    print("Received client message")
                    return Message.CLIENT, msg
                elif msg['phase'] == Message.PHASE1B:
                    print("Received Phase1B message")
                    return Message.PHASE1B, msg
        except json.JSONDecodeError:
            pass
        return "Unknown", None

    @staticmethod
    def handle_acceptor_received_message(data):
        try:
            msg = json.loads(data)
            if 'c_rnd_1' in msg and 'c_rnd_2' in msg and 'c_val' in msg:
                print("Received Phase2A message")
                return Message.PHASE2A, msg
            elif 'c_rnd_1' in msg and 'c_rnd_2' in msg:
                print("Received Phase1A message")
                return Message.PHASE1A, msg
        except json.JSONDecodeError as e:
            print(f"Failed to parse message: {e}")
        
        return "Unknown", None

# ----------------------------------------------------

def mcast_receiver(hostport):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock

def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock

def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg

# ----------------------------------------------------
lock = threading.Lock()
active_acceptors = set()

def acceptor(config, id):
    print(f"-> acceptor {id} starting")
    with lock:
        active_acceptors.add(id)
    state = {}
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    
    try:
        while True:
            msg = r.recv(2**16)
            phase, data = Message.handle_acceptor_received_message(msg)
            
            if phase == Message.PHASE1A:
                key = (data['key_1'], data['key_2'])
                if key not in state:
                    state[key] = {"rnd": (0, 0), "v_rnd": (0,0), "v_val": None}
                
                c_rnd = (data['c_rnd_1'], data['c_rnd_2'])
                if c_rnd > state[key]['rnd']:
                    state[key]['rnd'] = c_rnd
                    phase1b_msg = Message.phase_1b(
                        state[key]['rnd'],
                        state[key]['v_rnd'],
                        state[key]['v_val'],
                        Message.PHASE1B,
                        key
                    )
                    s.sendto(phase1b_msg.encode(), config["proposers"])
            
            elif phase == Message.PHASE2A:
                key = (data['key_1'], data['key_2'])
                if key not in state:
                    state[key] = {"rnd": (0, 0), "v_rnd": (0,0), "v_val": None}
                
                c_rnd = (data['c_rnd_1'], data['c_rnd_2'])
                if c_rnd >= state[key]['rnd']:
                    state[key]['rnd'] = c_rnd
                    state[key]['v_rnd'] = c_rnd
                    state[key]['v_val'] = data['c_val']
                    
                    # Send Phase2B directly to learners only
                    phase2b_msg = Message.phase_2b(
                        state[key]['v_rnd'],
                        state[key]['v_val'],
                        Message.PHASE2B,
                        key
                    )
                    s.sendto(phase2b_msg.encode(), config["learners"])
    finally:
        with lock:
            active_acceptors.remove(id)

def proposer(config, id):
    print(f"-> proposer {id} starting")
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = defaultdict(list)
    pending = {}
    
    while True:
        msg = r.recv(2**16)
        phase, data = Message.handle_proposer_received_message(msg)
        
        if phase == Message.CLIENT:
            timestamp = int(time.time() * 1000)
            key = (id, timestamp)
            c_rnd_cnt = (c_rnd_cnt[0] + 1, id)
            c_rnd[key] = c_rnd_cnt
            c_val[key] = data['value']
            pending[key] = {
                'value': data['value'],
                'timestamp': time.time()
            }
            
            phase1a_msg = Message.phase_1a(c_rnd[key], Message.PHASE1A, key)
            s.sendto(phase1a_msg.encode(), config["acceptors"])
        
        elif phase == Message.PHASE1B:
            key = (data['key_1'], data['key_2'])
            if key not in c_rnd or (data['rnd_1'], data['rnd_2']) != c_rnd[key]:
                continue
                
            v_rnd = (data['v_rnd_0'], data['v_rnd_1'])
            promises[key].append((v_rnd, data['v_val']))
            
            if len(promises[key]) > len(active_acceptors) / 2:
                highest_v_rnd = max((p[0] for p in promises[key]), default=(0,0))
                highest_vals = [p[1] for p in promises[key] if p[0] == highest_v_rnd]
                
                if key not in pending:
                    promises[key] = []
                    continue
                
                if highest_v_rnd == (0,0) or all(v is None for v in highest_vals):
                    c_val[key] = pending[key]['value']
                else:
                    c_val[key] = next(v for v in highest_vals if v is not None)
                
                phase2a_msg = Message.phase_2a(c_rnd[key], c_val[key], Message.PHASE2A, key)
                s.sendto(phase2a_msg.encode(), config["acceptors"])
                promises[key] = []
        
        # Retry mechanism
        current_time = time.time()
        for key in list(pending.keys()):
            if current_time - pending[key]['timestamp'] > 5:
                c_rnd_cnt = (c_rnd_cnt[0] + 1, id)
                c_rnd[key] = c_rnd_cnt
                pending[key]['timestamp'] = current_time
                phase1a_msg = Message.phase_1a(c_rnd[key], Message.PHASE1A, key)
                s.sendto(phase1a_msg.encode(), config["acceptors"])
                
                
def learner(config, id):
    print(f"-> learner {id} starting")
    r = mcast_receiver(config["learners"])
    learned_values = []
    learned_keys = set()
    phase2b_responses = defaultdict(lambda: defaultdict(set))  # Track 2B responses
    
    while True:
        msg = r.recv(2**16)
        data = json.loads(msg)
        key = (data['key_1'], data['key_2'])
        
        if data['phase'] == Message.PHASE2B:  
            v_rnd = (data['v_rnd_1'], data['v_rnd_2'])
            v_val = data['v_val']
            
            if key not in learned_keys:
                phase2b_responses[key][v_rnd].add(v_val)
                
                # Check if we have majority for this round and value
                if len(phase2b_responses[key][v_rnd]) == 1 and len(phase2b_responses[key][v_rnd]) > math.ceil(3/2):
                    learned_keys.add(key)
                    learned_values.append(v_val)
                    print(v_val)
                    sys.stdout.flush()
                    phase2b_responses[key].clear()

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        client_msg = Message.phase_client(value, Message.CLIENT)
        s.sendto(client_msg.encode(), config["proposers"])
    
    print(f"Client {id} finished")


if __name__ == "__main__":
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    
    if role == "acceptor":
        rolefunc = acceptor
    elif role == "proposer":
        rolefunc = proposer
    elif role == "learner":
        rolefunc = learner
    elif role == "client":
        rolefunc = client
    rolefunc(config, id)