#!/usr/bin/env python3
import sys
import socket
import struct
import time
from collections import defaultdict
import threading
import json

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
    global active_acceptors
    print(f"-> acceptor {id} starting")
    with lock:
        active_acceptors.add(id)
        print(f"Acceptor {id}: Total acceptors now {active_acceptors}")
    try:    
        state = {}
        r = mcast_receiver(config["acceptors"])
        s = mcast_sender()
        
        while True:
            msg = r.recv(2**16)
            print(f"\nAcceptor {id} received message: {msg}")
            phase, data = Message.handle_acceptor_received_message(msg)
        
            if phase == Message.PHASE1A:
                key = (data['key_1'], data['key_2'])
                print(f"Acceptor {id}: Processing Phase1A for key {key}")
                print(f"Acceptor {id}: Current state for key {key}: {state.get(key, 'Not found')}")
            
                if key not in state:
                    state[key] = {"rnd": (0, id), "v_rnd": (0, 0), "v_val": None}
                    print(f"Acceptor {id}: Initialized state for key {key}")
            
                if data['c_rnd_1'] > state[key]['rnd'][0] or (data['c_rnd_1'] == state[key]['rnd'][0] and data['c_rnd_2'] > state[key]['rnd'][1]):
                    old_rnd = state[key]['rnd']
                    state[key]['rnd'] = (data['c_rnd_1'], data['c_rnd_2'])
                    print(f"Acceptor {id}: Updated round from {old_rnd} to {state[key]['rnd']}")
                    
                    phase1b_message = Message.phase_1b(state[key]['rnd'], state[key]['v_rnd'], state[key]['v_val'], Message.PHASE1B, key)
                    print(f"Acceptor {id}: Sending Phase1B message: {phase1b_message}")
                    s.sendto(phase1b_message.encode(), config["proposers"])
                else:
                    print(f"Acceptor {id}: Rejected Phase1A, current round {state[key]['rnd']} >= received round ({data['c_rnd_1']}, {data['c_rnd_2']})")
        
            elif phase == Message.PHASE2A:
                key = (data['key_1'], data['key_2'])
                print(f"Acceptor {id}: Processing Phase2A for key {key}")
                print(f"Acceptor {id}: Current state for key {key}: {state.get(key, 'Not found')}")
                
                if key not in state:
                    state[key] = {"rnd": (0, id), "v_rnd": (0, 0), "v_val": None}
                    print(f"Acceptor {id}: Initialized state for key {key}")
                        
                if (data['c_rnd_1'], data['c_rnd_2']) >= state[key]['rnd']:
                    old_v_rnd = state[key]['v_rnd']
                    old_v_val = state[key]['v_val']
                    state[key]['v_rnd'] = (data['c_rnd_1'], data['c_rnd_2'])
                    state[key]['v_val'] = data['c_val']
                    print(f"Acceptor {id}: Accepted Phase2A - Updated v_rnd from {old_v_rnd} to {state[key]['v_rnd']}")
                    print(f"Acceptor {id}: Updated v_val from {old_v_val} to {state[key]['v_val']}")
                            
                    phase2b_message = Message.phase_2b(state[key]['v_rnd'], state[key]['v_val'], Message.PHASE2B, key)
                    s.sendto(phase2b_message.encode(), config["proposers"])
                else:
                    print(f"Acceptor {id}: Rejected Phase2A, current round {state[key]['rnd']} > received round ({data['c_rnd_1']}, {data['c_rnd_2']})")
    finally:
        with lock:
            active_acceptors.remove(id)

def proposer(config, id):
    print(f"-> proposer {id} starting")
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    
    c_rnd_cnt = (id, 0)
    c_rnd = {}
    c_val = {}
    promises = defaultdict(list)
    phase2b_responses = defaultdict(list)
    accepted_values = defaultdict(set)
    
    while True:
        msg = r.recv(2**16)
        print(f"\nProposer {id} received message: {msg}")
        
        phase, data = Message.handle_proposer_received_message(msg)
        print(f"Proposer {id}: Message type: {phase}")
        
        if phase == Message.CLIENT:
            timestamp = int(time.time() * 1000)
            key = (id, timestamp)
            c_rnd_cnt = (c_rnd_cnt[0], c_rnd_cnt[1] + 1)
            c_rnd[key] = c_rnd_cnt
            c_val[key] = data['value']
            print(f"Proposer {id}: New proposal - Key: {key}, Round: {c_rnd[key]}, Value: {c_val[key]}")
            
            phase1a_msg = Message.phase_1a(c_rnd[key], Message.PHASE1A, key)
            print(f"Proposer {id}: Sending Phase1A message: {phase1a_msg}")
            s.sendto(phase1a_msg.encode(), config["acceptors"])
        
        elif phase == Message.PHASE1B:
            key = (data['key_1'], data['key_2'])
            current_rnd = (data['rnd_1'], data['rnd_2'])
            print(f"Proposer {id}: Processing Phase1B for key {key}")
            print(f"Proposer {id}: Current promises: {promises[key]}")
            print(f"Proposer {id}: Known rounds: {c_rnd}")
            
            if key in c_rnd and current_rnd == c_rnd[key]:
                v_rnd = (data['v_rnd_0'], data['v_rnd_1'])
                promises[key].append((v_rnd, data['v_val']))
                print(f"Proposer {id}: Added promise. Count: {len(promises[key])}/{active_acceptors}")
                
                if len(promises[key]) > len(active_acceptors) / 2:
                    print(f"Proposer {id}: Achieved majority for key {key}")
                    k = None
                    for p in promises[key]:
                        if p[0] != (0, 0):
                            if k is None or p[0] > k:
                                k = p[0]
                    
                    if k:
                        c_val[key] = next(p[1] for p in promises[key] if p[0] == k)
                        print(f"Proposer {id}: Selected value {c_val[key]} from round {k}")
                    
                    phase2a_msg = Message.phase_2a(c_rnd[key], c_val[key], Message.PHASE2A, key)
                    print(f"Proposer {id}: Sending Phase2A message: {phase2a_msg}")
                    s.sendto(phase2a_msg.encode(), config["acceptors"])
                    promises[key] = []
        elif phase == Message.PHASE2B:
            key = (data['key_1'], data['key_2'])
            if key in c_rnd:
                phase2b_responses[key].append(data) 
                if len(phase2b_responses[key]) == (len(active_acceptors) // 2) + 1:  # Check for majority
                    decision_msg = Message.decision(c_val[key], Message.DECIDE, key)  
                    s.sendto(decision_msg.encode(), config["learners"])
                    

def learner(config, id):
    print(f"-> learner {id} starting")
    r = mcast_receiver(config["learners"])
    decisions = defaultdict(list)
    
    while True:
        msg = r.recv(2**16)
        print(f"\nLearner {id} received message: {msg}")
        data = json.loads(msg)
        
        key = (data['key_1'], data['key_2'])
        decisions[key].append(data['v_val'])
        print(f"Learner {id}: Decision for key {key}")
        print(f"Learner {id}: Value: {data['v_val']}")
        print(f"Learner {id}: Total decisions for this key: {len(decisions[key])}")
        sys.stdout.flush()

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        client_msg = Message.phase_client(value, Message.CLIENT)
        print(f"Client {id}: Sending value {value}")
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