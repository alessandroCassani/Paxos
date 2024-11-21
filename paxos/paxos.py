#!/usr/bin/env python3
import sys
import socket
import struct
import json
import math
import time
from collections import defaultdict
import threading

def create_phase1a_message(c_rnd, key):
    return json.dumps({
        "type": "PHASE1A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "instance": [str(key[0]), str(key[1])]  
    }).encode()

def create_phase1b_message(rnd, v_rnd, v_val, key):
    return json.dumps({
        "type": "PHASE1B",
        "rnd_1": rnd[0],
        "rnd_2": rnd[1],
        "v_rnd_1": v_rnd[0] if v_rnd else None,
        "v_rnd_2": v_rnd[1] if v_rnd else None,
        "v_val": v_val,
        "instance": [str(key[0]), str(key[1])]  
    }).encode()

def create_phase2a_message(c_rnd, c_val, key):
    return json.dumps({
        "type": "PHASE2A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "c_val": c_val,
        "instance": [str(key[0]), str(key[1])]  
    }).encode()

def create_phase2b_message(v_rnd, v_val, key):
    return json.dumps({
        "type": "PHASE2B",
        "v_rnd_1": v_rnd[0],
        "v_rnd_2": v_rnd[1],
        "v_val": v_val,
        "instance": [str(key[0]), str(key[1])]  
    }).encode()

def create_decision_message(v_val, key):
    return json.dumps({
        "type": "DECISION",
        "v_val": v_val,
        "instance": [str(key[0]), str(key[1])] 
    }).encode()

def create_propose_message(value, client_id, timestamp):
    return json.dumps({
        "type": "PROPOSE",
        "value": value,
        "client_id": client_id,
        "timestamp": timestamp
    }).encode()
    
def key_to_str(key_tuple):
        return f"{key_tuple[0]}:{key_tuple[1]}"
    
def str_to_key(key_str):
        timestamp, client_id = key_str.split(":")
        return (int(timestamp), int(client_id))
    

def mcast_receiver(hostport):
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)
    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock

def mcast_sender():
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg

def parse_instance_key(msg):
        instance = msg.get("instance", [])
        if len(instance) == 2:
            return (int(instance[0]), int(instance[1]))  
        return None

def get_quorum(n_acceptors):     #fixed number of acceptors. to modify at runtime!!
    return math.ceil((n_acceptors + 1) / 2)

def acceptor(config, id):
    print(f"-> acceptor {id}")
    states = {}
    
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            key = parse_instance_key(msg)
            
            if key and key not in states:  #initialize
                states[key] = {"rnd": (0, 0), "v_rnd": (0, 0), "v_val": None}
                print(f"Acceptor {id}: New instance {key}")
            
            if msg_type == "PHASE1A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                state = states[key]
                #print(f"Acceptor {id}: PHASE1A for instance {key}, c_rnd {c_rnd}")
                
                if c_rnd > state["rnd"]:
                    state["rnd"] = c_rnd
                    phase1b = create_phase1b_message(state["rnd"], state["v_rnd"], state["v_val"], key)
                    s.sendto(phase1b, config["proposers"])
            
            elif msg_type == "PHASE2A" and key:
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                c_val = msg["c_val"]
                state = states[key]
                print(f"Acceptor {id}: PHASE2A for instance {key}")
                
                if c_rnd >= state["rnd"]:
                    state["v_rnd"] = c_rnd
                    state["v_val"] = c_val
                    phase2b = create_phase2b_message(state["v_rnd"], state["v_val"], key)
                    s.sendto(phase2b, config["proposers"])
                    
        except Exception as e:
            print(f"Acceptor {id} error: {e}")

def resend_propose(key, c_rnd, c_val, sender, retry_timers):
    phase1a = create_phase1a_message(c_rnd[key], key)
    sender.sendto(phase1a, config["acceptors"])
    print(f"Proposer: Retrying propose for instance {key}")
    if key in retry_timers:
        retry_timers[key] = threading.Timer(5.0, lambda: resend_propose(key, c_rnd, c_val, sender, retry_timers))
        retry_timers[key].start()

def proposer(config, id):
    print(f"-> proposer {id}")
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    
    c_rnd_cnt = (0, id)
    c_rnd = {}  
    c_val = {} 
    promises = defaultdict(list)  
    phase2b_msgs = defaultdict(list) 
    retry_timers = {}
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]

            if msg_type == "PROPOSE":
                key = (msg["timestamp"], msg["client_id"])
                
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                c_rnd[key] = c_rnd_cnt
                c_val[key] = msg["value"]
                    
                phase1a = create_phase1a_message(c_rnd[key], key)
                s.sendto(phase1a, config["acceptors"])
                
                retry_timers[key] = threading.Timer(5.0, lambda: resend_propose(key, c_rnd, c_val, s, retry_timers))
                retry_timers[key].start()

            elif msg_type == "PHASE1B":
                key = parse_instance_key(msg)
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                
                if key in c_rnd and msg_rnd == c_rnd[key]:                        
                    promises[key].append({
                        "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                        "v_val": msg["v_val"]
                    })
                    
                    if len(promises[key]) >= get_quorum(3):
                        valid_promises = [p for p in promises[key] if p["v_rnd"] is not None and p["v_val"] is not None]
                        
                        if valid_promises:  
                            highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                            c_val[key] = highest_promise["v_val"]
                        
                        phase2a = create_phase2a_message(c_rnd[key], c_val[key], key)
                        s.sendto(phase2a, config["acceptors"])
                        promises[key] = []

            elif msg_type == "PHASE2B":
                key = parse_instance_key(msg)
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                
                if key in c_rnd and msg_v_rnd == c_rnd[key]:
                    phase2b_msgs[key].append(msg["v_val"])

                    if len(phase2b_msgs[key]) >= get_quorum(3):
                        # Cancel retry timer when we get Phase2B quorum
                        if key in retry_timers:
                            retry_timers[key].cancel()
                            del retry_timers[key]
                            
                        decision = create_decision_message(c_val[key], key)
                        s.sendto(decision, config["learners"])
                        print(f"Proposer {id}: Decision reached for instance {key}")

        except Exception as e:
            print(f"Proposer {id} error: {str(e)}")

def learner(config, id):
    r = mcast_receiver(config["learners"])
    instance_decisions = {}
    
    while True:
        try:
            data = r.recv(2**16)
           # print(f"Learner {id}: Received data: {data.decode()}")  # Debug raw message
            msg = json.loads(data.decode())
            #print(f"Learner {id}: Parsed message type: {msg['type']}")  # Debug message type
            
            if msg["type"] == "DECISION":
                instance_key = parse_instance_key(msg)
                #print(f"Learner {id}: Decision for instance {instance_key}")  # Debug instance
                
                if instance_key not in instance_decisions:
                    value = msg["v_val"]
                    instance_decisions[instance_key] = value
                    print(value)  # Final output
                    sys.stdout.flush()
               # else:
                    #if not instance_key:
                        #print(f"Learner {id}: Failed to parse instance key from {msg.get('instance')}")
                    #else:
                        #print(f"Learner {id}: Already saw instance {instance_key}")
                    
        except Exception as e:
            print(f"Learner {id} error: {str(e)}")

def client(config, id):
    print(f"-> client {id}")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        timestamp = int(time.time() * 1_000_000)
        proposal = create_propose_message(value, id, timestamp)
        s.sendto(proposal, config["proposers"])
        time.sleep(0.001)
    
    print(f"Client {id} finished")

if __name__ == "__main__":
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    
    role_funcs = {
        "acceptor": acceptor,
        "proposer": proposer,
        "learner": learner,
        "client": client
    }
        
    role_funcs[role](config, id)