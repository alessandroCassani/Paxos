#!/usr/bin/env python3

import sys
import socket
import struct
import json
import math
import time

def create_phase1a_message(c_rnd, instance_id):
    return json.dumps({
        "type": "PHASE1A",
        "c_rnd": c_rnd,
        "instance": instance_id
    }).encode()

def create_phase1b_message(rnd, v_rnd, v_val, instance_id):
    return json.dumps({
        "type": "PHASE1B",
        "rnd": rnd,
        "v_rnd": v_rnd,
        "v_val": v_val,
        "instance": instance_id
    }).encode()

def create_phase2a_message(c_rnd, c_val, instance_id):
    return json.dumps({
        "type": "PHASE2A",
        "c_rnd": c_rnd,
        "c_val": c_val,
        "instance": instance_id
    }).encode()

def create_phase2b_message(v_rnd, v_val, instance_id):
    return json.dumps({
        "type": "PHASE2B",
        "v_rnd": v_rnd,
        "v_val": v_val,
        "instance": instance_id
    }).encode()

def create_decision_message(v_val, instance_id):
    return json.dumps({
        "type": "DECISION",
        "v_val": v_val,
        "instance": instance_id
    }).encode()

def create_propose_message(value, client_id, timestamp):
    return json.dumps({
        "type": "PROPOSE",
        "value": value,
        "client_id": client_id,
        "timestamp": timestamp
    }).encode()

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

def get_quorum(n_acceptors):
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
            instance_id = msg.get("instance")

            if instance_id is not None and instance_id not in states:
                states[instance_id] = {"rnd": 0, "v_rnd": 0, "v_val": None}
                print(f"Acceptor {id}: New instance {instance_id}")

            if msg_type == "PHASE1A":
                c_rnd = msg["c_rnd"]
                state = states[instance_id]
                print(f"Acceptor {id}: PHASE1A for instance {instance_id}, c_rnd {c_rnd}")
                
                if c_rnd > state["rnd"]:
                    state["rnd"] = c_rnd
                    phase1b = create_phase1b_message(state["rnd"], state["v_rnd"], state["v_val"], instance_id)
                    s.sendto(phase1b, config["proposers"])

            elif msg_type == "PHASE2A":
                c_rnd = msg["c_rnd"]
                c_val = msg["c_val"]
                state = states[instance_id]
                print(f"Acceptor {id}: PHASE2A for instance {instance_id}")
                
                if c_rnd >= state["rnd"]:
                    state["v_rnd"] = c_rnd
                    state["v_val"] = c_val
                    phase2b = create_phase2b_message(state["v_rnd"], state["v_val"], instance_id)
                    s.sendto(phase2b, config["proposers"])
                    
        except Exception as e:
            print(f"Acceptor {id} error: {e}")

def proposer(config, id):
    print(f"-> proposer {id}")
    c_rnd = id
    current_instance = 0
    pending_values = []
    decided_values = set()
    instances = {}

    r = mcast_receiver(config["proposers"])
    s = mcast_sender()

    def start_instance(instance_id, proposal_tuple):
        nonlocal c_rnd
        c_rnd += 100
        
        instances[instance_id] = {
            "c_val": {
                "value": proposal_tuple[0],
                "client_id": proposal_tuple[1],
                "timestamp": proposal_tuple[2]
            },
            "promises": [],
            "phase2b_msgs": []
        }
        
        phase1a = create_phase1a_message(c_rnd, instance_id)
        s.sendto(phase1a, config["acceptors"])
        print(f"Proposer {id}: Started instance {instance_id} for value {proposal_tuple[0]}")

    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            if msg_type == "PROPOSE":
                proposal = (msg["value"], msg["client_id"], msg["timestamp"])
                if proposal not in decided_values:
                    pending_values.append(proposal)
                    if len(instances) == current_instance:
                        start_instance(current_instance, pending_values[0])
                
            elif msg_type == "PHASE1B":
                instance_id = msg["instance"]
                if instance_id != current_instance:
                    continue
                
                inst = instances[instance_id]
                inst["promises"].append(msg)
                
                if len(inst["promises"]) >= get_quorum(3):
                    promises = inst["promises"]
                    k = max((p["v_rnd"] for p in promises), default=0)
                    
                    if k == 0:
                        c_val = inst["c_val"]
                    else:
                        c_val = next(p["v_val"] for p in promises if p["v_rnd"] == k)
                    
                    phase2a = create_phase2a_message(c_rnd, c_val, instance_id)
                    s.sendto(phase2a, config["acceptors"])
                    inst["promises"] = []
                
            elif msg_type == "PHASE2B":
                instance_id = msg["instance"]
                if instance_id != current_instance:
                    continue
                
                inst = instances[instance_id]
                inst["phase2b_msgs"].append(msg)
                
                if len(inst["phase2b_msgs"]) >= get_quorum(3):
                    if all(m["v_rnd"] == c_rnd for m in inst["phase2b_msgs"]):
                        v_val = inst["phase2b_msgs"][0]["v_val"]
                        decided_tuple = (v_val["value"], v_val["client_id"], v_val["timestamp"])
                        decided_values.add(decided_tuple)
                        pending_values.pop(0)
                        
                        decision = create_decision_message(v_val, instance_id)
                        s.sendto(decision, config["learners"])
                        print(f"Proposer {id}: Decision reached for instance {instance_id}")
                        
                        current_instance += 1
                        if pending_values:
                            start_instance(current_instance, pending_values[0])
                        
                        inst["phase2b_msgs"] = []
                
        except Exception as e:
            print(f"Proposer {id} error: {e}")

def learner(config, id):
    r = mcast_receiver(config["learners"])
    learned = {}
    current_instance = 0
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            
            if msg["type"] == "DECISION":
                instance_id = msg["instance"]
                v_val = msg["v_val"]
                value_tuple = (v_val["value"], v_val["client_id"], v_val["timestamp"])
                
                if instance_id == current_instance and instance_id not in learned:
                    learned[instance_id] = value_tuple
                    print(value_tuple[0])
                    sys.stdout.flush()
                    current_instance += 1
                    
        except Exception as e:
            print(f"Learner {id} error: {e}")

def client(config, id):
    print(f"-> client {id}")
    s = mcast_sender()
    
    values = []
    for value in sys.stdin:
        values.append(value.strip())
    
    print(f"Client {id} will propose {len(values)} values")
    
    for value in values:
        timestamp = int(time.time() * 1_000_000)  
        proposal = create_propose_message(value, id, timestamp)
        s.sendto(proposal, config["proposers"])
        print(f"Client {id} proposed: {value}")
        time.sleep(0.001)  
    
    print(f"Client {id} done.")

if __name__ == "__main__":
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    
    if role == "acceptor":
        acceptor(config, id)
    elif role == "proposer":
        proposer(config, id)
    elif role == "learner":
        learner(config, id)
    elif role == "client":
        client(config, id)