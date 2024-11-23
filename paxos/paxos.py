#!/usr/bin/env python3
import sys
import socket
import struct
import json
import math
import time
from collections import defaultdict

def create_phase1a_message(c_rnd, instance, client_id):
    return json.dumps({
        "type": "PHASE1A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "slot": instance,
        "client_id": client_id
    }).encode()

def create_phase1b_message(rnd, v_rnd, v_val, instance, client_id):
    return json.dumps({
        "type": "PHASE1B",
        "rnd_1": rnd[0],
        "rnd_2": rnd[1],
        "v_rnd_1": v_rnd[0] if v_rnd else None,
        "v_rnd_2": v_rnd[1] if v_rnd else None,
        "v_val": v_val,
        "slot": instance,
        "client_id": client_id
    }).encode()

def create_phase2a_message(c_rnd, c_val, instance, client_id):
    return json.dumps({
        "type": "PHASE2A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "c_val": c_val,
        "client_id": client_id,
        "slot": instance
    }).encode()

def create_phase2b_message(v_rnd, v_val, instance, client_id):
    return json.dumps({
        "type": "PHASE2B",
        "v_rnd_1": v_rnd[0],
        "v_rnd_2": v_rnd[1],
        "v_val": v_val,
        "slot": instance,
        "client_id": client_id,
    }).encode()

def create_decision_message(v_val, instance):
    return json.dumps({
        "type": "DECISION",
        "v_val": v_val,
        "slot": instance
    }).encode()

def create_propose_message(value, client_id):
    return json.dumps({
        "type": "PROPOSE",
        "value": value,
        "client_id": client_id
    }).encode()

def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg

def mcast_receiver(hostport):
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)
    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock

def mcast_sender():
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

def get_quorum(n_acceptors):
    return math.ceil((n_acceptors + 1) / 2)

def acceptor(config, id):
    print(f"-> acceptor {id}")
    acceptor_states = defaultdict(lambda: {"rnd": (0, 0), "v_rnd": (0, 0), "v_val": None})
    decisions = {}  
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()

    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            instance = msg.get("slot")
            client_id = msg.get("client_id")

            if msg_type == "PHASE1A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                state = acceptor_states[instance]
                if c_rnd > state["rnd"]:
                    state["rnd"] = c_rnd
                    phase1b = create_phase1b_message(state["rnd"], state["v_rnd"], state["v_val"], instance, client_id)
                    s.sendto(phase1b, config["proposers"])

            elif msg_type == "PHASE2A":
                client_id= msg["client_id"]
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                c_val = msg["c_val"]
                state = acceptor_states[instance]
                if c_rnd >= state["rnd"]:
                    state["v_rnd"] = c_rnd
                    state["v_val"] = c_val
                    phase2b = create_phase2b_message(state["v_rnd"], state["v_val"], instance, client_id)
                    s.sendto(phase2b, config["proposers"])
                    decisions[instance] = (c_val,client_id) 

            elif msg_type == "CATCHUP":
                catchup_response = {
                    "type": "CATCHUP",
                    "decided": decisions
                }
                s.sendto(json.dumps(catchup_response).encode(), config["learners"])

        except Exception as e:
            print(f"Acceptor {id} error: {e}")


def proposer(config, id):
    print(f"-> proposer {id}")
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    
    c_rnd = (0, id)
    instance = 0
    pending_values = []
    promises = defaultdict(list)
    phase2b_msgs = defaultdict(list)
    decided = set()
    last_attempt = 0
    TIMEOUT = 1.0
    
    def start_phase1():
        nonlocal last_attempt
        phase1a = create_phase1a_message(c_rnd, instance, client_id)
        s.sendto(phase1a, config["acceptors"])
        last_attempt = time.time()
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            client_id = msg.get("client_id")
            
            if msg_type == "PROPOSE":
                value_tuple = (msg["value"], msg["client_id"], msg["client_id"])
                if not pending_values:
                    pending_values.append(value_tuple)
                    start_phase1()
                else:
                    pending_values.append(value_tuple)
                    
            elif msg_type == "PHASE1B":
                msg_instance = msg["slot"]
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                
                if msg_rnd == c_rnd and msg_instance == instance:
                    v_val = msg["v_val"]
                    if isinstance(v_val, list):
                        v_val = tuple(v_val)
                        
                    promises[instance].append({
                        "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                        "v_val": v_val
                    })
                    
                    if len(promises[instance]) >= get_quorum(3):
                        valid_promises = [p for p in promises[instance] if p["v_rnd"] is not None and p["v_val"] is not None]
                        
                        if valid_promises:
                            # Must decide on the value already present in this instance
                            highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                            c_val = highest_promise["v_val"]
                            phase2a = create_phase2a_message(c_rnd, c_val, instance, client_id )
                            s.sendto(phase2a, config["acceptors"])
                        else:
                            # Instance is free - propose our value
                            if pending_values:
                                phase2a = create_phase2a_message(c_rnd, pending_values[0], instance, client_id)
                                s.sendto(phase2a, config["acceptors"])
                        promises[instance].clear()
                        
            elif msg_type == "PHASE2B":
                msg_instance = msg["slot"]
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                
                if msg_v_rnd == c_rnd and msg_instance == instance:
                    value = msg["v_val"]
                    if isinstance(value, list):
                        value = tuple(value)
                    phase2b_msgs[instance].append(value)
                    
                    if len(phase2b_msgs[instance]) >= get_quorum(3):
                        value_counts = {}
                        for v in phase2b_msgs[instance]:
                            if isinstance(v, list):
                                v = tuple(v)
                            value_counts[v] = value_counts.get(v, 0) + 1
                            
                        majority_value = max(value_counts.items(), key=lambda x: x[1])[0]
                        
                        if instance not in decided:
                            decided.add(instance)
                            decision = create_decision_message(majority_value, instance)
                            s.sendto(decision, config["learners"])
                            
                            # Clear current instance state before moving to next
                            promises[instance].clear()
                            phase2b_msgs[instance].clear()
                            
                            if majority_value == pending_values[0]:
                                pending_values.pop(0)
                            
                            instance += 1
                            c_rnd = (0, id)  # Reset c_rnd for new instance
                            if pending_values:
                                start_phase1()
            
            elif msg_type == "DECISION":
                # Track other proposers' decisions to maintain synchronization
                decided_instance = msg["slot"]
                if decided_instance >= instance:
                    instance = decided_instance + 1
                    if pending_values:
                        c_rnd = (0, id)
                        start_phase1()
                                
        except BlockingIOError:
            if pending_values and time.time() - last_attempt > TIMEOUT and instance not in decided:
                c_rnd = (c_rnd[0] + 1, c_rnd[1])
                start_phase1()
        except Exception as e:
            print(f"Proposer {id} error: {e}")

def learner(config, id):
    r = mcast_receiver(config["learners"])
    s = mcast_sender()
    catchup_request = json.dumps({"type": "CATCHUP"}).encode()
    s.sendto(catchup_request, config["acceptors"])
    printed_values = set()
    decisions = {}

    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            if msg["type"] == "DECISION":
                value_tuple = msg["v_val"]
                if (value_tuple[0], value_tuple[1]) not in printed_values:
                    print(f"{value_tuple[0]}")
                    printed_values.add((value_tuple[0], value_tuple[1]))
                    sys.stdout.flush()

            elif msg["type"] == "CATCHUP":
                decided = msg.get("decided", {})
                for slot, (value_tuple, client_id) in sorted(decided.items(), key=lambda x: int(x[0])):
                    if (value_tuple[0], client_id) not in printed_values:
                        print(f"{value_tuple[0]}")
                        printed_values.add((value_tuple[0], client_id))
                        sys.stdout.flush()

        except Exception as e:
            print(f"Learner {id} error: {e}")


def client(config, id):
    print(f"-> client {id}")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        proposal = create_propose_message(value, id)
        s.sendto(proposal, config["proposers"])
    
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