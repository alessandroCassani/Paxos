#!/usr/bin/env python3
import sys
import socket
import struct
import time
from collections import defaultdict
import threading
import json
import math

class Message:
    PHASE1A = "PHASE_1A"
    PHASE1B = "PHASE_1B"
    PHASE2A = "PHASE_2A"
    PHASE2B = "PHASE_2B"
    CLIENT = "CLIENT_VALUE"

    @staticmethod
    def prepare(c_rnd, instance_id):
        msg = {
            "phase": Message.PHASE1A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def promise(rnd, v_rnd, v_val, instance_id):
        msg = {
            "phase": Message.PHASE1B,
            "rnd_1": rnd[0],
            "rnd_2": rnd[1],
            "v_rnd_1": v_rnd[0] if v_rnd else None,
            "v_rnd_2": v_rnd[1] if v_rnd else None,
            "v_val": v_val,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def accept(c_rnd, c_val, instance_id):
        msg = {
            "phase": Message.PHASE2A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "c_val": c_val,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def decide(v_rnd, v_val, instance_id):
        msg = {
            "phase": Message.PHASE2B,
            "v_rnd_1": v_rnd[0],
            "v_rnd_2": v_rnd[1],
            "v_val": v_val,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def client_value(value, client_id, timestamp):
        msg = {
            "phase": Message.CLIENT,
            "value": value,
            "id": client_id,
            "timestamp": timestamp
        }
        return json.dumps(msg).encode()

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

def get_quorum():
    if len(active_acceptors) < 2:
        return 0
    else:
        return math.ceil(len(active_acceptors) / 2)

lock = threading.Lock()
active_acceptors = set()

def resend_prepare(instance_id, prepare_msg, acceptors, sender):
    sender.sendto(prepare_msg, acceptors)
    print(f"Resending prepare message for instance {instance_id}")

def acceptor(config, id):   
    print("-> acceptor", id)
    instances = defaultdict(lambda: {"rnd": (0, id), "v_rnd": None, "v_val": None})
    
    with lock:
        active_acceptors.add(id)
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    
    try:
        while True:
            msg = json.loads(r.recv(2**16).decode())
            instance_id = msg.get("instance")
            
            if msg["phase"] == Message.PHASE1A:
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                if c_rnd > instances[instance_id]["rnd"]:
                    instances[instance_id]["rnd"] = c_rnd
                    promise = Message.promise(
                        instances[instance_id]["rnd"],
                        instances[instance_id]["v_rnd"],
                        instances[instance_id]["v_val"],
                        instance_id
                    )
                    s.sendto(promise, config["proposers"])
                    
            elif msg["phase"] == Message.PHASE2A:
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                if c_rnd >= instances[instance_id]["rnd"]:
                    instances[instance_id]["v_rnd"] = c_rnd
                    instances[instance_id]["v_val"] = msg["c_val"]
                    
                    decide_msg = Message.decide(
                        instances[instance_id]["v_rnd"],
                        instances[instance_id]["v_val"],
                        instance_id
                    )
                    s.sendto(decide_msg, config["learners"])    
                    s.sendto(decide_msg, config["proposers"])
    finally:
        with lock:
            active_acceptors.remove(id)

def proposer(config, id):
    print("-> proposer", id)
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    
    c_rnd_cnt = (0, id)
    current_instance = 0  # Track the current instance
    instances = {}  # Track state for each instance
    decided_values = set()  # Track already decided values
    client_queue = []  # Queue of client values waiting to be proposed
    
    def initialize_instance(instance_id):
        if instance_id not in instances:
            instances[instance_id] = {
                "c_rnd": None,
                "c_val": None,
                "promises": [],
                "is_decided": False,
                "pending_timer": None
            }
    
    def start_new_instance(value):
        nonlocal current_instance, c_rnd_cnt
        if value not in decided_values:
            initialize_instance(current_instance)
            c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
            instances[current_instance]["c_rnd"] = c_rnd_cnt
            instances[current_instance]["c_val"] = value
            
            prepare_msg = Message.prepare(c_rnd_cnt, current_instance)
            instances[current_instance]["pending_timer"] = threading.Timer(
                5.0, resend_prepare, 
                args=(current_instance, prepare_msg, config["acceptors"], s)
            )
            instances[current_instance]["pending_timer"].start()
            s.sendto(prepare_msg, config["acceptors"])
    
    while True:
        msg = json.loads(r.recv(2**16).decode())
        phase = msg["phase"]
        
        if phase == Message.CLIENT:
            value = msg["value"]
            client_queue.append(value)
            
            if not instances or instances[current_instance]["is_decided"]:
                if client_queue:
                    start_new_instance(client_queue[0])
                    client_queue.pop(0)
        
        elif phase == Message.PHASE1B:
            instance_id = msg["instance"]
            if instance_id not in instances:
                continue
                
            inst = instances[instance_id]
            rnd = (msg["rnd_1"], msg["rnd_2"])
            
            if rnd != inst["c_rnd"]:
                continue
                
            inst["promises"].append(msg)
            
            if len(inst["promises"]) > get_quorum() and get_quorum() != 0:
                # Handle promises and highest numbered proposal
                if any(p["v_rnd_1"] is not None and p["v_rnd_2"] is not None for p in inst["promises"]):
                    k = max(
                        (p["v_rnd_1"], p["v_rnd_2"]) 
                        for p in inst["promises"] 
                        if p["v_rnd_1"] is not None and p["v_rnd_2"] is not None
                    )
                    if k:
                        inst["c_val"] = next(
                            p["v_val"] for p in inst["promises"] 
                            if (p["v_rnd_1"], p["v_rnd_2"]) == k
                        )
                
                accept_msg = Message.accept(inst["c_rnd"], inst["c_val"], instance_id)
                s.sendto(accept_msg, config["acceptors"])
                inst["promises"] = []
                
                if inst["pending_timer"]:
                    inst["pending_timer"].cancel()
                    inst["pending_timer"] = None
        
        elif phase == Message.PHASE2B:
            instance_id = msg["instance"]
            if instance_id in instances:
                inst = instances[instance_id]
                inst["is_decided"] = True
                decided_values.add(inst["c_val"])
                
                if inst["pending_timer"]:
                    inst["pending_timer"].cancel()
                    inst["pending_timer"] = None
                
                if client_queue:
                    current_instance += 1
                    start_new_instance(client_queue[0])
                    client_queue.pop(0)

def learner(config, id):
    print("-> learner", id)
    r = mcast_receiver(config["learners"])
    decisions = {}  # Track decisions by instance
    current_instance = 0  # Track the next instance we expect to learn
    
    while True:
        msg = json.loads(r.recv(2**16).decode())
        if msg["phase"] == Message.PHASE2B:
            instance_id = msg["instance"]
            if instance_id not in decisions:
                decisions[instance_id] = msg["v_val"]
                
                while current_instance in decisions:
                    print(f"Instance {current_instance}: {decisions[current_instance]}")
                    sys.stdout.flush()
                    current_instance += 1

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        current_time = time.time()
        timestamp = int(current_time * 1_000_000)
        client_msg = Message.client_value(value, id, timestamp)
        s.sendto(client_msg, config["proposers"])
        time.sleep(0.001)
    
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