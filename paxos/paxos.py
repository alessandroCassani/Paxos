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
    def prepare(c_rnd, key, instance_id):
        msg = {
            "phase": Message.PHASE1A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "key": key,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def promise(rnd, v_rnd, v_val, key, instance_id):
        msg = {
            "phase": Message.PHASE1B,
            "rnd_1": rnd[0],
            "rnd_2": rnd[1],
            "v_rnd_1": v_rnd[0] if v_rnd else None,
            "v_rnd_2": v_rnd[1] if v_rnd else None,
            "v_val": v_val,
            "key": key,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def accept(c_rnd, c_val, key, instance_id):
        msg = {
            "phase": Message.PHASE2A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "c_val": c_val,
            "key": key,
            "instance": instance_id
        }
        return json.dumps(msg).encode()

    @staticmethod
    def decide(v_rnd, v_val, key, instance_id):
        msg = {
            "phase": Message.PHASE2B,
            "v_rnd_1": v_rnd[0],
            "v_rnd_2": v_rnd[1],
            "v_val": v_val,
            "key": key,
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
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)
    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock

def mcast_sender():
    """create a udp socket"""
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
    print(f"-> acceptor {id}")
    states = {}
    with lock:
        active_acceptors.add(id)
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    try:
        while True:
            msg = json.loads(r.recv(2**16).decode())
            instance_id = msg.get("instance")
            key = msg.get("key")

            if instance_id not in states:
                states[instance_id] = {"rnd": (0, id), "v_rnd": None, "v_val": None}
                print(f"Created new state for instance {instance_id}")

            if msg["phase"] == Message.PHASE1A:
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                print(f"Received PHASE1A for instance {instance_id}")
                if c_rnd > states[instance_id]["rnd"]:
                    states[instance_id]["rnd"] = c_rnd
                    promise = Message.promise(
                        states[instance_id]["rnd"],
                        states[instance_id]["v_rnd"],
                        states[instance_id]["v_val"],
                        key,
                        instance_id
                    )
                    s.sendto(promise, config["proposers"])
                    print(f"Sent promise for instance {instance_id}")

            elif msg["phase"] == Message.PHASE2A:
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                print(f"Received PHASE2A for instance {instance_id}")
                if c_rnd >= states[instance_id]["rnd"]:
                    states[instance_id]["v_rnd"] = c_rnd
                    states[instance_id]["v_val"] = msg["c_val"]
                    decide_msg = Message.decide(
                        states[instance_id]["v_rnd"],
                        states[instance_id]["v_val"],
                        key,
                        instance_id
                    )
                    s.sendto(decide_msg, config["proposers"])
                    s.sendto(decide_msg, config["learners"])  # Send directly to learners
                    print(f"Sent PHASE2B for instance {instance_id} to proposers and learners")
    except Exception as e:
        print(f"Error in acceptor: {e}")
        raise e
    finally:
        with lock:
            active_acceptors.remove(id)

def proposer(config, id):
    print(f"-> proposer {id}")
    r = mcast_receiver(config["proposers"])
    s = mcast_sender()
    
    c_rnd_cnt = (0, id)
    current_instance = 0
    decided_values = set()
    pending_values = []
    instances = {}
    pending_timers = {}

    def start_next_instance():
        nonlocal current_instance, c_rnd_cnt
        if pending_values and not instances.get(current_instance, {}).get("pending", False):
            value = pending_values[0]
            if value not in decided_values:
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                
                if current_instance not in instances:
                    instances[current_instance] = {
                        "c_rnd": c_rnd_cnt,
                        "c_val": value,
                        "promises": [],
                        "pending": True
                    }
                
                prepare_msg = Message.prepare(c_rnd_cnt, ("value", id), current_instance)
                s.sendto(prepare_msg, config["acceptors"])
                print(f"Started instance {current_instance} with value {value}")
                
                if current_instance in pending_timers:
                    pending_timers[current_instance].cancel()
                pending_timers[current_instance] = threading.Timer(
                    5.0, 
                    resend_prepare, 
                    args=(current_instance, prepare_msg, config["acceptors"], s)
                )
                pending_timers[current_instance].start()

    try:
        while True:
            msg = json.loads(r.recv(2**16).decode())
            phase = msg["phase"]

            if phase == Message.CLIENT:
                pending_values.append(msg["value"])
                print(f"Received value: {msg['value']}")
                if current_instance == len(instances):
                    start_next_instance()

            elif phase == Message.PHASE1B:
                instance_id = msg["instance"]
                if instance_id != current_instance:
                    continue

                inst = instances[instance_id]
                inst["promises"].append(msg)
                print(f"Received promise for instance {instance_id}, now have {len(inst['promises'])} promises")

                if len(inst["promises"]) > get_quorum() and get_quorum() != 0:
                    if any(p["v_rnd_1"] is not None and p["v_rnd_2"] is not None for p in inst["promises"]):
                        k = max(
                            (p["v_rnd_1"], p["v_rnd_2"]) 
                            for p in inst["promises"] 
                            if p["v_rnd_1"] is not None and p["v_rnd_2"] is not None
                        )
                        if k:
                            inst["c_val"] = next(
                                p["v_val"] 
                                for p in inst["promises"] 
                                if (p["v_rnd_1"], p["v_rnd_2"]) == k
                            )

                    accept_msg = Message.accept(inst["c_rnd"], inst["c_val"], ("value", id), instance_id)
                    s.sendto(accept_msg, config["acceptors"])
                    print(f"Sent accept for instance {instance_id}")
                    inst["promises"] = []

                    if instance_id in pending_timers:
                        pending_timers[instance_id].cancel()
                        del pending_timers[instance_id]

            elif phase == Message.PHASE2B:
                instance_id = msg["instance"]
                if instance_id != current_instance:
                    continue

                print(f"Received PHASE2B for instance {instance_id}")

                if instance_id in pending_timers:
                    pending_timers[instance_id].cancel()
                    del pending_timers[instance_id]

                decided_values.add(msg["v_val"])
                pending_values.pop(0)
                instances[instance_id]["pending"] = False
                current_instance += 1
                
                if pending_values:
                    start_next_instance()

    except Exception as e:
        print(f"Error in proposer: {e}")
        raise e

def learner(config, id):
    print(f"-> learner {id}")
    r = mcast_receiver(config["learners"])
    learned = {}
    current_instance = 0
    
    try:
        while True:
            data = r.recv(2**16)
            print(f"Learner received data of length: {len(data)}")
            msg = json.loads(data.decode())
            print(f"Learner decoded message for instance: {msg.get('instance')}")
            
            if msg["phase"] == Message.PHASE2B:
                instance_id = msg["instance"]
                if instance_id not in learned:
                    learned[instance_id] = msg["v_val"]
                    print(f"=== DECIDED === Value {msg['v_val']} in instance {instance_id}")
                    sys.stdout.flush()
    except Exception as e:
        print(f"Error in learner: {e}")
        raise e

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    
    # Read all values first
    values = []
    for value in sys.stdin:
        values.append(value.strip())
    
    print(f"Client {id} will propose {len(values)} values")
    
    # Send values
    for value in values:
        current_time = time.time()
        timestamp = int(current_time * 1_000_000)
        client_msg = Message.client_value(value, id, timestamp)
        s.sendto(client_msg, config["proposers"])
        time.sleep(0.001)
        print(f"Client {id} sent value: {value}")
    
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