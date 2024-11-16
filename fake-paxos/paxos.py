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
    PHASE2B = "PHASE_3"
    CLIENT = "CLIENT_VALUE"

    @staticmethod
    def prepare(c_rnd, key):
        msg = {
            "phase": Message.PHASE1A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "key": key
        }
        return json.dumps(msg).encode()

    @staticmethod
    def promise(rnd, v_rnd, v_val, key):
        msg = {
            "phase": Message.PHASE1B,
            "rnd_1": rnd[0],
            "rnd_2": rnd[1],
            "v_rnd_1": v_rnd[0] if v_rnd else None,
            "v_rnd_2": v_rnd[1] if v_rnd else None,
            "v_val": v_val,
            "key": key
        }
        return json.dumps(msg).encode()

    @staticmethod
    def accept(c_rnd, c_val, key):
        msg = {
            "phase": Message.PHASE2A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "c_val": c_val,
            "key": key
        }
        return json.dumps(msg).encode()

    @staticmethod
    def decide(v_rnd, v_val, key):
        msg = {
            "phase": Message.PHASE2B,
            "v_rnd_1": v_rnd[0],
            "v_rnd_2": v_rnd[1],
            "v_val": v_val,
            "key": key
        }
        return json.dumps(msg).encode()

    @staticmethod
    def client_value(value, client_id, timestamp):
        msg = {
            "phase": Message.CLIENT,
            "value": value,
            "client_id": client_id,
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

def acceptor(config, id):   
    print("-> acceptor", id)
    states = {}
    with lock:
        active_acceptors.add(id)
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    try:
        while True:
            msg = json.loads(r.recv(2**16).decode())
            
            key = tuple(msg["key"]) if "key" in msg else None
            c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])

            if msg["phase"] == Message.PHASE1A:
                if key not in states:
                    states[key] = {"rnd": (0, id), "v_rnd": None, "v_val": None}
                if c_rnd > states[key]["rnd"]:
                    states[key]["rnd"] = c_rnd
                    promise_msg = Message.promise(
                        states[key]["rnd"],
                        states[key]["v_rnd"],
                        states[key]["v_val"],
                        key
                    )
                    s.sendto(promise_msg, config["proposers"])
            elif msg["phase"] == Message.PHASE2A:
                if c_rnd >= states[key]["rnd"]:
                    states[key]["v_rnd"] = c_rnd
                    states[key]["v_val"] = msg["c_val"]
                    decide_msg = Message.decide(
                        states[key]["v_rnd"],
                        states[key]["v_val"],
                        key
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
    c_rnd = {}
    c_val = {}
    promises = {}
    pending = {}
    learned = []

    while True:
        msg = json.loads(r.recv(2**16).decode())
        
        phase = msg["phase"]
        key = tuple(msg["key"]) if "key" in msg else None
        rnd = (msg["rnd_1"], msg["rnd_2"]) if "rnd_1" in msg and "rnd_2" in msg else None
        
        if phase == Message.CLIENT:
            key = (msg["timestamp"], msg["client_id"])
            c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
            c_rnd[key] = c_rnd_cnt
            c_val[key] = msg["value"]
            prepare_msg = Message.prepare(c_rnd[key], key)
            pending[key] = time.time()
            s.sendto(prepare_msg, config["acceptors"])

        elif phase == Message.PHASE1B:
            if key not in c_rnd or rnd != c_rnd[key]:
                continue

            if key not in promises:
                promises[key] = []

            promises[key].append(msg)
            if len(promises[key]) > get_quorum() and get_quorum != 0:
                # Check if there are any valid v_rnd values
                if any(p["v_rnd_1"] is not None and p["v_rnd_2"] is not None for p in promises[key]):
                    k = max((p["v_rnd_1"], p["v_rnd_2"]) for p in promises[key] if p["v_rnd_1"] is not None and p["v_rnd_2"] is not None)
                    if k:
                        c_val[key] = next(
                            p["v_val"] for p in promises[key] if (p["v_rnd_1"], p["v_rnd_2"]) == k
                        )
                accept_msg = Message.accept(c_rnd[key], c_val[key], key)
                s.sendto(accept_msg, config["acceptors"])
                promises[key] = []
            else:
                print('acceptors unavailables')

        elif phase == Message.PHASE2B:
            if key not in learned:
                learned.append(key)
                if key in pending:
                    pending.pop(key)

        # Retry mechanism
        current_time = time.time()
        for key in list(pending.keys()):
            if key not in learned and current_time - pending[key] > 5:
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                c_rnd[key] = c_rnd_cnt
                prepare_msg = Message.prepare(c_rnd[key], key)
                pending[key] = time.time()
                s.sendto(prepare_msg, config["acceptors"])

def learner(config, id):
    r = mcast_receiver(config["learners"])
    learned = {}

    while True:
        msg = json.loads(r.recv(2**16).decode())
        
        key = tuple(msg["key"]) if "key" in msg else None

        if msg["phase"] == Message.PHASE2B:
            if key not in learned:
                learned[key] = msg["v_val"]
                print(msg["v_val"])
                sys.stdout.flush()

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    last_timestamp = 0

    for value in sys.stdin:
        value = value.strip()
        current_time = time.time()
        timestamp = max(int(current_time * 1_000_000), last_timestamp + 1)
        last_timestamp = timestamp
        
        print(f"client {id}: sending {value} to proposers with timestamp {timestamp}")
        client_msg = Message.client_value(value, id, timestamp)
        s.sendto(client_msg, config["proposers"])
        time.sleep(0.0001)
    
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