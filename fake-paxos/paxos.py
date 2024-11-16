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
    # Static phase values
    PHASE1A = "PHASE_1A"
    PHASE1B = "PHASE_1B"
    PHASE2A = "PHASE_2A"
    PHASE2B = "PHASE_3"  # This is DECIDE
    CLIENT = "CLIENT_VALUE"

    @staticmethod
    def prepare(c_rnd, seq):
        msg = {
            "type": Message.PHASE1A,
            "c_rnd": c_rnd,
            "seq": seq
        }
        return json.dumps(msg).encode()

    @staticmethod
    def promise(rnd, v_rnd, v_val, seq):
        msg = {
            "type": Message.PHASE1B,
            "rnd": rnd,
            "v_rnd": v_rnd,
            "v_val": v_val,
            "seq": seq
        }
        return json.dumps(msg).encode()

    @staticmethod
    def accept(c_rnd, c_val, seq):
        msg = {
            "type": Message.PHASE2A,
            "c_rnd": c_rnd,
            "c_val": c_val,
            "seq": seq
        }
        return json.dumps(msg).encode()

    @staticmethod
    def decide(v_rnd, v_val, seq):
        msg = {
            "type": Message.PHASE2B,
            "v_rnd": v_rnd,
            "v_val": v_val,
            "seq": seq
        }
        return json.dumps(msg).encode()

    @staticmethod
    def client_value(value, client_id, timestamp):
        msg = {
            "type": Message.CLIENT,
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

def acceptor(config, id):
    print("-> acceptor", id)
    states = {}
    r = mcast_receiver(config["acceptors"])
    s = mcast_sender()
    while True:
        msg = json.loads(r.recv(2**16).decode())
        
        seq = tuple(msg["seq"]) if "seq" in msg else None
        c_rnd = tuple(msg["c_rnd"]) if "c_rnd" in msg else None

        if msg["type"] == Message.PHASE1A:
            if seq not in states:
                states[seq] = {"rnd": (0, id), "v_rnd": None, "v_val": None}
            if c_rnd > states[seq]["rnd"]:
                states[seq]["rnd"] = c_rnd
                promise_msg = Message.promise(
                    states[seq]["rnd"],
                    states[seq]["v_rnd"],
                    states[seq]["v_val"],
                    seq
                )
                print(
                    "-> acceptor",
                    id,
                    " Received: PHASE1A",
                    "seq",
                    seq,
                    "Rnd:",
                    states[seq]["rnd"],
                )
                s.sendto(promise_msg, config["proposers"])
        elif msg["type"] == Message.PHASE2A:
            if c_rnd >= states[seq]["rnd"]:
                states[seq]["v_rnd"] = c_rnd
                states[seq]["v_val"] = msg["c_val"]
                decide_msg = Message.decide(
                    states[seq]["v_rnd"],
                    states[seq]["v_val"],
                    seq
                )
                print(
                    "-> acceptor",
                    id,
                    "seq",
                    seq,
                    "Received: PHASE2A",
                    "v_rnd:",
                    states[seq]["v_rnd"],
                    "v_val:",
                    states[seq]["v_val"],
                )
                s.sendto(decide_msg, config["learners"])
                s.sendto(decide_msg, config["proposers"])

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
        
        msg_type = msg["type"]
        seq = tuple(msg["seq"]) if "seq" in msg else None
        rnd = tuple(msg["rnd"]) if "rnd" in msg else None
        v_rnd = tuple(msg["v_rnd"]) if "v_rnd" in msg and msg["v_rnd"] is not None else None

        if msg_type == Message.CLIENT:
            seq = (msg["timestamp"], msg["client_id"])
            c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
            c_rnd[seq] = c_rnd_cnt
            c_val[seq] = msg["value"]
            prepare_msg = Message.prepare(c_rnd[seq], seq)
            print(
                "-> proposer",
                id,
                "Received: CLIENT_VALUE",
                "c_rnd:",
                c_rnd[seq],
                "seq:",
                seq,
                "value:",
                msg["value"]
            )
            pending[seq] = time.time()
            s.sendto(prepare_msg, config["acceptors"])

        elif msg_type == Message.PHASE1B:
            if seq not in c_rnd or rnd != c_rnd[seq]:
                continue

            if seq not in promises:
                promises[seq] = []

            promises[seq].append(msg)
            if len(promises[seq]) >= math.ceil(3 / 2):
                k = max((p["v_rnd"] for p in promises[seq] if p["v_rnd"]), default=None)
                if k:
                    c_val[seq] = next(
                        p["v_val"] for p in promises[seq] if p["v_rnd"] == k
                    )
                print(
                    "-> proposer",
                    id,
                    "seq",
                    seq,
                    "Received: PHASE1B",
                    "c_rnd:",
                    c_rnd[seq],
                    "c_val:",
                    c_val[seq],
                )
                accept_msg = Message.accept(c_rnd[seq], c_val[seq], seq)
                s.sendto(accept_msg, config["acceptors"])
                promises[seq] = []

        elif msg_type == Message.PHASE2B:
            if seq not in learned:
                learned.append(seq)
                if seq in pending:
                    pending.pop(seq)

        # Retry mechanism
        current_time = time.time()
        for seq in list(pending.keys()):
            if seq not in learned and current_time - pending[seq] > 5:
                c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
                c_rnd[seq] = c_rnd_cnt
                prepare_msg = Message.prepare(c_rnd[seq], seq)
                print(
                    "-> proposer",
                    id,
                    "Retrying PREPARE",
                    "c_rnd:",
                    c_rnd[seq],
                    "seq:",
                    seq,
                )
                pending[seq] = time.time()
                s.sendto(prepare_msg, config["acceptors"])

def learner(config, id):
    r = mcast_receiver(config["learners"])
    learned = {}

    while True:
        msg = json.loads(r.recv(2**16).decode())
        
        seq = tuple(msg["seq"]) if "seq" in msg else None
        v_rnd = tuple(msg["v_rnd"]) if "v_rnd" in msg and msg["v_rnd"] is not None else None

        if msg["type"] == Message.PHASE2B:
            if seq not in learned:
                learned[seq] = msg["v_val"]
                print(msg["v_val"])
                sys.stdout.flush()

def client(config, id):
    print(f"-> client {id} starting")
    s = mcast_sender()
    last_timestamp = 0

    for value in sys.stdin:
        value = value.strip()
        current_time = time.time()
        # Ensure monotonically increasing timestamp
        timestamp = max(int(current_time * 1_000_000), last_timestamp + 1)
        last_timestamp = timestamp
        
        print(f"client {id}: sending {value} to proposers with timestamp {timestamp}")
        client_msg = Message.client_value(value, id, timestamp)
        s.sendto(client_msg, config["proposers"])
        # Small sleep to ensure unique timestamps
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