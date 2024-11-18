#!/usr/bin/env python3
import sys
import socket
import struct
import time
import threading
import json
import math
from collections import defaultdict


class Message:
    PHASE1A = "PHASE_1A"
    PHASE1B = "PHASE_1B"
    PHASE2A = "PHASE_2A"
    PHASE2B = "PHASE_2B"
    CLIENT = "CLIENT_VALUE"

    @staticmethod
    def prepare(c_rnd, instance_id, timestamp):
        return json.dumps({
            "phase": Message.PHASE1A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "instance": instance_id,
            "timestamp": timestamp
        }).encode()

    @staticmethod
    def promise(rnd, v_rnd, v_val, instance_id, timestamp):
        return json.dumps({
            "phase": Message.PHASE1B,
            "rnd_1": rnd[0],
            "rnd_2": rnd[1],
            "v_rnd_1": v_rnd[0] if v_rnd else None,
            "v_rnd_2": v_rnd[1] if v_rnd else None,
            "v_val": v_val,
            "instance": instance_id,
            "timestamp": timestamp
        }).encode()

    @staticmethod
    def accept(c_rnd, c_val, instance_id, timestamp):
        return json.dumps({
            "phase": Message.PHASE2A,
            "c_rnd_1": c_rnd[0],
            "c_rnd_2": c_rnd[1],
            "c_val": c_val,
            "instance": instance_id,
            "timestamp": timestamp
        }).encode()

    @staticmethod
    def decide(v_rnd, v_val, instance_id, timestamp):
        return json.dumps({
            "phase": Message.PHASE2B,
            "v_rnd_1": v_rnd[0],
            "v_rnd_2": v_rnd[1],
            "v_val": v_val,
            "instance": instance_id,
            "timestamp": timestamp
        }).encode()

    @staticmethod
    def client_value(value, timestamp):
        return json.dumps({
            "phase": Message.CLIENT,
            "value": value,
            "timestamp": timestamp
        }).encode()


def mcast_receiver(hostport):
    """Create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)
    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock

def mcast_sender():
    """Create a UDP socket"""
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

def parse_cfg(cfgpath):
    """Parse configuration file"""
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg

"""
State Classes
------------
Classes for managing state of different Paxos roles
"""
class InstanceState:
    def __init__(self):
        self.c_rnd = None
        self.c_val = None
        self.timestamp = None
        self.promises = []
        self.is_decided = False
        self.pending_timer = None

class AcceptorState:
    def __init__(self, id):
        self.rnd = (0, id)
        self.v_rnd = None
        self.v_val = None
        self.timestamp = None

class ProposerState:
    def __init__(self, id):
        self.id = id
        self.c_rnd_counter = (0, id)
        self.current_instance = 0
        self.instances = {}  # instance_id -> InstanceState
        self.decided_values = set()
        self.pending_values = []  # list of values sent by clients

    def increment_round(self):
        self.c_rnd_counter = (self.c_rnd_counter[0] + 1, self.c_rnd_counter[1])
        return self.c_rnd_counter


lock = threading.Lock()
active_acceptors = set()

def get_quorum():
    if len(active_acceptors) < 2:
        return 0
    else:
        return math.ceil(len(active_acceptors) / 2)

def resend_prepare(instance_id, prepare_msg, acceptors, sender):
    sender.sendto(prepare_msg, acceptors)
    print(f"Resending prepare message for instance {instance_id}")

"""
Paxos Roles
-----------
Implementation of each Paxos role
"""
class PaxosAcceptor:
    """Acceptor role implementation"""
    def __init__(self, config, id):
        self.config = config
        self.id = id
        self.instances = defaultdict(lambda: AcceptorState(id))
        self.sender = mcast_sender()

    def handle_prepare(self, msg):
        instance_id = msg["instance"]
        timestamp = msg["timestamp"]
        c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
        
        instance = self.instances[instance_id]
        if c_rnd > instance.rnd:
            instance.rnd = c_rnd
            instance.timestamp = timestamp
            promise = Message.promise(
                instance.rnd,
                instance.v_rnd,
                instance.v_val,
                instance_id,
                timestamp
            )
            self.sender.sendto(promise, self.config["proposers"])

    def handle_accept(self, msg):
        instance_id = msg["instance"]
        timestamp = msg["timestamp"]
        c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
        
        instance = self.instances[instance_id]
        if c_rnd >= instance.rnd:
            instance.v_rnd = c_rnd
            instance.v_val = msg["c_val"]
            instance.timestamp = timestamp
            
            decide_msg = Message.decide(
                instance.v_rnd,
                instance.v_val,
                instance_id,
                timestamp
            )
            self.sender.sendto(decide_msg, self.config["learners"])    
            self.sender.sendto(decide_msg, self.config["proposers"])

class PaxosProposer:
    """Proposer role implementation"""
    def __init__(self, config, id):
        self.config = config
        self.state = ProposerState(id)
        self.sender = mcast_sender()

    def handle_client_message(self, msg):
        self.state.pending_values.append((msg["timestamp"], msg["value"]))
        
        if (not self.state.instances or 
            self.state.instances[self.state.current_instance].is_decided):
            self.start_new_instance()

    def start_new_instance(self):
        if not self.state.pending_values:
            return
            
        self.state.pending_values.sort(key=lambda x: x[0])
        timestamp, value = self.state.pending_values.pop(0)
        
        if value in self.state.decided_values:
            return
            
        instance = InstanceState()
        instance.c_rnd = self.state.increment_round()
        instance.c_val = value
        instance.timestamp = timestamp
        self.state.instances[self.state.current_instance] = instance
        
        prepare_msg = Message.prepare(
            instance.c_rnd,
            self.state.current_instance,
            timestamp
        )
        instance.pending_timer = threading.Timer(
            5.0, resend_prepare, 
            args=(self.state.current_instance, prepare_msg, self.config["acceptors"], self.sender)
        )
        instance.pending_timer.start()
        self.sender.sendto(prepare_msg, self.config["acceptors"])

    def handle_promise(self, msg):
        instance_id = msg["instance"]
        if instance_id not in self.state.instances:
            return
            
        instance = self.state.instances[instance_id]
        rnd = (msg["rnd_1"], msg["rnd_2"])
        
        if rnd != instance.c_rnd:
            return
            
        instance.promises.append(msg)
        
        if len(instance.promises) > get_quorum() and get_quorum() != 0:
            self.handle_quorum_promises(instance, instance_id)

    def handle_quorum_promises(self, instance, instance_id):
        if any(p["v_rnd_1"] is not None and p["v_rnd_2"] is not None for p in instance.promises):
            k = max(
                (p["v_rnd_1"], p["v_rnd_2"]) 
                for p in instance.promises 
                if p["v_rnd_1"] is not None and p["v_rnd_2"] is not None
            )
            if k:
                instance.c_val = next(
                    p["v_val"] for p in instance.promises 
                    if (p["v_rnd_1"], p["v_rnd_2"]) == k
                )
                    
        accept_msg = Message.accept(
            instance.c_rnd,
            instance.c_val,
            instance_id,
            instance.timestamp
        )
        self.sender.sendto(accept_msg, self.config["acceptors"])
        
        instance.promises = []
        if instance.pending_timer:
            instance.pending_timer.cancel()
            instance.pending_timer = None

    def handle_decide(self, msg):
        instance_id = msg["instance"]
        if instance_id in self.state.instances:
            instance = self.state.instances[instance_id]
            instance.is_decided = True
            self.state.decided_values.add(instance.c_val)
            
            if instance.pending_timer:
                instance.pending_timer.cancel()
                instance.pending_timer = None
            
            if self.state.pending_values:
                self.state.current_instance += 1
                self.start_new_instance()

class PaxosLearner:
    """Learner role implementation"""
    def __init__(self):
        self.decisions = []  # List of (timestamp, value) pairs
        self.printed_up_to = 0  # Track the timestamps we've printed
    
    def handle_decision(self, msg):
        self.decisions.append((msg["timestamp"], msg["v_val"]))
        self.decisions.sort(key=lambda x: x[0])
        
        while self.decisions and self.decisions[0][0] > self.printed_up_to:
            timestamp, value = self.decisions.pop(0)
            print(f"[{timestamp}] {value}")
            sys.stdout.flush()
            self.printed_up_to = timestamp

#-------------------------------------------------------------------------------------

def acceptor(config, id):
    print("-> acceptor", id)
    with lock:
        active_acceptors.add(id)
    
    receiver = mcast_receiver(config["acceptors"])
    paxos = PaxosAcceptor(config, id)
    
    try:
        while True:
            msg = json.loads(receiver.recv(2**16).decode())
            if msg["phase"] == Message.PHASE1A:
                paxos.handle_prepare(msg)
            elif msg["phase"] == Message.PHASE2A:
                paxos.handle_accept(msg)
    finally:
        with lock:
            active_acceptors.remove(id)

def proposer(config, id):
    print("-> proposer", id)
    receiver = mcast_receiver(config["proposers"])
    paxos = PaxosProposer(config, id)
    
    while True:
        msg = json.loads(receiver.recv(2**16).decode())
        if msg["phase"] == Message.CLIENT:
            paxos.handle_client_message(msg)
        elif msg["phase"] == Message.PHASE1B:
            paxos.handle_promise(msg)
        elif msg["phase"] == Message.PHASE2B:
            paxos.handle_decide(msg)

def learner(config, id):
    print("-> learner", id)
    receiver = mcast_receiver(config["learners"])
    paxos = PaxosLearner()
    
    while True:
        msg = json.loads(receiver.recv(2**16).decode())
        if msg["phase"] == Message.PHASE2B:
            paxos.handle_decision(msg)

def client(config, id):
    print(f"-> client {id} starting")
    sender = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        timestamp = int(time.time() * 1_000_000)
        client_msg = Message.client_value(value, timestamp)
        sender.sendto(client_msg, config["proposers"])
        time.sleep(0.001)
    
    print(f"Client {id} finished")


if __name__ == "__main__":
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])
    
    roles = {
        "acceptor": acceptor,
        "proposer": proposer,
        "learner": learner,
        "client": client
    }
    
    if role not in roles:
        print(f"Invalid role: {role}")
        sys.exit(1)
        
    roles[role](config, id)