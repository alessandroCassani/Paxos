#!/usr/bin/env python3
import sys
import socket
import struct
import json
import math
from enum import Enum


class Message(Enum):
    PREPARE = "PHASE_1A"
    PROMISE = "PHASE_1B"
    ACCEPT_REQUEST = "PHASE_2A"
    DECIDE = "PHASE_3"
    CLIENT_VALUE = "CLIENT_VALUE"


def encode_json_msg(type, **kwargs):
    """Encode the message into JSON format"""
    return json.dumps({"type": type.value, **kwargs}).encode()


def decode_json_msg(msg):
    """Decode the JSON message and convert relevant fields"""
    parsed_msg = json.loads(msg.decode())
    parsed_msg["type"] = Message(parsed_msg["type"])
    parsed_msg = {key: tuple(val) if isinstance(val, list) else val for key, val in parsed_msg.items()}
    return parsed_msg


def create_mcast_socket(hostport, join_group=False):
    """Create and configure a multicast socket for receiving or sending messages"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    
    if join_group:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(hostport)
        mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)

    return sock


def parse_cfg(cfgpath):
    """Parse the configuration file for host-port mappings"""
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            role, host, port = line.split()
            cfg[role] = (host, int(port))
    return cfg


def handle_prepare_message(msg, states, seq, s, config, id):
    """Handle the 'PREPARE' message type"""
    if seq not in states:
        states[seq] = {"rnd": (0, id), "v_rnd": None, "v_val": None}
    if msg["c_rnd"] > states[seq]["rnd"]:
        states[seq]["rnd"] = msg["c_rnd"]
        promise_msg = encode_json_msg(
            Message.PROMISE,
            seq=seq,
            rnd=states[seq]["rnd"],
            v_rnd=states[seq]["v_rnd"],
            v_val=states[seq]["v_val"],
        )
        print(f"-> acceptor {id} Received: seq {seq} {Message.PREPARE} Rnd: {states[seq]['rnd']}")
        s.sendto(promise_msg, config["proposers"])


def handle_accept_request(msg, states, seq, s, config, id):
    """Handle the 'ACCEPT_REQUEST' message type"""
    if msg["c_rnd"] >= states[seq]["rnd"]:
        states[seq]["v_rnd"] = msg["c_rnd"]
        states[seq]["v_val"] = msg["c_val"]
        accepted_msg = encode_json_msg(
            Message.DECIDE,
            seq=seq,
            v_rnd=states[seq]["v_rnd"],
            v_val=states[seq]["v_val"],
        )
        print(f"-> acceptor {id} seq {seq} Received: {Message.ACCEPT_REQUEST} v_rnd: {states[seq]['v_rnd']} v_val: {states[seq]['v_val']}")
        s.sendto(accepted_msg, config["learners"])


def acceptor(config, id):
    """Acceptor role in the Paxos protocol"""
    print("-> acceptor", id)
    states = {}
    r = create_mcast_socket(config["acceptors"], join_group=True)
    s = create_mcast_socket(config["proposers"])

    while True:
        msg = decode_json_msg(r.recv(2**16))
        seq = msg["seq"]
        if msg["type"] == Message.PREPARE:
            handle_prepare_message(msg, states, seq, s, config, id)
        elif msg["type"] == Message.ACCEPT_REQUEST:
            handle_accept_request(msg, states, seq, s, config, id)


def handle_client_value(msg, seq, c_rnd_cnt, c_rnd, c_val, s, config, id):
    """Handle the 'CLIENT_VALUE' message type"""
    seq = (msg["prop_id"], msg["client_id"])
    c_rnd_cnt = (c_rnd_cnt[0] + 1, c_rnd_cnt[1])
    c_rnd[seq] = c_rnd_cnt
    c_val[seq] = msg["value"]
    prepare_msg = encode_json_msg(
        Message.PREPARE, c_rnd=c_rnd[seq], seq=seq
    )
    print(f"-> proposer {id} Received: {Message.CLIENT_VALUE} c_rnd: {c_rnd[seq]} seq, {seq}")
    s.sendto(prepare_msg, config["acceptors"])


def handle_promise_message(msg, promises, seq, c_rnd, c_val, s, config, id):
    """Handle the 'PROMISE' message type"""
    if msg["rnd"] == c_rnd[msg["seq"]]:
        seq = msg["seq"]
        promises.setdefault(seq, []).append(msg)

        print(f"-> proposer {id} seq {seq} promises count: {len(promises[seq])} for c_rnd {c_rnd[seq]}")

        if len(promises[seq]) > math.ceil(3 / 2):
            k = max((p["v_rnd"] for p in promises[seq] if p["v_rnd"]), default=None)
            if k:
                c_val[seq] = next(p["v_val"] for p in promises[seq] if p["v_rnd"] == k)
            accept_msg = encode_json_msg(
                Message.ACCEPT_REQUEST,
                seq=seq,
                c_rnd=c_rnd[seq],
                c_val=c_val[seq],
            )
            s.sendto(accept_msg, config["acceptors"])


def proposer(config, id):
    """Proposer role in the Paxos protocol"""
    print("-> proposer", id)
    r = create_mcast_socket(config["proposers"], join_group=True)
    s = create_mcast_socket(config["acceptors"])

    seq = (0, 0)
    c_rnd_cnt = (0, id)
    c_rnd = {}
    c_val = {}
    promises = {}

    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == Message.CLIENT_VALUE:
            handle_client_value(msg, seq, c_rnd_cnt, c_rnd, c_val, s, config, id)
        elif msg["type"] == Message.PROMISE:
            handle_promise_message(msg, promises, seq, c_rnd, c_val, s, config, id)


def learner(config, id):
    """Learner role in the Paxos protocol"""
    r = create_mcast_socket(config["learners"], join_group=True)
    AB_val = {}
    
    while True:
        msg = decode_json_msg(r.recv(2**16))
        if msg["type"] == Message.DECIDE:
            AB_val[msg["seq"]] = msg["v_val"]
            print(f"Learner {id}: Learned about {msg['v_val']} in rnd {msg['v_rnd']} with seq: {msg['seq']}")
        sys.stdout.flush()


def client(config, id):
    """Client role that sends values to proposers"""
    print("-> client", id)
    s = create_mcast_socket(config["proposers"])

    prop_id = 0
    for value in sys.stdin:
        value = value.strip()
        prop_id += 1
        print(f"client {id}: sending {value} to proposers with prop_id {prop_id}")
        client_msg = encode_json_msg(
            Message.CLIENT_VALUE, value=value, client_id=id, prop_id=prop_id
        )
        s.sendto(client_msg, config["proposers"])
    print("client done.")


def main():
    """Main function to run the appropriate Paxos role"""
    cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    role = sys.argv[2]
    id = int(sys.argv[3])

    role_map = {
        "acceptor": acceptor,
        "proposer": proposer,
        "learner": learner,
        "client": client
    }

    if role not in role_map:
        print(f"Invalid role: {role}")
        sys.exit(1)

    role_map[role](config, id)


if __name__ == "__main__":
    main()
