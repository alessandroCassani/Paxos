#!/usr/bin/env python3
import sys
import socket
import struct
import json
import math
import time
import random
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

def create_phase1a_message(c_rnd, instance, client_id):
    msg = json.dumps({
        "type": "PHASE1A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "slot": instance,
        "client_id": client_id
    }).encode()
    logging.debug(f"Created PHASE1A message: c_rnd={c_rnd}, instance={instance}, client_id={client_id}")
    return msg

def create_phase1b_message(rnd, v_rnd, v_val, instance, client_id):
    msg = json.dumps({
        "type": "PHASE1B",
        "rnd_1": rnd[0],
        "rnd_2": rnd[1],
        "v_rnd_1": v_rnd[0] if v_rnd else None,
        "v_rnd_2": v_rnd[1] if v_rnd else None,
        "v_val": v_val,
        "slot": instance,
        "client_id": client_id
    }).encode()
    logging.debug(f"Created PHASE1B message: rnd={rnd}, v_rnd={v_rnd}, v_val={v_val}, instance={instance}")
    return msg

def create_phase2a_message(c_rnd, c_val, instance, client_id):
    msg = json.dumps({
        "type": "PHASE2A",
        "c_rnd_1": c_rnd[0],
        "c_rnd_2": c_rnd[1],
        "c_val": c_val,
        "client_id": client_id,
        "slot": instance
    }).encode()
    logging.debug(f"Created PHASE2A message: c_rnd={c_rnd}, c_val={c_val}, instance={instance}")
    return msg

def create_phase2b_message(v_rnd, v_val, instance, client_id):
    msg = json.dumps({
        "type": "PHASE2B",
        "v_rnd_1": v_rnd[0],
        "v_rnd_2": v_rnd[1],
        "v_val": v_val,
        "slot": instance,
        "client_id": client_id,
    }).encode()
    logging.debug(f"Created PHASE2B message: v_rnd={v_rnd}, v_val={v_val}, instance={instance}")
    return msg

def create_decision_message(v_val, instance):
    msg = json.dumps({
        "type": "DECISION",
        "v_val": v_val,
        "slot": instance
    }).encode()
    logging.debug(f"Created DECISION message: v_val={v_val}, instance={instance}")
    return msg

def create_propose_message(value, client_id):
    msg = json.dumps({
        "type": "PROPOSE",
        "value": value,
        "client_id": client_id
    }).encode()
    logging.debug(f"Created PROPOSE message: value={value}, client_id={client_id}")
    return msg

def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    logging.debug(f"Parsed config: {cfg}")
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
    logger = logging.getLogger(f"Acceptor-{id}")
    logger.info(f"Starting acceptor {id}")
    
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
            
            logger.debug(f"Received message: type={msg_type}, instance={instance}, client_id={client_id}")

            if msg_type == "PHASE1A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                state = acceptor_states[instance]
                logger.debug(f"PHASE1A - Current state for instance {instance}: {state}")
                
                if c_rnd > state["rnd"]:
                    logger.debug(f"Accepting PHASE1A: c_rnd {c_rnd} > current rnd {state['rnd']}")
                    state["rnd"] = c_rnd
                    phase1b = create_phase1b_message(state["rnd"], state["v_rnd"], state["v_val"], instance, client_id)
                    s.sendto(phase1b, config["proposers"])
                else:
                    logger.debug(f"Rejecting PHASE1A: c_rnd {c_rnd} <= current rnd {state['rnd']}")

            elif msg_type == "PHASE2A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                c_val = msg["c_val"]
                state = acceptor_states[instance]
                logger.debug(f"PHASE2A - Current state for instance {instance}: {state}")
                
                if c_rnd >= state["rnd"]:
                    logger.debug(f"Accepting PHASE2A: c_rnd {c_rnd} >= current rnd {state['rnd']}")
                    state["v_rnd"] = c_rnd
                    state["v_val"] = c_val
                    phase2b = create_phase2b_message(state["v_rnd"], state["v_val"], instance, client_id)
                    s.sendto(phase2b, config["proposers"])
                    decisions[instance] = (c_val, client_id)
                else:
                    logger.debug(f"Rejecting PHASE2A: c_rnd {c_rnd} < current rnd {state['rnd']}")

            elif msg_type == "CATCHUP":
                logger.debug(f"Sending CATCHUP response with decisions: {decisions}")
                catchup_response = {
                    "type": "CATCHUP",
                    "decided": decisions
                }
                s.sendto(json.dumps(catchup_response).encode(), config["learners"])

        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)

def proposer(config, id):
    logger = logging.getLogger(f"Proposer-{id}")
    logger.info(f"Starting proposer {id}")
    
    # Add random startup delay to improve concurrency
    time.sleep(random.uniform(0, 0.1))
    
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    
    c_rnd = (0, id)  # Initial round number unique to this proposer
    instance = 0     # Current instance number
    pending_values = []  # Values waiting to be proposed (value, client_id)
    promises = defaultdict(list)  # Phase 1B promises received
    phase2b_msgs = defaultdict(list)  # Phase 2B messages received
    decided = set()  # Instances that have been decided
    instance_attempts = defaultdict(int)  # Number of attempts per instance
    last_attempt = 0  # Timestamp of last attempt
    
    INITIAL_TIMEOUT = 1.0
    MAX_TIMEOUT = 4.0
    
    def get_timeout(instance):
        # Exponential backoff based on number of attempts
        attempts = instance_attempts[instance]
        return min(INITIAL_TIMEOUT * (1.5 ** attempts), MAX_TIMEOUT)
    
    def start_phase1():
        nonlocal last_attempt
        if pending_values:  # Make sure we have values to propose
            client_id = pending_values[0][1]  # Extract client_id from pending value
            logger.debug(f"Starting Phase 1 for instance {instance} with c_rnd {c_rnd}")
            phase1a = create_phase1a_message(c_rnd, instance, client_id)
            s.sendto(phase1a, config["acceptors"])
            last_attempt = time.time()
            instance_attempts[instance] += 1
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            logger.debug(f"Received message: type={msg_type}")
            
            if msg_type == "PROPOSE":
                value_tuple = (msg["value"], msg["client_id"])
                logger.debug(f"Received PROPOSE: value={value_tuple}")
                
                if not pending_values:
                    logger.debug("No pending values, starting Phase 1")
                    pending_values.append(value_tuple)
                    start_phase1()
                else:
                    logger.debug(f"Adding to pending values: {value_tuple}")
                    pending_values.append(value_tuple)
                    
            elif msg_type == "PHASE1B":
                msg_instance = msg["slot"]
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                logger.debug(f"Received PHASE1B for instance {msg_instance}, rnd={msg_rnd}")
                
                if msg_rnd == c_rnd and msg_instance == instance:
                    v_val = msg["v_val"]
                    if isinstance(v_val, list):
                        v_val = tuple(v_val)
                        
                    promises[instance].append({
                        "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                        "v_val": v_val
                    })
                    
                    logger.debug(f"Promises for instance {instance}: {len(promises[instance])}/{get_quorum(3)}")
                    
                    if len(promises[instance]) >= get_quorum(3):
                        valid_promises = [p for p in promises[instance] if p["v_rnd"] is not None and p["v_val"] is not None]
                        logger.debug(f"Reached quorum. Valid promises: {valid_promises}")
                        
                        if valid_promises:
                            # Must propose the highest numbered value
                            highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                            c_val = highest_promise["v_val"]
                            logger.debug(f"Using highest promise value: {c_val}")
                        else:
                            # Free to propose our value
                            if pending_values:
                                c_val = pending_values[0]
                                logger.debug(f"No valid promises, proposing new value: {c_val}")
                            
                        if pending_values:  # Make sure we still have pending values
                            client_id = pending_values[0][1]  # Get client_id from pending value
                            phase2a = create_phase2a_message(c_rnd, c_val, instance, client_id)
                            s.sendto(phase2a, config["acceptors"])
                            promises[instance].clear()
                        
            elif msg_type == "PHASE2B":
                msg_instance = msg["slot"]
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                logger.debug(f"Received PHASE2B for instance {msg_instance}, v_rnd={msg_v_rnd}")
                
                if msg_v_rnd == c_rnd and msg_instance == instance:
                    value = msg["v_val"]
                    if isinstance(value, list):
                        value = tuple(value)
                    phase2b_msgs[instance].append(value)
                    
                    logger.debug(f"Phase2B messages for instance {instance}: {len(phase2b_msgs[instance])}/{get_quorum(3)}")
                    
                    if len(phase2b_msgs[instance]) >= get_quorum(3):
                        value_counts = {}
                        for v in phase2b_msgs[instance]:
                            if isinstance(v, list):
                                v = tuple(v)
                            value_counts[v] = value_counts.get(v, 0) + 1
                            
                        majority_value = max(value_counts.items(), key=lambda x: x[1])[0]
                        logger.debug(f"Reached quorum with majority value: {majority_value}")
                        
                        if instance not in decided:
                            decided.add(instance)
                            decision = create_decision_message(majority_value, instance)
                            s.sendto(decision, config["learners"])
                            logger.debug(f"Sent decision for instance {instance}")
                            
                            # Clear state for this instance
                            promises[instance].clear()
                            phase2b_msgs[instance].clear()
                            
                            # Remove value if it was ours
                            if majority_value == pending_values[0]:
                                pending_values.pop(0)
                                logger.debug("Removed decided value from pending values")
                            
                            # Move to next instance
                            instance += 1
                            c_rnd = (0, id)  # Reset round number for new instance
                            logger.debug(f"Moving to instance {instance}")
                            
                            # Start next instance if we have more values
                            if pending_values:
                                start_phase1()
            
            elif msg_type == "DECISION":
                decided_instance = msg["slot"]
                logger.debug(f"Received DECISION for instance {decided_instance}")
                if decided_instance >= instance:
                    instance = decided_instance + 1
                    logger.debug(f"Updated instance to {instance}")
                    if pending_values:
                        c_rnd = (0, id)
                        start_phase1()
                                
        except BlockingIOError:
            # Handle timeouts with exponential backoff
            current_timeout = get_timeout(instance)
            if pending_values and time.time() - last_attempt > current_timeout and instance not in decided:
                logger.debug(f"Timeout occurred. Incrementing c_rnd for instance {instance}")
                c_rnd = (c_rnd[0] + 1, c_rnd[1])
                start_phase1()
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)

def learner(config, id):
    logger = logging.getLogger(f"Learner-{id}")
    logger.info(f"Starting learner {id}")
    
    r = mcast_receiver(config["learners"])
    s = mcast_sender()
    catchup_request = json.dumps({"type": "CATCHUP"}).encode()
    s.sendto(catchup_request, config["acceptors"])
    printed_values = set()
    decisions = {}

    logger.info("Sent initial CATCHUP request")

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
            logger.error(f"Learner {id} error: {e}")

def client(config, id):
    logger = logging.getLogger(f"Client-{id}")
    logger.info(f"Starting client {id}")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        proposal = create_propose_message(value, id)
        s.sendto(proposal, config["proposers"])
        logger.debug(f"Sent proposal with value: {value}")
    
    logger.info(f"Client {id} finished")

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