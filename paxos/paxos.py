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
    acceptor_count = 0
    with open(cfgpath, "r") as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            if role.startswith('acceptor'):
                acceptor_count += 1
            cfg[role] = (host, int(port))
    cfg['acceptor_count'] = acceptor_count
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
    
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    
    TOTAL_ACCEPTORS = config['acceptor_count']
    QUORUM_SIZE = get_quorum(TOTAL_ACCEPTORS)
    PHASE_TIMEOUT = 5.0  
    INITIAL_TIMEOUT = 2.0
    
    logger.info(f"Operating with {TOTAL_ACCEPTORS} acceptors, quorum size is {QUORUM_SIZE}")
    
    c_rnd = (0, id)
    instance = 0
    pending_values = []
    promises = defaultdict(list)
    phase2b_msgs = defaultdict(list)
    decided = set()
    last_attempt = 0
    phase_start_time = None
    
    def check_liveness():
        nonlocal phase_start_time
        if phase_start_time and time.time() - phase_start_time > PHASE_TIMEOUT:
            logger.warning(f"Timeout waiting for quorum responses. Only received "
                         f"{len(promises[instance])} promises or "
                         f"{len(phase2b_msgs[instance])} phase2b messages. "
                         f"Need {QUORUM_SIZE} out of {TOTAL_ACCEPTORS} acceptors.")
            return False
        return True
    
    def start_phase1():
        nonlocal last_attempt, phase_start_time
        if pending_values:
            client_id = pending_values[0][1]
            logger.debug(f"Starting Phase 1 for instance {instance} with c_rnd {c_rnd}")
            phase1a = create_phase1a_message(c_rnd, instance, client_id)
            s.sendto(phase1a, config["acceptors"])
            last_attempt = time.time()
            phase_start_time = time.time()
            promises[instance].clear()
            phase2b_msgs[instance].clear()
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            if msg_type == "PROPOSE":
                value_tuple = (msg["value"], msg["client_id"])
                pending_values.append(value_tuple)
                if len(pending_values) == 1:
                    start_phase1()
            
            elif msg_type == "PHASE1B":
                msg_instance = msg["slot"]
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                
                # Check if this is a response to our current round
                if msg_instance == instance:
                    # If we got rejected (received round higher than ours)
                    if msg_rnd > c_rnd:
                        logger.debug(f"Our round {c_rnd} was rejected, got higher round {msg_rnd}")
                        c_rnd = (msg_rnd[0] + 1, id)  # Increment higher than the received round
                        start_phase1()  # Retry same instance with higher round
                        continue
                    
                    # If this is a response to our current round
                    if msg_rnd == c_rnd:
                        v_val = msg["v_val"]
                        if isinstance(v_val, list):
                            v_val = tuple(v_val)
                            
                        promises[instance].append({
                            "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                            "v_val": v_val
                        })
                        
                        logger.debug(f"Received {len(promises[instance])}/{QUORUM_SIZE} promises for instance {instance}")
                        
                        if len(promises[instance]) >= QUORUM_SIZE and check_liveness():
                            # Find highest numbered value among responses
                            valid_promises = [p for p in promises[instance] 
                                           if p["v_rnd"] is not None and p["v_val"] is not None]
                            
                            if valid_promises:
                                # Must propose highest numbered value to preserve consensus
                                highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                                c_val = highest_promise["v_val"]
                                logger.debug(f"Found existing value in instance {instance}, must propose: {c_val}")
                            else:
                                # Free to propose our value
                                c_val = pending_values[0]
                                logger.debug(f"Instance {instance} is empty, proposing our value: {c_val}")
                            
                            client_id = pending_values[0][1]
                            phase2a = create_phase2a_message(c_rnd, c_val, instance, client_id)
                            s.sendto(phase2a, config["acceptors"])
                            phase_start_time = time.time()
            
            elif msg_type == "PHASE2B":
                msg_instance = msg["slot"]
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                
                # Check if we got rejected
                if msg_instance == instance and msg_v_rnd > c_rnd:
                    logger.debug(f"Our round {c_rnd} was rejected in phase 2, got higher round {msg_v_rnd}")
                    c_rnd = (msg_v_rnd[0] + 1, id)  # Increment higher than the received round
                    start_phase1()  # Retry same instance with higher round
                    continue
                
                if msg_instance == instance and msg_v_rnd == c_rnd:
                    value = msg["v_val"]
                    if isinstance(value, list):
                        value = tuple(value)
                    phase2b_msgs[instance].append(value)
                    
                    logger.debug(f"Received {len(phase2b_msgs[instance])}/{QUORUM_SIZE} phase2b messages for instance {instance}")
                    
                    if len(phase2b_msgs[instance]) >= QUORUM_SIZE and check_liveness():
                        value_counts = {}
                        for v in phase2b_msgs[instance]:
                            if isinstance(v, list):
                                v = tuple(v)
                            value_counts[v] = value_counts.get(v, 0) + 1
                        
                        majority_count = max(value_counts.values())
                        if majority_count >= QUORUM_SIZE:
                            majority_value = max(
                                (v for v, c in value_counts.items() if c == majority_count)
                            )
                            
                            # Send decision
                            decision = create_decision_message(majority_value, instance)
                            s.sendto(decision, config["learners"])
                            
                            # Only advance to next instance after successfully deciding current one
                            if instance not in decided:
                                decided.add(instance)
                                # Remove our value from pending if it was the one decided
                                if majority_value == pending_values[0]:
                                    pending_values.pop(0)
                                # Move to next instance
                                instance += 1
                                c_rnd = (0, id)  # Reset round for new instance
                                if pending_values:  # If we still have values to propose
                                    start_phase1()
            
            elif msg_type == "DECISION":
                decided_instance = msg["slot"]
                decided_value = msg["v_val"]
                if decided_instance not in decided:
                    decided.add(decided_instance)
                    # If this was our instance and value, remove it from pending
                    if pending_values and decided_value == pending_values[0]:
                        pending_values.pop(0)
                    # Move to next instance if we were working on this one
                    if decided_instance == instance:
                        instance += 1
                        c_rnd = (0, id)
                        if pending_values:
                            start_phase1()
                
        except BlockingIOError:
            # If we haven't heard back in a while, retry with higher round number
            if pending_values and time.time() - last_attempt > INITIAL_TIMEOUT and instance not in decided:
                if check_liveness():
                    c_rnd = (c_rnd[0] + 1, c_rnd[1])
                    start_phase1()  # Retry same instance with higher round
                else:
                    logger.warning(f"Cannot make progress: not enough acceptors responding (need {QUORUM_SIZE} out of {TOTAL_ACCEPTORS})")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)

def learner(config, id):
    logger = logging.getLogger(f"Learner-{id}")
    logger.info(f"Starting learner {id}")
    
    TOTAL_ACCEPTORS = config['acceptor_count']
    QUORUM_SIZE = get_quorum(TOTAL_ACCEPTORS)
    
    logger.info(f"Operating with {TOTAL_ACCEPTORS} acceptors, quorum size is {QUORUM_SIZE}")
    
    r = mcast_receiver(config["learners"])
    s = mcast_sender()
    decisions = defaultdict(dict)  # slot -> (value -> count)
    confirmed_decisions = set()  # Track confirmed decisions
    
    catchup_request = json.dumps({"type": "CATCHUP"}).encode()
    s.sendto(catchup_request, config["acceptors"])
    
    logger.info("Sent initial CATCHUP request")
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            
            if msg["type"] == "DECISION":
                instance = msg["slot"]
                value_tuple = msg["v_val"]
                
                # Count identical decisions
                decisions[instance][str(value_tuple)] = decisions[instance].get(str(value_tuple), 0) + 1
                
                # Only accept and print decision when we have a quorum of identical values
                if instance not in confirmed_decisions:
                    for value, count in decisions[instance].items():
                        if count >= QUORUM_SIZE:
                            print(f"{eval(value)[0]}")
                            sys.stdout.flush()
                            confirmed_decisions.add(instance)
                            break
            
            elif msg["type"] == "CATCHUP":
                decided = msg.get("decided", {})
                for slot, (value_tuple, client_id) in sorted(decided.items(), key=lambda x: int(x[0])):
                    if int(slot) not in confirmed_decisions:
                        decisions[slot][str(value_tuple)] = decisions[slot].get(str(value_tuple), 0) + 1
                        if decisions[slot][str(value_tuple)] >= QUORUM_SIZE:
                            print(f"{value_tuple[0]}")
                            sys.stdout.flush()
                            confirmed_decisions.add(int(slot))
            
        except Exception as e:
            logger.error(f"Learner {id} error: {e}", exc_info=True)

def client(config, id):
    logger = logging.getLogger(f"Client-{id}")
    logger.info(f"Starting client {id}")
    s = mcast_sender()
    
    for value in sys.stdin:
        value = value.strip()
        proposal = create_propose_message(value, id)
        s.sendto(proposal, config["proposers"])
        logger.debug(f"Sent proposal with value: {value}")
        time.sleep(0.001)
    
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