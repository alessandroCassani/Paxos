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
    PHASE_TIMEOUT = 0.5  
    INITIAL_TIMEOUT = 0.2
    
    logger.info(f"Operating with {TOTAL_ACCEPTORS} acceptors, quorum size is {QUORUM_SIZE}")
    
    num_proposers = len([k for k in config.keys() if k.startswith('proposer')])
    c_rnd = (id, id)  # Start with proposer id
    next_instance = 0
    current_instances = set()  # Track instances being worked on
    pending_values = []
    promises = defaultdict(list)
    phase2b_msgs = defaultdict(list)
    decided = set()
    last_attempts = defaultdict(float)  # Track last attempt per instance
    phase_start_times = defaultdict(float)  # Track phase start time per instance
    
    def check_liveness(instance):
        if phase_start_times[instance] and time.time() - phase_start_times[instance] > PHASE_TIMEOUT:
            logger.warning(f"Timeout waiting for quorum responses for instance {instance}")
            return False
        return True
    
    def start_phase1(instance):
        if pending_values:
            client_id = pending_values[0][1]
            logger.debug(f"Starting Phase 1 for instance {instance} with c_rnd {c_rnd}")
            phase1a = create_phase1a_message(c_rnd, instance, client_id)
            s.sendto(phase1a, config["acceptors"])
            current_instances.add(instance)
            last_attempts[instance] = time.time()
            phase_start_times[instance] = time.time()
            promises[instance].clear()
            phase2b_msgs[instance].clear()

    def handle_phase2b(msg_instance, msg_v_rnd, value):
        nonlocal next_instance
        if msg_instance in current_instances and msg_v_rnd == c_rnd:
            if isinstance(value, list):
                value = tuple(value)
            phase2b_msgs[msg_instance].append(value)
            
            logger.debug(f"Received {len(phase2b_msgs[msg_instance])}/{QUORUM_SIZE} phase2b messages for instance {msg_instance}")
            
            if len(phase2b_msgs[msg_instance]) >= QUORUM_SIZE and check_liveness(msg_instance):
                value_counts = {}
                for v in phase2b_msgs[msg_instance]:
                    if isinstance(v, list):
                        v = tuple(v)
                    value_counts[v] = value_counts.get(v, 0) + 1
                
                majority_count = max(value_counts.values())
                if majority_count >= QUORUM_SIZE:
                    majority_value = max(
                        (v for v, c in value_counts.items() if c == majority_count)
                    )
                    
                    # Send decision
                    decision = create_decision_message(majority_value, msg_instance)
                    s.sendto(decision, config["learners"])
                    
                    # Remove check for msg_instance not in decided
                    current_instances.remove(msg_instance)
                    if majority_value == pending_values[0]:  # If we decided our own value
                        pending_values.pop(0)
                        if pending_values:  # Still have values to propose
                            start_phase1(next_instance)
                            next_instance += 1
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            if msg_type == "PROPOSE":
                value_tuple = (msg["value"], msg["client_id"])
                pending_values.append(value_tuple)
                if len(current_instances) == 0:
                    start_phase1(next_instance)
                    next_instance += 1
            
            elif msg_type == "PHASE1B":
                msg_instance = msg["slot"]
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                
                if msg_instance in current_instances:
                    if msg_rnd > c_rnd:
                        logger.debug(f"Our round {c_rnd} was rejected, got higher round {msg_rnd}")
                        c_rnd = (msg_rnd[0] + num_proposers, id)  # Increase by num_proposers
                        start_phase1(msg_instance)
                        continue
                    
                    if msg_rnd == c_rnd:
                        v_val = msg["v_val"]
                        if isinstance(v_val, list):
                            v_val = tuple(v_val)
                            
                        promises[msg_instance].append({
                            "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                            "v_val": v_val
                        })
                        
                        if len(promises[msg_instance]) >= QUORUM_SIZE and check_liveness(msg_instance):
                            valid_promises = [p for p in promises[msg_instance] 
                                           if p["v_rnd"] is not None and p["v_val"] is not None]
                            
                            if valid_promises:
                                highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                                c_val = highest_promise["v_val"]
                                logger.debug(f"Found existing value in instance {msg_instance}, must propose: {c_val}")
                            else:
                                c_val = pending_values[0]
                                logger.debug(f"Instance {msg_instance} is empty, proposing our value: {c_val}")
                            
                            client_id = pending_values[0][1]
                            phase2a = create_phase2a_message(c_rnd, c_val, msg_instance, client_id)
                            s.sendto(phase2a, config["acceptors"])
                            phase_start_times[msg_instance] = time.time()
            
            elif msg_type == "PHASE2B":
                msg_instance = msg["slot"]
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                value = msg["v_val"]
                handle_phase2b(msg_instance, msg_v_rnd, value)

            elif msg_type == "DECISION":
                decided_instance = msg["slot"]
                decided_value = msg["v_val"]
                if isinstance(decided_value, list):
                    decided_value = tuple(decided_value)
                
                if decided_instance in current_instances:
                    current_instances.remove(decided_instance)
                if pending_values and decided_value == pending_values[0]:
                    pending_values.pop(0)
                    if pending_values and len(current_instances) == 0:
                        start_phase1(next_instance)
                        next_instance += 1
                
        except BlockingIOError:
            now = time.time()
            for instance in list(current_instances):
                if now - last_attempts[instance] > INITIAL_TIMEOUT and instance not in decided:
                    if check_liveness(instance):
                        c_rnd = (c_rnd[0] + num_proposers, id)
                        start_phase1(instance)
                    else:
                        logger.warning(f"Cannot make progress on instance {instance}: not enough acceptors responding")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)

def learner(config, id):
    logger = logging.getLogger(f"Learner-{id}")
    logger.info(f"Starting learner {id}")
    
    r = mcast_receiver(config["learners"])
    s = mcast_sender()
    decisions = {}  # instance -> [value_tuple, client_id]
    decision_counts = defaultdict(int)  # instance -> count
    next_to_print = 0  # Next instance number to print
    
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
                
                # Store decision
                if instance not in decisions:
                    decisions[instance] = [value_tuple, None]
                    decision_counts[instance] = 1
                else:
                    # If we already have a decision for this instance, check if it's different
                    if decisions[instance][0] != value_tuple:
                        logger.error(f"Conflicting decisions for instance {instance}: {decisions[instance][0]} vs {value_tuple}")
                    decision_counts[instance] += 1
                
                # Print all decisions in order once we have received all proposers' decisions
                while next_to_print in decisions and decision_counts[next_to_print] == config['acceptor_count']:
                    value = decisions[next_to_print][0][0]
                    print(f"{value}")
                    sys.stdout.flush()
                    next_to_print += 1
            
            elif msg["type"] == "CATCHUP":
                decided = msg.get("decided", {})
                for slot, (value_tuple, client_id) in sorted(decided.items(), key=lambda x: int(x[0])):
                    slot = int(slot)
                    if slot not in decisions:
                        decisions[slot] = [value_tuple, client_id]
                        decision_counts[slot] = 1
                    else:
                        # If we already have a decision for this instance, check if it's different
                        if decisions[slot][0] != value_tuple:
                            logger.error(f"Conflicting decisions for instance {slot}: {decisions[slot][0]} vs {value_tuple}")
                        decision_counts[slot] += 1
                        
                # Print all decisions in order after catchup
                while next_to_print in decisions and decision_counts[next_to_print] == config['acceptor_count']:
                    value = decisions[next_to_print][0]
                    print(f"{value}")
                    sys.stdout.flush()
                    next_to_print += 1
            
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