import json
import logging
import time
from collections import defaultdict
from messages import create_phase1a_message, create_phase2a_message, create_decision_message
from network import mcast_receiver, mcast_sender, get_quorum

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