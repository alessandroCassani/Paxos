import json
import logging
import time
import math
import random
from collections import defaultdict
from messages import create_phase1a_message, create_phase2a_message, create_decision_message
from network import mcast_receiver, mcast_sender, get_quorum

def proposer(config, id):
    logger = logging.getLogger(f"Proposer-{id}")
    logger.info(f"[{id}] started")
    
    r = mcast_receiver(config["proposers"])
    r.setblocking(False)
    s = mcast_sender()
    
    TOTAL_ACCEPTORS = config['acceptor_count']
    QUORUM_SIZE = math.ceil(TOTAL_ACCEPTORS / 2)
    
    # State tracking
    rnd_counter = 0
    next_instance = 0  # Track next available instance
    c_rnd = {}  # Map instance to round number
    c_val = {}  # Map instance to proposed value
    promises = defaultdict(list)  # Map instance to list of promises
    accepts = defaultdict(list)  # Map instance to list of accepts
    pending = {}  # Map instance to timestamp
    
    def start_phase1(instance, value):
        nonlocal rnd_counter
        rnd_counter += 1
        new_rnd = (rnd_counter, id)
        c_rnd[instance] = new_rnd
        c_val[instance] = value
        
        logger.debug(f"[{id}] Starting Phase 1 for instance {instance} with c_rnd {new_rnd}")
        phase1a = create_phase1a_message(c_rnd=new_rnd, instance=instance, client_id=id)
        s.sendto(phase1a, config["acceptors"])
        pending[instance] = time.time()
        promises[instance].clear()
        accepts[instance].clear()
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            if msg_type == "PROPOSE":
                value = msg["value"]
                client_id = msg["client_id"]
                # Use next available instance
                instance = next_instance
                next_instance += 1
                start_phase1(instance, value)
            
            elif msg_type == "PHASE1B":
                instance = msg["slot"]
                msg_rnd = (msg["rnd_1"], msg["rnd_2"])
                
                if instance in c_rnd and msg_rnd == c_rnd[instance]:
                    promises[instance].append({
                        "v_rnd": (msg["v_rnd_1"], msg["v_rnd_2"]) if msg["v_rnd_1"] is not None else None,
                        "v_val": msg["v_val"]
                    })
                    
                    if len(promises[instance]) >= QUORUM_SIZE:
                        # Find highest round among promises
                        valid_promises = [p for p in promises[instance] if p["v_rnd"] is not None]
                        if valid_promises:
                            highest_promise = max(valid_promises, key=lambda p: p["v_rnd"])
                            c_val[instance] = highest_promise["v_val"]
                        
                        logger.debug(f"[{id}] instance {instance} Received sufficient PHASE1B, proposing value: {c_val[instance]}")
                        phase2a = create_phase2a_message(
                            c_rnd=c_rnd[instance],
                            c_val=c_val[instance],
                            instance=instance,
                            client_id=id
                        )
                        s.sendto(phase2a, config["acceptors"])

            elif msg_type == "PHASE2B":
                instance = msg["slot"]
                msg_v_rnd = (msg["v_rnd_1"], msg["v_rnd_2"])
                value = msg["v_val"]
                
                if instance in c_rnd and msg_v_rnd == c_rnd[instance]:
                    accepts[instance].append(value)
                    
                    if len(accepts[instance]) >= QUORUM_SIZE:
                        # Send decision to learners
                        decision = create_decision_message(c_val[instance], instance)
                        s.sendto(decision, config["learners"])
                        
                        # Cleanup state for this instance
                        del pending[instance]
                        del c_rnd[instance]
                        del c_val[instance]
                        promises[instance].clear()
                        accepts[instance].clear()
        
        except BlockingIOError:
            # Handle timeouts and retries
            now = time.time()
            for instance in list(pending.keys()):
                elapsed_time = now - pending[instance]
                if elapsed_time > random.randint(1, 3):
                    start_phase1(instance, c_val[instance])
        
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)