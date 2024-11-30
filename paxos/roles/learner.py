import json
import logging
import sys
import time
from collections import defaultdict
from messages import create_decision_message
from network import mcast_receiver, mcast_sender

def learner(config, id):
    logger = logging.getLogger(f"Learner-{id}")
    logger.info(f"[{id}] started")
    
    r = mcast_receiver(config["learners"])
    r.setblocking(False)
    s = mcast_sender()
    
    # State tracking
    decisions = {}  # instance -> value
    decision_counts = defaultdict(int)
    next_to_print = 0
    requested_instances = set()
    start_time = float("inf")
    CATCHUP_TIMEOUT = 3
    
    # Send initial catchup request
    catchup_request = json.dumps({"type": "CATCHUP"}).encode()
    s.sendto(catchup_request, config["acceptors"])
    logger.info(f"[{id}] Sent initial CATCHUP request")
    
    while True:
        try:
            data = r.recv(2**16)
            msg = json.loads(data.decode())
            msg_type = msg["type"]
            
            if msg_type == "DECISION":
                instance = msg["slot"]
                value = msg["v_val"]
                
                if instance not in decisions:
                    decisions[instance] = value
                    decision_counts[instance] = 1
                else:
                    # Check for conflicting decisions
                    if decisions[instance] != value:
                        logger.error(f"[{id}] Conflicting decisions for instance {instance}: {decisions[instance]} vs {value}")
                    decision_counts[instance] += 1
                
                start_time = time.time()
                
                # Print decisions in order
                while next_to_print in decisions:
                    print(f"{decisions[next_to_print]}")
                    sys.stdout.flush()
                    next_to_print += 1
                
                logger.debug(f"[{id}] Learned decision for instance {instance}: {value}")
            
            elif msg_type == "CATCHUP":
                catchup_data = msg.get("decided", {})
                for slot_str, value in catchup_data.items():
                    instance = int(slot_str)
                    if instance not in decisions:
                        decisions[instance] = value
                        decision_counts[instance] = 1
                    else:
                        if decisions[instance] != value:
                            logger.error(f"[{id}] Conflicting catchup decisions for instance {instance}")
                        decision_counts[instance] += 1
                
                # Print any newly learned decisions
                while next_to_print in decisions:
                    print(f"{decisions[next_to_print]}")
                    sys.stdout.flush()
                    next_to_print += 1
                
                logger.debug(f"[{id}] Processed catchup data for {len(catchup_data)} decisions")
        
        except BlockingIOError:
            # Handle catchup for missing instances
            now = time.time()
            if now - start_time > CATCHUP_TIMEOUT:
                if decisions:
                    max_instance = max(decisions.keys())
                    missing_instances = []
                    
                    # Check for missing instances
                    for instance in range(next_to_print, max_instance + 1):
                        if instance not in decisions and instance not in requested_instances:
                            missing_instances.append(instance)
                    
                    if missing_instances:
                        requested_instances.update(missing_instances)
                        catchup_request = create_decision_message(None, missing_instances[0])
                        s.sendto(catchup_request, config["acceptors"])
                        logger.debug(f"[{id}] Requesting catchup for instances: {missing_instances}")
        
        except Exception as e:
            logger.error(f"[{id}] Error: {e}", exc_info=True)