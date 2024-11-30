import json
import logging
import sys
from collections import defaultdict
from messages import create_decision_message
from network import mcast_receiver, mcast_sender

def learner(config, id):
    logger = logging.getLogger(f"Learner-{id}")
    logger.info(f"Starting learner {id}")
    
    r = mcast_receiver(config["learners"])
    s = mcast_sender()
    decisions = {}  # instance -> [value_tuple, client_id]
    decision_counts = defaultdict(int)  
    next_to_print = 0 
    
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
                while next_to_print in decisions and decision_counts[next_to_print]:
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