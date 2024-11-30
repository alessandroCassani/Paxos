import json
import logging
from collections import defaultdict
from messages import create_phase1b_message, create_phase2b_message
from network import mcast_receiver, mcast_sender

def acceptor(config, id):
    logger = logging.getLogger(f"Acceptor-{id}")
    logger.info(f"[{id}] started")
    
    acceptor_states = defaultdict(lambda: {
        "rnd": (0, 0),        
        "v_rnd": (0, 0),      
        "v_val": None         
    })
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
            
            logger.debug(f"[{id}] Received message: type={msg_type}, instance={instance}, client_id={client_id}")
            
            if msg_type == "PHASE1A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                state = acceptor_states[instance]
                
                logger.debug(f"[{id}] PHASE1A - Current state for instance {instance}: {state}")
                
                # Accept prepare if round number is higher than any we've seen
                if c_rnd > state["rnd"]:
                    logger.debug(f"[{id}] Accepting PHASE1A: c_rnd {c_rnd} > current rnd {state['rnd']}")
                    state["rnd"] = c_rnd
                    phase1b = create_phase1b_message(
                        rnd=state["rnd"],
                        v_rnd=state["v_rnd"] if state["v_val"] is not None else None,
                        v_val=state["v_val"],
                        instance=instance,
                        client_id=client_id
                    )
                    s.sendto(phase1b, config["proposers"])
                else:
                    logger.debug(f"[{id}] Rejecting PHASE1A: c_rnd {c_rnd} <= current rnd {state['rnd']}")
            
            elif msg_type == "PHASE2A":
                c_rnd = (msg["c_rnd_1"], msg["c_rnd_2"])
                c_val = msg["c_val"]
                state = acceptor_states[instance]
                
                logger.debug(f"[{id}] PHASE2A - Current state for instance {instance}: {state}")
                
                # Accept only if round number is >= highest prepare round seen
                if c_rnd >= state["rnd"]:
                    logger.debug(f"[{id}] Accepting PHASE2A: c_rnd {c_rnd} >= current rnd {state['rnd']}")
                    state["rnd"] = c_rnd
                    state["v_rnd"] = c_rnd
                    state["v_val"] = c_val
                    
                    phase2b = create_phase2b_message(
                        v_rnd=state["v_rnd"],
                        v_val=state["v_val"],
                        instance=instance,
                        client_id=client_id
                    )
                    s.sendto(phase2b, config["proposers"])
                else:
                    logger.debug(f"[{id}] Rejecting PHASE2A: c_rnd {c_rnd} < current rnd {state['rnd']}")
            
            elif msg_type == "CATCHUP":
                logger.debug(f"[{id}] Sending CATCHUP response with decisions: {decisions}")
                catchup_response = {
                    "type": "CATCHUP",
                    "decided": decisions
                }
                s.sendto(json.dumps(catchup_response).encode(), config["learners"])
        
        except Exception as e:
            logger.error(f"[{id}] Error: {e}", exc_info=True)