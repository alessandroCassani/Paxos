import json
import logging

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