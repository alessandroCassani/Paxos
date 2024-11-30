import json
import logging
import sys
import time
from messages import create_propose_message
from network import mcast_sender

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