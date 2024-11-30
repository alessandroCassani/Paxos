#!/usr/bin/env python3
import sys
import logging
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from network import parse_cfg
from roles.acceptor import acceptor
from roles.proposer import proposer
from roles.learner import learner
from roles.client import client

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

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