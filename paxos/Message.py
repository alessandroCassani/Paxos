import json

class Message:

    PHASE1A = 'Phase1A'
    PHASE1B = 'Phase1B'
    PHASE2A = 'Phase2A'
    PHASE2B = 'Phase2B'
    CLIENT = 'Client'
    DECIDE = 'Decide'

    @staticmethod
    def phase_client(value, phase):
        msg = {
            'value': value,
            'phase': phase
        }
        return json.dumps(msg)

    @staticmethod
    def phase_1a(c_rnd, phase, key):
        msg = {
            'c_rnd': (c_rnd[0],c_rnd[1]),
            'phase': phase,
            'key': (key[0],key[1])
        }
        return json.dumps(msg)

    @staticmethod
    def phase_1b(rnd, v_rnd, v_val, phase, key):
        msg = {
            'rnd': (rnd[0],rnd[1]),         
            'v_rnd': (v_rnd[0],v_rnd[1]),
            'v_val': v_val,
            'phase': phase,
            'key': (key[0],key[1])
        }
        return json.dumps(msg)

    @staticmethod
    def phase_2a(c_rnd, c_val, phase, key):
        msg = {
            'c_rnd': (c_rnd[0],c_rnd[1]),
            'c_val': c_val,
            'phase': phase,
            'key': (key[0],key[1])
        }
        return json.dumps(msg)

    @staticmethod
    def phase_2b(v_rnd, v_val, phase, key):
        msg = {
            'v_rnd': (v_rnd[0],v_rnd[1]),
            'v_val': v_val,
            'phase': phase,
            'key': (key[0],key[1])
        }
        return json.dumps(msg)

    @staticmethod
    def decision(v_val, phase, key):
        msg = {
            'v_val': v_val,
            'phase': phase,
            'key': (key[0],key[1])
        }
        return json.dumps(msg)

    @staticmethod
    def handle_proposer_received_message(data):
        """Handles messages received by the proposer."""
        try:
            msg = json.loads(data)
            if 'value' in msg and 'phase' in msg:
                print("Received client message")
                return Message.CLIENT, msg
        except json.JSONDecodeError as e:
            pass

        try:
            msg = json.loads(data)
            if msg.phase == Message.PHASE1B:
                print("Received Phase1B message")
                return Message.PHASE1B, msg
        except json.JSONDecodeError as e:
            pass

        try:
            msg = json.loads(data)
            if msg.phase == Message.PHASE2B:
                print("Received Phase2B message")
                return Message.PHASE2B, msg
        except json.JSONDecodeError as e:
            pass

        try:
            msg = json.loads(data)
            if msg.phase == Message.DECIDE:
                print("Received Decide message")
                return Message.DECIDE, msg
        except json.JSONDecodeError as e:
            pass

        print("Unknown message type received")
        return "Unknown", None

    @staticmethod
    def handle_acceptor_received_message(data):
        """Handles messages received by the acceptor."""
        print(f"Raw data received: {data}")  # Debugging line to inspect the raw data
        
        try:
            msg = json.loads(data)
            if msg.phase == Message.PHASE1A:
                print("Received Phase1A message")
                return Message.PHASE1A, msg
        except json.JSONDecodeError as e:
            print(f"Failed to parse Phase1A: {e}")
    
        try:
            msg = json.loads(data)
            if msg.phase == Message.PHASE2A:
                print("Received Phase2A message")
                return Message.PHASE2A, msg
        except json.JSONDecodeError as e:
            print(f"Failed to parse Phase2A: {e}")

        print("Unknown message type received from proposer")
        return "Unknown", None