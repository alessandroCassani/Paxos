#!/usr/bin/env bash
n="$1"

if [[ x$n == "x" ]]; then
    echo "Usage: $0 <number of values per proposer>"
    exit 1
fi

# Get the Paxos directory path
PAXOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
conf="$PAXOS_DIR/paxos.conf"
PYTHONPATH="$PAXOS_DIR"

# Change to the Paxos directory
cd "$PAXOS_DIR" || exit 1

pkill -f "$conf"

# Clean up existing files
rm -f learn1 learn2 prop1 prop2 prop.sorted *.sorted 2>/dev/null

echo "Generating test values..."
./generate.sh "$n" > prop1
./generate.sh "$n" > prop2

echo "starting acceptors..."
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" acceptor 1 &
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" acceptor 2 &
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" acceptor 3 &

sleep 1
echo "starting learners..."
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" learner 1 > learn1 &
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" learner 2 > learn2 &

sleep 1
echo "starting proposers..."
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" proposer 1 &
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" proposer 2 &

echo "waiting to start clients"
sleep 10
echo "starting clients..."
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" client 1 < prop1 &
PYTHONPATH="$PAXOS_DIR" python3 "$PAXOS_DIR/paxos/main.py" "$conf" client 2 < prop2 &

sleep 10

pkill -f "$conf"
wait

# Sort outputs for comparison
sort learn1 learn2 prop1 prop2 > prop.sorted 2>/dev/null