#!/usr/bin/env bash

projdir="$1"
conf="$(pwd)/paxos.conf"

if [[ -z "$projdir" ]]; then
    echo "Usage: $0 <project directory>"
    exit 1
fi

cd "$projdir" || exit 1

pkill -f "$conf"
wait

../generate.sh 100 > prop1.txt
../generate.sh 100 > prop2.txt

echo "Starting acceptors..."
./acceptor.sh 1 "$conf" &
./acceptor.sh 2 "$conf" &
./acceptor.sh 3 "$conf" &
sleep 1

echo "Starting the first learner..."
./learner.sh 1 "$conf" >../learn1 &
sleep 1

echo "Starting the proposer..."
./proposer.sh 1 "$conf" &
sleep 1

echo "Client proposing 10 values..."
./client.sh 1 "$conf" < prop1.txt &
sleep 10

echo "Starting the second learner..."
./learner.sh 2 "$conf" >../learn2 &
sleep 1

echo "Adding another client to propose another 10 values..."
./client.sh 2 "$conf" < prop2.txt &
sleep 10

echo "Waiting for learners to complete..."
sleep 5

echo "Stopping all processes..."
pkill -f "$conf"
wait

