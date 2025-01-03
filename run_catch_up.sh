#!/usr/bin/env bash

projdir="$1"
conf="$(pwd)/paxos.conf"
n="$2"

if [[ x$projdir == "x" || x$n == "x" ]]; then
    echo "Usage: $0 <project dir> <number of values per proposer>"
    exit 1
fi

pkill -f "$conf"

cd $projdir

../generate.sh $n >../prop1
../generate.sh $n >../prop2

echo "starting acceptors..."

./acceptor.sh 1 "$conf" &
./acceptor.sh 2 "$conf" &
./acceptor.sh 3 "$conf" &

sleep 1
echo "starting learner 1..."

./learner.sh 1 "$conf" >../learn1 &

sleep 1
echo "starting proposers..."

./proposer.sh 1 "$conf" &
./proposer.sh 2 "$conf" &

echo "waiting to start clients"
sleep 10
echo "starting client 1..."

./client.sh 1 "$conf" <../prop1 &

sleep 1
echo "starting learners 2..."
./learner.sh 2 "$conf" >../learn2 &

sleep 1
echo "starting client 2..."
./client.sh 2 "$conf" <../prop2 &

sleep 5

pkill -f "$conf"
wait

cd ..
