#!/bin/sh
set -e

echo "Starting the JKVS database..."
java -jar jkvs-server.jar &

# TODO: bit of a temporary hack here impl retrials for fsm to connected to jkvs
sleep 2
echo "Running fsm"
./fsm --config cluster-config.toml
