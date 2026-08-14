#!/bin/sh
set -e

echo "Starting the JKVS database..."
java -jar jkvs-server.jar &

sleep 2
echo "Running fsm"
./fsm --config cluster-config.toml
