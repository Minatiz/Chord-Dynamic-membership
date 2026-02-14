#!/bin/bash

# Check if nodes.txt exists
if [ ! -f nodes.txt ]; then
  echo "nodes.txt not found! Please run the server start script first."
  exit 1
fi

# Read the node addresses from the file
NODES=$(cat nodes.txt)
IFS=' ' read -r -a NODES_ARRAY <<< "$NODES"

# Loop through each node and perform the curl request
for NODE in "${NODES_ARRAY[@]}"; do
  HOST="${NODE%%:*}"
  PORT="${NODE##*:}"
  
  # Fetch the node info using curl
  curl "http://$HOST:$PORT/node-info"
  echo
done
