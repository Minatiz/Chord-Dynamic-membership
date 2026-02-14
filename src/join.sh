#!/bin/bash

# Check if nodes.txt exists
if [ ! -f nodes.txt ]; then
  echo "nodes.txt not found! Please run the server start script first."
  exit 1
fi

# Read the node addresses from the file
NODES=$(cat nodes.txt)
IFS=' ' read -r -a NODES_ARRAY <<< "$NODES"

# Run test.py with all node addresses
python3 join_test.py "${NODES_ARRAY[@]}"