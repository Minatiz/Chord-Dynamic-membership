#!/bin/sh

# Check if the number of servers to start is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <number_of_servers>"
    exit 1
fi

NUM_SERVERS=$1

# Get the list of available nodes
NODES=$(sh /share/ifi/available-nodes.sh)
NODES_ARRAY=($NODES)
NUM_NODES=${#NODES_ARRAY[@]}

# Function to generate a random port number between 49152 and 65535
generate_random_port() {
    shuf -i 49152-65535 -n 1
}

# Prepare the host:port combinations first
HOST_PORTS=()
for ((i=0; i<NUM_SERVERS; i++)); do
    NODE_INDEX=$((i % NUM_NODES))
    NODE=${NODES_ARRAY[$NODE_INDEX]}
    PORT=$(generate_random_port)
    HOST_PORT="$NODE:$PORT"
    HOST_PORTS+=("$HOST_PORT")
done

# Now deploy each server, only passing the host:port of the current node
for HOST_PORT in "${HOST_PORTS[@]}"; do
    # Start the server on the current node with just the current node's address
    ssh -f "${HOST_PORT%%:*}" "python3 $PWD/main.py $HOST_PORT"
done

# Sleep  seconds before printing out the servers it connected to
sleep 10

# Output the host-port combinations in JSON format
echo "Servers started on: ${HOST_PORTS[*]}" | sed 's/ / /g'
echo "'[\"${HOST_PORTS[*]}\"]'" | sed 's/ /", "/g'

# Save the node addresses (host:port) to a file for use in the second script
echo "${HOST_PORTS[*]}" > nodes.txt
