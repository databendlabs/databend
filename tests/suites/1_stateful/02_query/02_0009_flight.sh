#!/usr/bin/env bash

# Function to perform the initial query and extract the stats_uri and final_uri
perform_initial_query() {
    local response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select avg(number) from numbers(2000000000)"}')
    local stats_uri=$(echo "$response" | jq -r '.stats_uri')
    local final_uri=$(echo "$response" | jq -r '.final_uri')
    echo "$stats_uri|$final_uri"
}

# Function to poll the stats_uri until state field disappears
poll_stats_uri() {
    local uri=$1
    local state_exists=true
    while $state_exists; do
        local response=$(curl -s -u root: -XGET "http://localhost:8000$uri")
        if ! echo "$response" | jq -e '.state' > /dev/null; then
            state_exists=false
        else
            echo "Still processing... $(date)"
            sleep 2
        fi
    done
}

# Function to get the final state from final_uri
get_final_state() {
    local uri=$1
    local response=$(curl -s -u root: -XGET "http://localhost:8000$uri")
    echo "$response" | jq -r '.state'
}

# Perform the initial query and get the stats_uri and final_uri
IFS='|' read -r stats_uri final_uri <<< $(perform_initial_query)

# Start polling the stats_uri in the background
poll_stats_uri "$stats_uri" &
POLL_PID=$!

sleep 2

netstat_output=$(netstat -an | grep '9092')

port=$(echo "$netstat_output" | awk '
    $NF == "ESTABLISHED" {
        if ($4 ~ /:9092$/) {
            split($5, a, ":")
            port = a[2]
        } else if ($5 ~ /:9092$/) {
            split($4, a, ":")
            port = a[2]
        }
    }
    END {
        print port
    }
')

# Start tcpkill in the background
sudo tcpkill -i lo host 127.0.0.1 and port $port > tcpkill_output.txt 2>&1 &
TCPKILL_PID=$!

# Wait for tcpkill to output at least 3 lines or terminate if done earlier
while [ $(wc -l < tcpkill_output.txt) -lt 3 ]; do
    if ! kill -0 $TCPKILL_PID 2> /dev/null; then
        echo "tcpkill has terminated earlier."
        break
    fi
    sleep 1
done

# Kill tcpkill after the desired number of lines if it's still running
if kill -0 $TCPKILL_PID 2> /dev/null; then
    kill $TCPKILL_PID
    echo "tcpkill was terminated after reaching the desired number of output lines."
fi

# Wait for the polling to complete
wait $POLL_PID

# Get and echo the final state
final_state=$(get_final_state "$final_uri")
echo "Final state: $final_state"

echo "Query monitoring completed."
