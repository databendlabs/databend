#!/usr/bin/env bash
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "explain analyze graphical select 1"}')

data=$(echo $response | jq -r '.data')

json_string=$(echo "$data" | jq -r '.[][]')
profiles=$(echo "$json_string" | jq -r '.profiles')

profile_count=$(echo "$profiles" | jq length)
# Check the number of profiles
echo $profile_count

# Initialize memory_usage, error_count, cpu_time
memory_usage=0
error_count=0
cpu_time=0

# Loop through profiles and calculate statistics
for i in $(seq 0 $((profile_count - 1))); do
    profile=$(echo "$profiles" | jq ".[$i]")
    statistics=$(echo "$profile" | jq '.statistics')
    errors=$(echo "$profile" | jq '.errors')

    # Check if statistics has enough data (17 elements)
    if [ "$(echo "$statistics" | jq length)" -ge 17 ]; then
        memory_usage=$((memory_usage + $(echo "$statistics" | jq '.[16]')))
        cpu_time=$((cpu_time + $(echo "$statistics" | jq '.[0]')))
    fi
    

    # Count errors
    error_count=$((error_count + $(echo "$errors" | jq length)))
done


echo $memory_usage
echo "$( [ "$cpu_time" -gt 0 ] && echo true || echo false )"
echo $error_count

