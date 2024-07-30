#!/usr/bin/env bash
curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select avg(number) from numbers(3000000000)"}'

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

cat tcpkill_output.txt
