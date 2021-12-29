#!/bin/bash

/databend-meta --single &> /tmp/databend-meta.log  &
P1=$!
# add health check to remove the race condition issue during databend-query bootstrap
sleep 1
/databend-query -c databend-query.toml &> /tmp/databend-query.log  &
P2=$!

tail -f /tmp/databend-query.log &
P3=$!

tail -f /tmp/databend-meta.log &
P4=$!
wait $P1 $P2 $P3 $P4
