#!/bin/bash

/datafuse-store --single true &> /tmp/datafuse-store.log  &
P1=$!
/databend-query -c databend-query.toml &> /tmp/databend-query.log  &
P2=$!

tail -f /tmp/databend-query.log &
P3=$!

tail -f /tmp/datafuse-store.log & 
P4=$!
wait $P1 $P2 $P3 $P4
