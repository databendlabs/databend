#!/bin/bash

/datafuse-store --single true &> /tmp/datafuse-store.log  &
P1=$!
/datafuse-query -c datafuse-query.toml &> /tmp/datafuse-query.log  &
P2=$!

tail -f /tmp/datafuse-query.log &
P3=$!

tail -f /tmp/datafuse-store.log & 
P4=$!
wait $P1 $P2 $P3 $P4
