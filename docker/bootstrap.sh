#!/bin/bash

/databend-meta --single true &> /tmp/databend-meta.log  &
P1=$!
/databend-query -c databend-query.toml &> /tmp/databend-query.log  &
P2=$!

tail -f /tmp/databend-query.log &
P3=$!

tail -f /tmp/databend-meta.log &
P4=$!
wait $P1 $P2 $P3 $P4
