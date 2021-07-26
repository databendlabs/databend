#!/bin/bash

/fuse-store &> /tmp/fuse-store.log  &
P1=$!
/fuse-query &> /tmp/fuse-query.log  &
P2=$!

tail -f /tmp/fuse-query.log &
P3=$!

tail -f /tmp/fuse-store.log & 
P4=$!
wait $P1 $P2 $P3 $P4