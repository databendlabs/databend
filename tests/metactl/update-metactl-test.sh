#!/bin/sh

cargo build

killall databend-meta
killall databend-query

rm -rf _meta*

# Generate sample data with a testing load.
make stateless-cluster-test

killall databend-meta
killall databend-query

sleep 2

# Export all meta data from metasrv dir
./target/debug/databend-metactl --export --raft-dir _meta1 > tests/metactl/meta.json

# Optional: run the test
make metactl-test
