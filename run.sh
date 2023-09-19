#!/bin/bash

for i in {1..100} ; do
  echo "$i"
  ./target/debug/databend-sqllogictests --handlers mysql --run_dir query --run_file join.test --enable_sandbox --debug --parallel 8
done
