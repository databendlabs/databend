#!/bin/bash

set -e -x -a
sudo nohup /datafuse-query &

sleep 5

function run_tests()
{
  cd tests
  mkdir -p test_output
  ./fuse-test --print-time  --jobs 4   
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests
