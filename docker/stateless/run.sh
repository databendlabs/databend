#!/bin/bash

set -e -x -a
sudo nohup /fuse-query &

sleep 5

function run_tests()
{
  mkdir -p test_output
  cd tests
  ./fuse-test --print-time  --jobs 4  2>&1 \
      | tee -a test_output/test_result.txt
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests ||:
