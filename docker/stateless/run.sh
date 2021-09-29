#!/bin/bash

set -e -x -a
sudo nohup /databend-query &

sleep 5

function run_tests()
{
  cd tests
  mkdir -p test_output
  ./databend-test --print-time  --jobs 4  --skip '^09_*'
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests
