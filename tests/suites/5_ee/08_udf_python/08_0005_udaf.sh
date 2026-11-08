#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_weighted_avg; DROP FUNCTION IF EXISTS python_udaf_probe; DROP FUNCTION IF EXISTS python_udaf_scalar;"
}
trap cleanup EXIT

bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_weighted_avg (INT, INT) STATE {sum INT, weight INT} RETURNS BIGINT
LANGUAGE python AS \$\$
class State:
    def __init__(self):
        self.sum = 0
        self.weight = 0

def create_state():
    return State()

def accumulate(state, value, weight):
    state.sum += value * weight
    state.weight += weight
    return state

def merge(state, other):
    state.sum += other.sum
    state.weight += other.weight
    return state

def finish(state):
    # Integer output keeps the expected result exact.
    return state.sum * 1000 // state.weight
\$\$;
"
bendsql_connect_root --query='SELECT python_weighted_avg(number + 1, number * 2) FROM numbers(10)'
bendsql_connect_root --query='SELECT number % 3, python_weighted_avg(number, 1) FROM numbers(10) GROUP BY 1 ORDER BY 1'
# Partial states are serialized and merged across many groups and blocks.
bendsql_connect_root --query='SELECT count(), sum(a) FROM (SELECT number % 1000 k, python_weighted_avg(number::INT % 4, 1) a FROM numbers(20000) GROUP BY k)'

# Aggregates run in the sandboxed Python workers, not in the query process,
# and can be combined with scalar Python UDFs in one query.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_udaf_probe (INT) STATE {seen INT} RETURNS STRING
LANGUAGE python AS \$\$
import os

class State:
    def __init__(self):
        self.seen = 0

def create_state():
    return State()

def accumulate(state, value):
    state.seen += 1
    return state

def merge(state, other):
    state.seen += other.seen
    return state

def finish(state):
    try:
        open('/etc/passwd').read()
        return 'unsandboxed'
    except PermissionError:
        return 'sandboxed %d' % state.seen
\$\$;
CREATE OR REPLACE FUNCTION python_udaf_scalar (INT) RETURNS INT
LANGUAGE python HANDLER = 'handler' AS \$\$
def handler(x):
    return x + 1
\$\$;
"
bendsql_connect_root --query='SELECT python_udaf_probe(python_udaf_scalar(number::INT)) FROM numbers(5)'
