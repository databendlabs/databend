#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="CREATE DATABASE test_report;" > /dev/null 2>&1
$BENDSQL_CLIENT_CONNECT --query="CREATE TABLE test_report.test_report_1(text String);" > /dev/null 2>&1
$BENDSQL_CLIENT_CONNECT --query="INSERT INTO test_report.test_report_1 VALUES('1234');" > /dev/null 2>&1
output=$(echo "REPORT ISSUE SELECT text FROM test_report.test_report_1" | $BENDSQL_CLIENT_CONNECT)

if echo "$output" | grep -q "Obfuscated Databases"; then
    echo "Obfuscated Databases"
else

if echo "$output" | grep -q "Obfuscated Tables"; then
    echo "Obfuscated Tables"
else

if echo "$output" | grep -q "Obfuscated Table Statistics"; then
    echo "Obfuscated Table Statistics"
else

if echo "$output" | grep -q "Obfuscated Queries"; then
    echo "Obfuscated Queries"
else

if echo "$output" | grep -q "## Logs"; then
    echo "## Logs"
else
