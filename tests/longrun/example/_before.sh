#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop table if exists example\"
log_command bendsql --dsn "$DSN"  -q \"create table if not exists example\(id int, message string, time timestamp\)\"
