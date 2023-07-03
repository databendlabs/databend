#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop table if exists t\"
log_command bendsql --dsn "$DSN"  -q \"drop table if exists del_id\"
log_command bendsql --dsn "$DSN"  -q \"create table t\(id int, c1 int, c2 int\) row_per_block=3\"
log_command bendsql --dsn "$DSN"  -q \"insert into t select number, number * 5, number * 7 from numbers \(10000\)\"
log_command bendsql --dsn "$DSN"  -q \"create table del_id \(id int\) as select cast\(FLOOR\(0 + RAND\(number\) * 10000\), int\) from numbers\(2000\)\"

