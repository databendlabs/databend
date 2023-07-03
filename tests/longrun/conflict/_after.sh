source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn \"$DSN\" -q \"create view v_after_deletion as select number as id, number * 5 as c1, number * 7 as c2 from numbers\(50\) where id not in \(select * from del_id\)\"
log_command bendsql --dsn \"$DSN\" -q \"select * from v_after_deletion order by id except select * from t order by id\"
log_command bendsql --dsn \"$DSN\" -q \"select * from t order by id except select * from v_after_deletion order by id\"
log_command bendsql --dsn \"$DSN\" -q \"select \(select count\(\) from v_after_deletion\) = \(select count\(\) from t\)\"
