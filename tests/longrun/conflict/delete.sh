source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn \"$DSN\" -q \"delete from t where id in (select id from del_id)\"