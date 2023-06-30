source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn \"$DSN\" -q \"select count\(\*\) from example\"
