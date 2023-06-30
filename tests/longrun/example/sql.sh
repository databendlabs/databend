source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN


for ((i=0; i<=3; i++)); do
    sleep 10
    log_command bendsql --dsn \"$DSN\" -q \"select \* from example order by time ASC LIMIT 10\"
done