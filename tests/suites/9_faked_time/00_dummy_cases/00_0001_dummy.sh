#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


#############################################################
# check that the timestamp we inserted during prepare phase #
# is at least 2 days ago                                    #
#############################################################

# format of `faked` is "2024-05-22 03:00:35.977589"
c=$(echo "select c from test_faketime.t" | $BENDSQL_CLIENT_CONNECT)
now=$(echo "select now()" | $BENDSQL_CLIENT_CONNECT)

# manually "time diff"
faked=$(date -d "$c" +%s)
current=$(date -d "$now" +%s)

time_diff=$((current- faked))

time_diff_days=$(python3 -c "print($time_diff / 86400)")

# Check if time difference is greater than 2 days
if python3 -c "import sys; sys.exit(0 if $time_diff_days > 2 else 1)"; then
    echo "OK"
else
    echo "assertion failure, time_diff_days is [$time_diff_days]"
fi
