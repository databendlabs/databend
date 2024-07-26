#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


#############################################################
# check that the timestamp we inserted during prepare phase #
# is at least 2 days ago                                    #
#############################################################

# format of `faked` is "2024-05-22 03:00:35.977589"
c=$(echo "select c from test_faketime.t" | $BENDSQL_CLIENT_CONNECT)
# manually "time diff"
faked=$(date -d "$c" +%s)
current_timestamp=$(date +%s)
time_diff=$((current_timestamp - faked))
time_diff_days=$(echo "scale=2; $time_diff / 86400" | bc)

# expects time difference is larger than 2 days
if (( $(echo "$time_diff_days > 2" | bc -l) )); then
    echo "OK"
else
    echo "assertion failure"
fi

