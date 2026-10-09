#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create user if not exists notification_sender identified by 'test'" | bendsql_connect_root

echo "call system\$send_notification('oncall', 'alert')" | bendsql_connect_user notification_sender test -A
echo "select * from send_notification('oncall', 'alert')" | bendsql_connect_user notification_sender test -A

echo "drop user notification_sender" | bendsql_connect_root

# The expected permission errors must occur before attempting any RPC.
exit 0
