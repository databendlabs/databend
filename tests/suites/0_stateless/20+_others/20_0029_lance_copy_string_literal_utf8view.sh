#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cat <<'EOF' | bendsql_connect_root > /dev/null
create or replace stage test_lance_utf8view;
copy into @test_lance_utf8view/t_wubx/
from (
    select number, 'abc' as literal, number + 1 as label
    from numbers(10)
)
file_format=(type=lance)
use_raw_path=true
overwrite=true
detailed_output=true;
EOF

echo "list @test_lance_utf8view/t_wubx PATTERN = '.*[.]lance';" \
    | bendsql_connect_root \
    | grep -q '[.]lance' \
    && echo 1 || echo 0
echo "list @test_lance_utf8view/t_wubx PATTERN = '.*/_versions/.*manifest';" \
    | bendsql_connect_root \
    | grep -q 'manifest' \
    && echo 1 || echo 0

echo "drop stage if exists test_lance_utf8view" | bendsql_connect_root > /dev/null
