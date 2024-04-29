#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# https://github.com/datafuselabs/databend/issues/15039

rm -rf /tmp/00_0002_issue_15039
mkdir -p /tmp/00_0002_issue_15039
cat << EOF > /tmp/00_0002_issue_15039/i0.csv
1,1
2,2
EOF

stmt "drop table if exists ii"
stmt "create table ii(a int, b int)"

stmt "drop stage if exists s_ii"
stmt "create stage s_ii url='fs:///tmp/00_0002_issue_15039/';"

query "copy into ii from @s_ii file_format = (type = CSV) purge = true"

query "list @s_ii"

stmt "select * from ii"

# put file i0 into the stage again
cat << EOF > /tmp/00_0002_issue_15039/i0.csv
1,1
2,2
EOF

# since purge_duplicated_files_in_copy is disabled by default, the file i0 should be listed
# but data of i0 should not be copied into table ii

query "copy into ii from @s_ii file_format = (type = CSV) files = ('i0.csv') purge = true"

query "list @s_ii" | awk '{print $1}'

stmt "select * from ii"

stmt "drop table if exists ii"
stmt "drop stage if exists s_ii"