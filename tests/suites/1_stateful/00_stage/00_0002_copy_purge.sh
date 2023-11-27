#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

rm -rf /tmp/00_0002
mkdir -p /tmp/00_0002
cat << EOF > /tmp/00_0002/i0.csv
1,1
2,2
EOF

stmt "drop table if exists ii"
stmt "create table ii(a int, b int)"

stmt "drop stage if exists s_ii"
stmt "create stage s_ii url='fs:///tmp/00_0002/';"

query "copy into ii from @s_ii file_format = (type = CSV) purge = true"

query "list @s_ii"

stmt "select * from ii"

stmt "truncate table ii"

cat << EOF > /tmp/00_0002/i1.csv
1,1
2,2
EOF

query "copy into ii from @s_ii file_format = (type = CSV) files = ('i1.csv') purge = true"

query "list @s_ii"

stmt "select * from ii"

stmt "drop table if exists ii"
stmt "drop stage if exists s_ii"