#!/usr/bin/env bash
# most features are tested in sqllogic tests with diff type of external stages
# this test is mainly to test internal stage and user stage (and paths) is parsed and used correctly, the file content is not important

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "DROP TABLE IF EXISTS ontime"
stmt "DROP STAGE IF EXISTS data"

comment "create table ontime ..."
cat $TESTS_DATA_DIR/ddl/ontime.sql |  $BENDSQL_CLIENT_CONNECT
stmt "ALTER TABLE ontime ADD COLUMN new_col int NOT NULL DEFAULT 3;"

comment "create stage data..."
echo "create stage data url='fs://$TESTS_DATA_DIR/';" |  $BENDSQL_CLIENT_CONNECT

query "copy into ontime from @data/ontime_200.parquet FILE_FORMAT = (type = parquet  missing_field_as = field_default);"
query " select tail_number, new_col from ontime where dayofmonth=1 order by new_col"

query "copy into ontime from @data/ontime_200.parquet FILE_FORMAT = (type = parquet  missing_field_as = 'field_default') force=true;"

stmt "DROP TABLE IF EXISTS ontime"
stmt "DROP STAGE IF EXISTS data"