#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

user=modify_not_null_user_0028
password=modify_not_null_password_0028
db=db_modify_not_null_0028

bendsql_connect_root_null --query="
DROP USER IF EXISTS $user;
DROP DATABASE IF EXISTS $db;
CREATE DATABASE $db;
"

# The table has no statistics for b: the UPDATE rewrites a segment that mixes
# a block written before ADD COLUMN with one that stores b.
bendsql_connect_root_null --query="
SET enable_compact_after_write = 0;
CREATE TABLE $db.t (a INT) storage_format = 'parquet' segment_format = 'row_oriented';
INSERT INTO $db.t VALUES (1);
ALTER TABLE $db.t ADD COLUMN b INT NULL DEFAULT 7;
INSERT INTO $db.t VALUES (2, 100);
OPTIMIZE TABLE $db.t COMPACT SEGMENT;
UPDATE $db.t SET b = 200 WHERE a = 2;
INSERT INTO $db.t VALUES (3, NULL);
CREATE USER $user IDENTIFIED BY '$password';
GRANT ALTER ON $db.* TO $user;
"

echo "-- statistics of b"
bendsql_connect_root --query="SELECT count(*) FROM fuse_statistic('$db', 't') WHERE column_name = 'b'"

# The NULL check is part of the ALTER and needs no SELECT privilege.
echo "-- with a NULL value"
bendsql_connect_user "$user" "$password" --query="ALTER TABLE $db.t MODIFY COLUMN b INT NOT NULL DEFAULT 7" 2>&1 |
	grep -o 'contains 1 NULL values'

bendsql_connect_root_null --query="DELETE FROM $db.t WHERE a = 3"

echo "-- without NULL values"
bendsql_connect_user "$user" "$password" --output null --query="ALTER TABLE $db.t MODIFY COLUMN b INT NOT NULL DEFAULT 7"
bendsql_connect_root --query="SELECT a, b FROM $db.t ORDER BY a"

bendsql_connect_root_null --query="
DROP USER IF EXISTS $user;
DROP DATABASE IF EXISTS $db;
"
