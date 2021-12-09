DROP DATABASE IF EXISTS db_09_0006;
CREATE DATABASE db_09_0006;
USE db_09_0006;

create table t(a uint64);

insert into t select number from numbers(10);
-- expects 1 blocks, 10 rows
select block_count, row_count from fuse_history('db_09_0006', 't') order by row_count desc limit 1;

insert into t select number from numbers(10);
-- expects 2 blocks, 20 rows
select block_count, row_count from fuse_history('db_09_0006', 't') order by row_count desc limit 1;

-- unknown objects
select * from fuse_history('db_09_0006', 'not_exist'); -- {ErrorCode 25}
select * from fuse_history('not_exist', 'not_exist'); -- {ErrorCode 3}

DROP TABLE t;
DROP DATABASE db_09_0006;
