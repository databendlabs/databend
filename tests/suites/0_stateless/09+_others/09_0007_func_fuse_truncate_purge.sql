DROP DATABASE IF EXISTS db_09_0007;
CREATE DATABASE db_09_0007;
USE db_09_0007;

create table t(a uint64);

insert into t values (5);
insert into t values (6);
insert into t values (7);

-- expects 3 history items
select count(*) from fuse_history('db_09_0007', 't');

-- truncate table will remove all the historical data but the latest snapshot
-- only the latest snapshot will be kept, no segments or block, but unfortunately, we can not verify it by sql (yet)
truncate table 't' purge;
-- expect 1 snapshot left
select count(*) from fuse_history('db_09_0007', 't');
-- but no data, since it is truncated
select * from t;
------------------------------

DROP TABLE t;
DROP DATABASE db_09_0007;
