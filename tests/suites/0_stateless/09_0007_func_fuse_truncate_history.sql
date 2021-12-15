DROP DATABASE IF EXISTS db_09_0007;
CREATE DATABASE db_09_0007;
USE db_09_0007;

create table t(a uint64);

insert into t values (5);
insert into t values (6);
insert into t values (7);

-- expects 3
select count(*) from fuse_history('db_09_0007', 't');


------------------------------

-- do the truncation (without syntax sugar)
-- 2 snapshot removed, and not segments or blocks will be removed
select * from fuse_truncate_history('db_09_0007', 't');
-- after truncation, count of history will 1
select count(*) from fuse_history('db_09_0007', 't');
-- the three rows will be kept
select * from t order by a;


------------------------------

-- before this, 1 snapshot, 3 segment and 3 blocks are left
insert overwrite t values (8);
insert overwrite t values (9);
insert overwrite t values (10);

-- 3 snapshot, 5 segments, and 5 blocks will be removed
select * from fuse_truncate_history('db_09_0007', 't');
-- only one rows will be kept, i.e. "10"
select * from t;

------------------------------

DROP TABLE t;
DROP DATABASE db_09_0007;
