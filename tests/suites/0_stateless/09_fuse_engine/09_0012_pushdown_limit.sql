create table t09_0012(c int);

-- there will be 2 blocks after two insertions, each block contains 2 rows
insert into t09_0012 values(1), (2);
insert into t09_0012 values(3), (4);

-- expects "partitions_scanned: 2"
explain select * from t09_0012;
-- expects "partitions_scanned: 1"
explain select * from t09_0012 limit 1;
-- expects "partitions_scanned: 1"
explain select * from t09_0012 limit 2;
-- expects "partitions_scanned: 2"
explain select * from t09_0012 limit 3;
-- expects "partitions_scanned: 2"
explain select * from t09_0012 limit 4;
-- expects "partitions_scanned: 0"
explain select * from t09_0012 limit 0;
drop table  t09_0012;
