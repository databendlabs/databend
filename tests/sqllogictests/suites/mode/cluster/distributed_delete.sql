statement ok
drop table if exists t;

statement ok
drop table if exists t_origin;

# make sure there will be multiple blocks there, by shrink the `row_per_block`
statement ok
create table t (id int, c1 int, c2 int) row_per_block=10;

# generate test data
statement ok
insert into t select number, number * 10, number * 5 from numbers(1000);

# "backup" t
statement ok
create table t_origin as select * from t;

# do the deletion (in distributed settings)
statement ok
delete from t where id % 3 = 0 and id > 500;

# check the sum of columns
query I
select (select sum(id) from t_origin where id % 3 != 0 or id <= 500) = (select sum(id) from t);
----
1

query I
select (select sum(c1) from t_origin where id % 3 != 0 or id <= 500) = (select sum(c1) from t);
----
1

query I
select (select sum(c2) from t_origin where id % 3 != 0 or id <= 500) = (select sum(c2) from t);
----
1