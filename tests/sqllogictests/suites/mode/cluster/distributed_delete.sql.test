statement ok
drop table if exists t;

statement ok
drop table if exists t_origin;

# make sure there will be multiple blocks there, by shrink the `row_per_block`
statement ok
create table t (id int not null, c1 int not null, c2 int not null) row_per_block=10;

# generate test data
statement ok
insert into t select number, number * 10, number * 5 from numbers(500) where number ;

statement ok
insert into t select number, number * 10, number * 5 from numbers(1000) where number > 499;

statement ok
insert into t select number, number * 10, number * 5 from numbers(1500) where number > 999;

# "backup" t
statement ok
create table t_origin as select * from t;

# do the deletion (in distributed settings)
# two segments are totally rewritten, one segment is reserved
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

# backup t again
statement ok
create table t_after_delete as select * from t;

# one segment is totally deleted, two segments are reserved
statement ok
delete from t where id <= 499;

# check the sum of columns
query I
select (select sum(id) from t_after_delete where id > 499) = (select sum(id) from t);
----
1

query I
select (select sum(c1) from t_after_delete where id > 499) = (select sum(c1) from t);
----
1

query I
select (select sum(c2) from t_after_delete where id > 499) = (select sum(c2) from t);
----
1

# backup t again
statement ok
create table t_after_delete_2 as select * from t;

# some block is totally deleted, some block is reserved, some block is partially reserved
statement ok
delete from t where id > 600 and id < 700;

# check the sum of columns
query I
select (select sum(id) from t_after_delete_2 where id <= 600 or id >= 700) = (select sum(id) from t);
----
1

query I
select (select sum(c1) from t_after_delete_2 where id <= 600 or id >= 700) = (select sum(c1) from t);
----
1

query I
select (select sum(c2) from t_after_delete_2 where id <= 600 or id >= 700) = (select sum(c2) from t);
----
1

# for issue #11784 https://github.com/datafuselabs/databend/issues/11784

statement ok
drop table if exists t;

statement ok
drop table if exists del_id;

statement ok
create table t (id int not null, c1 int not null, c2 int not null) row_per_block=3;

statement ok
insert into t select number, number * 5, number * 7 from numbers(50);

statement ok
create table del_id (id int not null) as select cast(FLOOR(0 + RAND(number) * 50), int) from numbers(10);

statement ok
delete from t where id in (select id from del_id);

statement ok
create view v_after_deletion as select number as id, number * 5 as c1, number * 7 as c2 from numbers(50) where id not in (select * from del_id);

query III
select * from v_after_deletion order by id except select * from t order by id;
----

query III
select * from t order by id except select * from v_after_deletion order by id;
----

query I
select (select count() from v_after_deletion) = (select count() from t);
----
1

statement ok
drop table t;

statement ok
drop view v_after_deletion;

statement ok
drop table if exists del_id;
