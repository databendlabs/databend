statement ok
drop table if exists t;

statement ok
drop table if exists t_origin;

# make sure there will be multiple blocks there, by shrink the `row_per_block`
statement ok
create table t (id int, c1 int, c2 int) row_per_block=10;

# generate test data
statement ok
insert into t select number, number * 10, number * 5 from numbers(500) where number ;

statement ok
insert into t select number, number * 10, number * 5 from numbers(1000) where number > 499;

statement ok
insert into t select number, number * 10, number * 5 from numbers(1500) where number > 1499;

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

# for issue #11784 https://github.com/datafuselabs/databend/issues/11784

statement ok
drop table if exists t;

statement ok
drop table if exists del_id;

statement ok
create table t (id int, c1 int, c2 int) row_per_block=3;

statement ok
insert into t select number, number * 5, number * 7 from numbers(50);

statement ok
create table del_id (id int) as select cast(FLOOR(0 + RAND(number) * 50), int) from numbers(10);

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




