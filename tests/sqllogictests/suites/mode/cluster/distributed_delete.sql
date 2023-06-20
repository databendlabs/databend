drop table if exists t;
drop table if exists t_origin;
drop table if exists del_id;
# make sure there will be multiple blocks there, by shrink the `row_per_block`
statement ok
create table t (id int, c1 int, c2 int) row_per_block=10;

# generate test data
statement ok
insert into t select number, number * 10, number * 5 from numbers(1000);

# generate the ids of rows to be deleted
statement ok
create table del_id (id int) as select cast(FLOOR(0 + RAND() * 1000), int) from numbers(100);

# "backup" t
statement ok
create table t_origin as select * from t;

# do the deletion (in distributed settings)
statement ok
delete from t where id in (select id from del_id);

# check the sum of columns
query I
select (select sum(id) from (select id from t_origin where id not in (select id from del_id))) = (select sum(id) from t);
----
1

query I
select (select sum(c1) from (select c1 from t_origin where id not in (select id from del_id))) = (select sum(c1) from t);
----
1

query I
select (select sum(c2) from (select c2 from t_origin where id not in (select id from del_id))) = (select sum(c2) from t);
----
