create table test(a string, b int, d boolean);
insert into test values('a', 1, true);
insert into test values('b', 2, false);
select * from test order by a desc;

truncate table test;
insert into test select to_string(number), number, false from numbers(100000);
select min(a), max(b), count() from test;

select 'bye';
drop table test;
