drop table if exists test;
create table test(a string, b int, c boolean, d variant);
insert into test values('a', 1, true, '[1,2]');
insert into test values('b', 2, false, '{"k":"v"}');
select * from test order by a desc;

truncate table test;
insert into test select to_string(number), number, false, number from numbers(100000);
select min(a), max(b), max(d), count() from test;

select '1';select 2; select 1+2;

select [], {};

-- ignore this line

select /* ignore this block */ 'with comment';

select 1; select 2; select '
a'; select 3;

-- enable it after we support code string in databend
-- select $$aa$$;
-- select $$
-- def add(a, b):
-- 	a + b
-- $$;

/* ignore this block /* /*
select 'in comment block';
*/

select 1.00 + 2.00, 3.00, 0.0000000170141183460469231731687303715884105727000, -0.0000000170141183460469231731687303715884105727000;

select/*+ SET_VAR(timezone='Asia/Shanghai') */ timezone();

drop table if exists test_decimal;
create table test_decimal(a decimal(40, 0), b decimal(20 , 2));
insert into test_decimal select number, number from numbers(3);

select * from test_decimal;

drop table if exists test_nested;
create table test_nested(a array(int), b map(string, string), c tuple(x int, y string null));
insert into test_nested values([1,2,3], null, (1, 'ab')), (null, {'k1':'v1', 'k2':'v2'}, (2, null));
select * from test_nested;
select a[1], b['k1'], c:x, c:y from test_nested;

select {'k1':'v1','k2':'v2'}, [to_binary('ab'), to_binary('xyz')], (parse_json('[1,2]'), to_date('2024-04-10'));

select 'bye';
drop table test;
drop table test_decimal;
drop table test_nested;
