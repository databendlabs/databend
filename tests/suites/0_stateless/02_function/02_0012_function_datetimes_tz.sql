-- cast function
select "====CAST====";
set timezone='UTC';
select to_timestamp(1630320462000000);
select to_timestamp('2000-01-01 00:00:00');
set timezone='Asia/Shanghai'; -- Asia/Shanghai: +8:00
select to_timestamp(1630320462000000);
select to_timestamp('2000-01-01 12:00:00');
-- insert into table, serialization and deserialization
select "====INSERT_WITH_VALUES====";
set timezone = 'UTC';
create table tt (a timestamp);
insert into table tt values ('2021-04-30 22:48:00'), (to_timestamp('2021-04-30 22:48:00'));
select * from tt;
set timezone = 'Asia/Shanghai';
select * from tt;
-- number function
-- 1619820000000000 = 2021-04-30 22:00:00
select "====NUMBER_FUNCTION====";
select "==UTC==";
set timezone = 'UTC';
select toyyyymm(to_timestamp(1619820000000000));
select toyyyymmdd(to_timestamp(1619820000000000));
select toyyyymmddhhmmss(to_timestamp(1619820000000000));
select tostartofmonth(to_timestamp(1619820000000000));
select tomonth(to_timestamp(1619820000000000));
select todayofyear(to_timestamp(1619820000000000));
select todayofmonth(to_timestamp(1619820000000000));
select todayofweek(to_timestamp(1619820000000000));
set timezone = 'Asia/Shanghai';
select "==Asia/Shanghai==";
select toyyyymm(to_timestamp(1619820000000000));
select toyyyymmdd(to_timestamp(1619820000000000));
select toyyyymmddhhmmss(to_timestamp(1619820000000000));
select tostartofmonth(to_timestamp(1619820000000000));
select tomonth(to_timestamp(1619820000000000));
select todayofyear(to_timestamp(1619820000000000));
select todayofmonth(to_timestamp(1619820000000000));
select todayofweek(to_timestamp(1619820000000000));
-- round function
select "====ROUNDER_FUNCTION====";
-- 1619822911999000 = 2021-04-30 22:48:31.999
select "==UTC==";
set timezone = 'UTC';
select tostartofsecond(to_timestamp(1619822911999000));
select tostartofminute(to_timestamp(1619822911999000));
select tostartoffiveminutes(to_timestamp(1619822911999000));
select tostartoftenminutes(to_timestamp(1619822911999000));
select tostartoffifteenminutes(to_timestamp(1619822911999000));
select timeslot(to_timestamp(1619822911999000));
select tostartofhour(to_timestamp(1619822911999000));
select tostartofday(to_timestamp(1619822911999000));
select tostartofweek(to_timestamp(1619822911999000));
set timezone = 'Asia/Shanghai';
select "==Asia/Shanghai==";
select tostartofsecond(to_timestamp(1619822911999000));
select tostartofminute(to_timestamp(1619822911999000));
select tostartoffiveminutes(to_timestamp(1619822911999000));
select tostartoftenminutes(to_timestamp(1619822911999000));
select tostartoffifteenminutes(to_timestamp(1619822911999000));
select timeslot(to_timestamp(1619822911999000));
select tostartofhour(to_timestamp(1619822911999000));
select tostartofday(to_timestamp(1619822911999000));
select tostartofweek(to_timestamp(1619822911999000));
select "====INTERVAL_FUNCTION====";
-- 1619822911999000 = 2021-04-30 22:48:31.999
-- 1583013600000000 = 2020-02-29 22:00:00
select "==UTC==";
set timezone = 'UTC';
select addMonths(to_timestamp(1619822911999000), 1);
select to_timestamp(1583013600000000);
select addYears(to_timestamp(1583013600000000), 1);
select "==Asia/Shanghai==";
set timezone = 'Asia/Shanghai';
select addMonths(to_timestamp(1619822911999000), 1);
select to_timestamp(1583013600000000);
select addYears(to_timestamp(1583013600000000), 1);