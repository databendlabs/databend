-- cast function
select "====CAST====";
set timezone='UTC';
select toTimestamp(1630320462000000);
select toTimestamp('2000-01-01 00:00:00');
set timezone='Asia/Shanghai'; -- Asia/Shanghai: +8:00
select toTimestamp(1630320462000000);
select toTimestamp('2000-01-01 12:00:00');
-- insert into table, serialization and deserialization
select "====INSERT_WITH_VALUES====";
set timezone = 'UTC';
create table tt (a timestamp);
insert into table tt values ('2021-04-30 22:48:00'), (toTimestamp('2021-04-30 22:48:00'));
select * from tt;
set timezone = 'Asia/Shanghai';
select * from tt;
-- number function
-- 1619820000000000 = 2021-04-30 22:00:00
select "====NUMBER_FUNCTION====";
select "==UTC==";
set timezone = 'UTC';
select toyyyymm(toTimestamp(1619820000000000));
select toyyyymmdd(toTimestamp(1619820000000000));
select toyyyymmddhhmmss(toTimestamp(1619820000000000));
select tostartofmonth(toTimestamp(1619820000000000));
select tomonth(toTimestamp(1619820000000000));
select todayofyear(toTimestamp(1619820000000000));
select todayofmonth(toTimestamp(1619820000000000));
select todayofweek(toTimestamp(1619820000000000));
set timezone = 'Asia/Shanghai';
select "==Asia/Shanghai==";
select toyyyymm(toTimestamp(1619820000000000));
select toyyyymmdd(toTimestamp(1619820000000000));
select toyyyymmddhhmmss(toTimestamp(1619820000000000));
select tostartofmonth(toTimestamp(1619820000000000));
select tomonth(toTimestamp(1619820000000000));
select todayofyear(toTimestamp(1619820000000000));
select todayofmonth(toTimestamp(1619820000000000));
select todayofweek(toTimestamp(1619820000000000));
-- round function
select "====ROUNDER_FUNCTION====";
-- 1619822911999000 = 2021-04-30 22:48:31.999
select "==UTC==";
set timezone = 'UTC';
select tostartofsecond(toTimestamp(1619822911999000));
select tostartofminute(toTimestamp(1619822911999000));
select tostartoffiveminutes(toTimestamp(1619822911999000));
select tostartoftenminutes(toTimestamp(1619822911999000));
select tostartoffifteenminutes(toTimestamp(1619822911999000));
select timeslot(toTimestamp(1619822911999000));
select tostartofhour(toTimestamp(1619822911999000));
select tostartofday(toTimestamp(1619822911999000));
select tostartofweek(toTimestamp(1619822911999000));
set timezone = 'Asia/Shanghai';
select "==Asia/Shanghai==";
select tostartofsecond(toTimestamp(1619822911999000));
select tostartofminute(toTimestamp(1619822911999000));
select tostartoffiveminutes(toTimestamp(1619822911999000));
select tostartoftenminutes(toTimestamp(1619822911999000));
select tostartoffifteenminutes(toTimestamp(1619822911999000));
select timeslot(toTimestamp(1619822911999000));
select tostartofhour(toTimestamp(1619822911999000));
select tostartofday(toTimestamp(1619822911999000));
select tostartofweek(toTimestamp(1619822911999000));
select "====INTERVAL_FUNCTION====";
-- 1619822911999000 = 2021-04-30 22:48:31.999
-- 1583013600000000 = 2020-02-29 22:00:00
select "==UTC==";
set timezone = 'UTC';
select addMonths(totimestamp(1619822911999000), 1);
select totimestamp(1583013600000000);
select addYears(totimestamp(1583013600000000), 1);
select "==Asia/Shanghai==";
set timezone = 'Asia/Shanghai';
select addMonths(totimestamp(1619822911999000), 1);
select totimestamp(1583013600000000);
select addYears(totimestamp(1583013600000000), 1);