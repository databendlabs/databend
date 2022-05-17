set timezone = 'UTC';
SELECT today() >= 18869;
SELECT now() >= 1630295616;
select  to_datetime(1630320462000000), to_int64(to_datetime(1630320462000000))  = 1630320462000000;
select  to_date(18869), to_uint32(to_date(18869))  = 18869;
select  to_datetime(1640019661000000), to_int64(to_datetime(1640019661000000))  = 1640019661000000;
select  to_date('1000-01-01');
select  to_date('9999-12-31');
select  to_date('10000-12-31'); -- {ErrorCode 1010}
select  to_date('0999-12-31'); -- {ErrorCode 1068}
select  to_datetime('1000-01-01 00:00:00');
select  to_datetime('9999-12-31 23:59:59');
select  to_datetime('10000-01-01 00:00:00'); -- {ErrorCode 1010}
select  to_datetime('0999-12-31 23:59:59'); -- {ErrorCode 1069}

select typeof(today() + 3) = 'DATE';
select typeof(today() - 3) = 'DATE';
select typeof(now() - 3) = 'TIMESTAMP(6)';
select typeof(to_datetime(1640019661000000)) = 'TIMESTAMP(6)';
select today() + 1 - today() = 1;

select typeof(today() - today()) = 'INT';
select typeof(now() - now()) = 'INT';
select sum(today() + number - today()) = 45 from numbers(10);

select today() - 1 = yesterday();
select today() - yesterday()  = 1;
select today() + 1 = tomorrow();
select tomorrow() - today() = 1;

select toYYYYMM(to_datetime(1630833797000000));
select toYYYYMM(to_date(18875));
select toYYYYMM(to_datetime(1630833797000000))  =  202109;
select toYYYYMM(to_date(18875))  =  202109;

select '===round===';
select timeSlot(to_datetime(1630320462000000));
select toStartOfHour(to_datetime(1630320462000000));
select toStartOfFifteenMinutes(to_datetime(1630320462000000));
select toStartOfMinute(to_datetime(1630320462000000));
select toStartOfFiveMinutes(to_datetime(1630320462000000));
select toStartOfTenMinutes(to_datetime(1630320462000000));
select timeSlot(now()) <= now();
select '===round-end===';

select '===toYYYYMMDDhhmmss===';
select toYYYYMMDDhhmmss(to_datetime(1630833797000000));
select toYYYYMMDDhhmmss(to_date(18875));
select toYYYYMMDDhhmmss(to_datetime(1630833797000000))  =  20210905092317;
select toYYYYMMDDhhmmss(to_date(18875))  =  20210905000000;
select '===toYYYYMMDDhhmmss===';

select '===toYYYYMMDD===';
select toYYYYMMDD(to_datetime(1630833797000000));
select toYYYYMMDD(to_date(18875));
select toYYYYMMDD(to_datetime(1630833797000000))  =  20210905;
select toYYYYMMDD(to_date(18875))  =  20210905;
select '===toYYYYMMDD===';

select '===toStartOf===';
select toStartOfYear(to_datetime(1630812366000000));
select toStartOfISOYear(to_datetime(1630812366000000));
select toStartOfYear(to_date(18869));
select toStartOfISOYear(to_date(18869));

select toStartOfQuarter(to_datetime(1631705259000000));
select toStartOfQuarter(to_datetime(1621078059000000));
select toStartOfMonth(to_datetime(1631705259000000));
select toStartOfQuarter(to_date(18885));
select toStartOfQuarter(to_date(18762));
select toStartOfMonth(to_date(18885));

select toStartOfWeek(to_datetime(1632397739000000));
select toStartOfWeek(to_datetime(1632397739000000), 0);
select toStartOfWeek(to_datetime(1632397739000000), 1);
select toStartOfWeek(to_datetime(1632397739000000), 2);
select toStartOfWeek(to_datetime(1632397739000000), 3);
select toStartOfWeek(to_datetime(1632397739000000), 4);
select toStartOfWeek(to_datetime(1632397739000000), 5);
select toStartOfWeek(to_datetime(1632397739000000), 6);
select toStartOfWeek(to_datetime(1632397739000000), 7);
select toStartOfWeek(to_datetime(1632397739000000), 8);
select toStartOfWeek(to_datetime(1632397739000000), 9);
select toStartOfWeek(to_date(18769));
select toStartOfWeek(to_date(18769), 0);
select toStartOfWeek(to_date(18769), 1);
select toStartOfWeek(to_date(18769), 2);
select toStartOfWeek(to_date(18769), 3);
select toStartOfWeek(to_date(18769), 4);
select toStartOfWeek(to_date(18769), 5);
select toStartOfWeek(to_date(18769), 6);
select toStartOfWeek(to_date(18769), 7);
select toStartOfWeek(to_date(18769), 8);
select toStartOfWeek(to_date(18769), 9);
select toStartOfWeek(to_date('1000-01-01')); -- {ErrorCode 1068}
select toStartOfWeek(to_datetime('1000-01-01 00:00:00')); -- {ErrorCode 1068}
select '===toStartOf===';

select '===addYears===';
select addYears(to_date(18321), cast(1, UINT8));  -- 2020-2-29 + 1 year
select addYears(to_date(18321), cast(1, UINT16));
select addYears(to_date(18321), cast(1, UINT32));
select addYears(to_date(18321), cast(1, UINT64));
select addYears(to_date(18321), cast(-1, INT8));
select addYears(to_date(18321), cast(-1, INT16));
select addYears(to_date(18321), cast(-1, INT32));
select addYears(to_date(18321), cast(-1, INT64));
select addYears(to_datetime(1582970400000000), cast(50, INT8)); -- 2020-2-29T10:00:00 + 50 years
select addYears(to_datetime(1582970400000000), cast(-50, INT8)); -- 2020-2-29T10:00:00 - 50 years
select addYears(to_date('9999-12-31'), 1); -- {ErrorCode 1068}
select addYears(to_datetime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addYears===';

select '===subtractMonths===';
select subtractMonths(to_date(18321), cast(13, INT16)); -- 2020-2-29 - 13 months
-- select to_date(18321) -  interval '13' month;

select subtractMonths(to_datetime(1582970400000000), cast(122, INT16)); -- 2020-2-29T10:00:00 - (12*10 + 2) months
-- select to_datetime(1582970400000000) -  interval '122' month;
select subtractMonths(to_date('1000-01-01'), 1); -- {ErrorCode 1068}
select subtractMonths(to_datetime('1000-01-01 00:00:00'), 1); -- {ErrorCode 1069}
select '===subtractMonths===';

select '===addDays===';
select addDays(to_date(18321), cast(1, INT16)); -- 2020-2-29 + 1 day
-- select to_date(18321) + interval '1' day;

select addDays(to_datetime(1582970400000000), cast(-1, INT16)); -- 2020-2-29T10:00:00 - 1 day
-- select to_datetime(1582970400000000) + interval '-1' day;
select addDays(to_date('9999-12-31'), 1); -- {ErrorCode 1068}
select addDays(to_datetime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addDays===';

select '===addHours===';
select addHours(to_datetime(1582970400000000), cast(25, INT32)); -- 2020-2-29T10:00:00 + 25 hours
-- select to_datetime(1582970400000000) + interval '25' hour;
select addHours(to_date(18321), cast(1.2, Float32));
select addHours(to_date('9999-12-31'), 24); -- {ErrorCode 1069}
select addHours(to_datetime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addHours===';

select '===subtractMinutes===';
select subtractMinutes(to_datetime(1582970400000000), cast(1, INT32)); -- 2020-2-29T10:00:00 - 1 minutes
select subtractMinutes(to_date('1000-01-01'), 1); -- {ErrorCode 1069}
select subtractMinutes(to_datetime('1000-01-01 00:00:00'), 1); -- {ErrorCode 1069}
select '===subtractMinutes===';

select '===addSeconds===';
select addSeconds(to_datetime(1582970400000000), cast(61, INT32)); -- 2020-2-29T10:00:00 + 61 seconds
select '===addSeconds===';

select '===toMonth===';
select toMonth(to_datetime(1633081817000000));
select toMonth(to_date(18901));
select toMonth(to_datetime(1633081817000000))  =  10;
select toMonth(to_date(18901))  =  10;
select '===toMonth===';

select '===toDayOfYear===';
select toDayOfYear(to_datetime(1633173324000000));
select toDayOfYear(to_date(18902));
select toDayOfYear(to_datetime(1633173324000000))  =  275;
select toDayOfYear(to_date(18902))  =  275;
select '===toDayOfYear===';

select '===toDayOfMonth===';
select toDayOfMonth(to_datetime(1633173324000000));
select toDayOfMonth(to_date(18902));
select toDayOfMonth(to_datetime(1633173324000000))  =  2;
select toDayOfMonth(to_date(18902))  =  2;
select '===toDayOfMonth===';

select '===toDayOfWeek===';
select toDayOfWeek(to_datetime(1633173324000000));
select toDayOfWeek(to_date(18902));
select toDayOfWeek(to_datetime(1633173324000000))  =  6;
select toDayOfWeek(to_date(18902))  =  6;
select '===toDayOfWeek===';

select '===toHour===';
select toHour(to_datetime(1634551542000000))  =  10;
select '===toHour===';

select '===toMinute===';
select toMinute(to_datetime(1634551542000000))  =  5;
select '===toMinute===';

select '===toSecond===';
select toSecond(to_datetime(1634551542000000))  =  42;
select '===toSecond===';
select '===toMonday===';
select toMonday(to_datetime(1634614318000000))  =  to_date('2021-10-18');
select '===toMonday===';

select '===toYear===';
select toYear(to_datetime(1646404329000000)) = 2022;
select '===toYear===';

select '===EXTRACT===';
select EXTRACT(YEAR FROM to_datetime('2022-03-04 22:32:09')) = 2022;
select EXTRACT(MONTH FROM to_datetime('2022-03-04 22:32:09')) = 3;
select EXTRACT(DAY FROM to_datetime('2022-03-04 22:32:09')) = 4;
select EXTRACT(HOUR FROM to_datetime('2022-03-04 22:32:09')) = 22;
select EXTRACT(MINUTE FROM to_datetime('2022-03-04 22:32:09')) = 32;
select EXTRACT(SECOND FROM to_datetime('2022-03-04 22:32:09')) = 9;
select '===EXTRACT===';


select '===CMP===';

select to_datetime('2022-04-01 06:50:20')   = '2022-04-01 06:50:20';
select to_datetime('2022-04-01 06:50:20')   > '2022-04-01 04:50:20';
select to_datetime('2022-04-01 06:50:20')   < '2022-04-02 04:50:20';

select '===INSERT===';
drop table if exists ts;
create table ts(a DateTime(6), b DateTime, c Date);
insert into ts values(now(), now(), today());
select to_datetime(a) = to_datetime(b) from ts;
drop table if exists ts;


create table t(a datetime, b DateTime(3), c Date);

insert into t values('2022-04-02 15:10:28', '2022-04-02 15:10:28', '1000-01-01');
insert into t values('2022-04-02 15:10:28.221', '2022-04-02 15:10:28.221', '9999-12-31');
insert into t values('0999-04-02 15:10:28.221', '2022-04-02 15:10:28.221', '2020-10-10'); -- {ErrorCode 1069}
insert into t values('10000-01-01 00:00:00', '2022-04-02 15:10:28.221', '2020-10-10'); -- {ErrorCode 1010}
insert into t values('2022-04-02 15:10:28.221', '2022-04-02 15:10:28.221', '0999-10-10'); -- {ErrorCode 1068}
insert into t values('2022-04-02 15:10:28.221', '2022-04-02 15:10:28.221', '10000-10-10'); -- {ErrorCode 1010}

select * from t order by b;
drop table t;

select '===dateAdd===';
set enable_planner_v2=1;
select dateAdd(to_date(18321), 1, YEAR);
select dateAdd(to_date(18321), -1, YEAR);
select dateAdd(to_date(18321), 1, SECOND);

create table t(a int);
insert into t values(1), (2);
select dateAdd(to_date(18321), a, YEAR) from t;
drop table t;

set enable_planner_v2=0;