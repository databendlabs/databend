SELECT today() >= 18869;
SELECT now() >= 1630295616;
select  toDateTime(1630320462000000), toInt64(toDateTime(1630320462000000))  = 1630320462000000;
select  toDate(18869), toUInt32(toDate(18869))  = 18869;
select  toDateTime(1640019661), toInt64(toDateTime(1640019661))  = 1640019661;

select toTypeName(today() + 3) = 'Date';
select toTypeName(today() - 3) = 'Date';
select toTypeName(now() - 3) = 'DateTime(0)';
select toTypeName(toDateTime(1640019661)) = 'DateTime(0)';
select today() + 1 - today() = 1;

select typeof(today() - today()) = 'INT';
select typeof(now() - now()) = 'INT';
select sum(today() + number - today()) = 45 from numbers(10);

select today() - 1 = yesterday();
select today() - yesterday()  = 1;
select today() + 1 = tomorrow();
select tomorrow() - today() = 1;

select toYYYYMM(toDateTime(1630833797000000));
select toYYYYMM(toDate(18875));
select toYYYYMM(toDateTime(1630833797000000))  =  202109;
select toYYYYMM(toDate(18875))  =  202109;

select '===round===';
select timeSlot(toDateTime(1630320462000000));
select toStartOfHour(toDateTime(1630320462000000));
select toStartOfFifteenMinutes(toDateTime(1630320462000000));
select toStartOfMinute(toDateTime(1630320462000000));
select toStartOfFiveMinutes(toDateTime(1630320462000000));
select toStartOfTenMinutes(toDateTime(1630320462000000));
select timeSlot(now()) <= now();
select '===round-end===';

select '===toYYYYMMDDhhmmss===';
select toYYYYMMDDhhmmss(toDateTime(1630833797000000));
select toYYYYMMDDhhmmss(toDate(18875));
select toYYYYMMDDhhmmss(toDateTime(1630833797000000))  =  20210905092317;
select toYYYYMMDDhhmmss(toDate(18875))  =  20210905000000;
select '===toYYYYMMDDhhmmss===';

select '===toYYYYMMDD===';
select toYYYYMMDD(toDateTime(1630833797000000));
select toYYYYMMDD(toDate(18875));
select toYYYYMMDD(toDateTime(1630833797000000))  =  20210905;
select toYYYYMMDD(toDate(18875))  =  20210905;
select '===toYYYYMMDD===';

select '===toStartOf===';
select toStartOfYear(toDateTime(1630812366000000));
select toStartOfISOYear(toDateTime(1630812366000000));
select toStartOfYear(toDate(18869));
select toStartOfISOYear(toDate(18869));

select toStartOfQuarter(toDateTime(1631705259000000));
select toStartOfQuarter(toDateTime(1621078059000000));
select toStartOfMonth(toDateTime(1631705259000000));
select toStartOfQuarter(toDate(18885));
select toStartOfQuarter(toDate(18762));
select toStartOfMonth(toDate(18885));

select toStartOfWeek(toDateTime(1632397739000000));
select toStartOfWeek(toDateTime(1632397739000000), 0);
select toStartOfWeek(toDateTime(1632397739000000), 1);
select toStartOfWeek(toDateTime(1632397739000000), 2);
select toStartOfWeek(toDateTime(1632397739000000), 3);
select toStartOfWeek(toDateTime(1632397739000000), 4);
select toStartOfWeek(toDateTime(1632397739000000), 5);
select toStartOfWeek(toDateTime(1632397739000000), 6);
select toStartOfWeek(toDateTime(1632397739000000), 7);
select toStartOfWeek(toDateTime(1632397739000000), 8);
select toStartOfWeek(toDateTime(1632397739000000), 9);
select toStartOfWeek(toDate(18769));
select toStartOfWeek(toDate(18769), 0);
select toStartOfWeek(toDate(18769), 1);
select toStartOfWeek(toDate(18769), 2);
select toStartOfWeek(toDate(18769), 3);
select toStartOfWeek(toDate(18769), 4);
select toStartOfWeek(toDate(18769), 5);
select toStartOfWeek(toDate(18769), 6);
select toStartOfWeek(toDate(18769), 7);
select toStartOfWeek(toDate(18769), 8);
select toStartOfWeek(toDate(18769), 9);
select toStartOfWeek(toDate('1000-01-01')); -- {ErrorCode 1068}
select toStartOfWeek(toDateTime('1000-01-01 00:00:00')); -- {ErrorCode 1068}
select '===toStartOf===';

select '===addYears===';
select addYears(toDate(18321), cast(1, UINT8));  -- 2020-2-29 + 1 year
select addYears(toDate(18321), cast(1, UINT16));
select addYears(toDate(18321), cast(1, UINT32));
select addYears(toDate(18321), cast(1, UINT64));
select addYears(toDate(18321), cast(-1, INT8));
select addYears(toDate(18321), cast(-1, INT16));
select addYears(toDate(18321), cast(-1, INT32));
select addYears(toDate(18321), cast(-1, INT64));
select addYears(toDateTime(1582970400000000), cast(50, INT8)); -- 2020-2-29T10:00:00 + 50 years
select addYears(toDateTime(1582970400000000), cast(-50, INT8)); -- 2020-2-29T10:00:00 - 50 years
select addYears(toDate('9999-12-31'), 1); -- {ErrorCode 1068}
select addYears(toDateTime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addYears===';

select '===subtractMonths===';
select subtractMonths(toDate(18321), cast(13, INT16)); -- 2020-2-29 - 13 months
select toDate(18321) -  interval '13' month;

select subtractMonths(toDateTime(1582970400000000), cast(122, INT16)); -- 2020-2-29T10:00:00 - (12*10 + 2) months
select toDateTime(1582970400000000) -  interval '122' month;
select subtractMonths(toDate('1000-01-01'), 1); -- {ErrorCode 1068}
select subtractMonths(toDateTime('1000-01-01 00:00:00'), 1); -- {ErrorCode 1069}
select '===subtractMonths===';

select '===addDays===';
select addDays(toDate(18321), cast(1, INT16)); -- 2020-2-29 + 1 day
select toDate(18321) + interval '1' day;

select addDays(toDateTime(1582970400000000), cast(-1, INT16)); -- 2020-2-29T10:00:00 - 1 day
select toDateTime(1582970400000000) + interval '-1' day;
select addDays(toDate('9999-12-31'), 1); -- {ErrorCode 1068}
select addDays(toDateTime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addDays===';

select '===addHours===';
select addHours(toDateTime(1582970400000000), cast(25, INT32)); -- 2020-2-29T10:00:00 + 25 hours
select toDateTime(1582970400000000) + interval '25' hour;
select addHours(toDate(18321), cast(1.2, Float32));
select addHours(toDate('9999-12-31'), 24); -- {ErrorCode 1069}
select addHours(toDateTime('9999-12-31 23:59:59'), 1); -- {ErrorCode 1069}
select '===addHours===';

select '===subtractMinutes===';
select subtractMinutes(toDateTime(1582970400000000), cast(1, INT32)); -- 2020-2-29T10:00:00 - 1 minutes
select subtractMinutes(toDate('1000-01-01'), 1); -- {ErrorCode 1069}
select subtractMinutes(toDateTime('1000-01-01 00:00:00'), 1); -- {ErrorCode 1069}
select '===subtractMinutes===';

select '===addSeconds===';
select addSeconds(toDateTime(1582970400000000), cast(61, INT32)); -- 2020-2-29T10:00:00 + 61 seconds
select '===addSeconds===';

select '===toMonth===';
select toMonth(toDateTime(1633081817000000));
select toMonth(toDate(18901));
select toMonth(toDateTime(1633081817000000))  =  10;
select toMonth(toDate(18901))  =  10;
select '===toMonth===';

select '===toDayOfYear===';
select toDayOfYear(toDateTime(1633173324000000));
select toDayOfYear(toDate(18902));
select toDayOfYear(toDateTime(1633173324000000))  =  275;
select toDayOfYear(toDate(18902))  =  275;
select '===toDayOfYear===';

select '===toDayOfMonth===';
select toDayOfMonth(toDateTime(1633173324000000));
select toDayOfMonth(toDate(18902));
select toDayOfMonth(toDateTime(1633173324000000))  =  2;
select toDayOfMonth(toDate(18902))  =  2;
select '===toDayOfMonth===';

select '===toDayOfWeek===';
select toDayOfWeek(toDateTime(1633173324000000));
select toDayOfWeek(toDate(18902));
select toDayOfWeek(toDateTime(1633173324000000))  =  6;
select toDayOfWeek(toDate(18902))  =  6;
select '===toDayOfWeek===';

select '===toHour===';
select toHour(toDateTime(1634551542000000))  =  10;
select '===toHour===';

select '===toMinute===';
select toMinute(toDateTime(1634551542000000))  =  5;
select '===toMinute===';

select '===toSecond===';
select toSecond(toDateTime(1634551542000000))  =  42;
select '===toSecond===';
select '===toMonday===';
select toMonday(toDateTime(1634614318000000))  =  toDate('2021-10-18');
select '===toMonday===';

select '===toYear===';
select toYear(toDateTime(1646404329000000)) = 2022;
select '===toYear===';

select '===EXTRACT===';
select EXTRACT(YEAR FROM toDateTime('2022-03-04 22:32:09')) = 2022;
select EXTRACT(MONTH FROM toDateTime('2022-03-04 22:32:09')) = 3;
select EXTRACT(DAY FROM toDateTime('2022-03-04 22:32:09')) = 4;
select EXTRACT(HOUR FROM toDateTime('2022-03-04 22:32:09')) = 22;
select EXTRACT(MINUTE FROM toDateTime('2022-03-04 22:32:09')) = 32;
select EXTRACT(SECOND FROM toDateTime('2022-03-04 22:32:09')) = 9;
select '===EXTRACT===';


select '===CMP===';

select toDateTime('2022-04-01 06:50:20')   = '2022-04-01 06:50:20';
select toDateTime('2022-04-01 06:50:20')   > '2022-04-01 04:50:20';
select toDateTime('2022-04-01 06:50:20')   < '2022-04-02 04:50:20';

select '===INSERT===';
drop table if exists ts;
create table ts(a DateTime(6), b DateTime, c Date);
insert into ts values(now(), now(), today());
select toDateTime(a) = toDateTime(b) from ts;
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
