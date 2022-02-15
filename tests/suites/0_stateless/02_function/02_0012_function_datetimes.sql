SELECT today() >= 18869;
SELECT now() >= 1630295616;
select  toDateTime(1630320462), toUInt32(toDateTime(1630320462))  = 1630320462;
select  toDate(18869), toUInt32(toDate(18869))  = 18869;
select  toDateTime64(1640019661000), toInt64(toDateTime64(1640019661000))  = 1640019661000;

select toTypeName(today() + 3) = 'Date16';
select toTypeName(today() - 3) = 'Date16';
select toTypeName(now() - 3) = 'DateTime32';
select toTypeName(toDateTime64(1640019661000)) = 'DateTime64(3)';
select today() + 1 - today() = 1;

select toTypeName(today() - today()) = 'Int32';
select toTypeName(now() - now()) = 'Int32';
select sum(today() + number - today()) = 45 from numbers(10);

select today() - 1 = yesterday();
select today() - yesterday()  = 1;
select today() + 1 = tomorrow();
select tomorrow() - today() = 1;

select toYYYYMM(toDateTime(1630833797));
select toYYYYMM(toDate(18875));
select toYYYYMM(toDateTime(1630833797))  =  202109;
select toYYYYMM(toDate(18875))  =  202109;

select '===round===';
select timeSlot(toDateTime(1630320462));
select toStartOfHour(toDateTime(1630320462));
select toStartOfFifteenMinutes(toDateTime(1630320462));
select toStartOfMinute(toDateTime(1630320462));
select toStartOfFiveMinutes(toDateTime(1630320462));
select toStartOfTenMinutes(toDateTime(1630320462));
select timeSlot(now()) <= now();
select '===round-end===';

select '===toYYYYMMDDhhmmss===';
select toYYYYMMDDhhmmss(toDateTime(1630833797));
select toYYYYMMDDhhmmss(toDate(18875));
select toYYYYMMDDhhmmss(toDateTime(1630833797))  =  20210905092317;
select toYYYYMMDDhhmmss(toDate(18875))  =  20210905000000;
select '===toYYYYMMDDhhmmss===';

select '===toYYYYMMDD===';
select toYYYYMMDD(toDateTime(1630833797));
select toYYYYMMDD(toDate(18875));
select toYYYYMMDD(toDateTime(1630833797))  =  20210905;
select toYYYYMMDD(toDate(18875))  =  20210905;
select '===toYYYYMMDD===';

select '===toStartOf===';
select toStartOfYear(toDateTime(1630812366));
select toStartOfISOYear(toDateTime(1630812366));
select toStartOfYear(toDate(18869));
select toStartOfISOYear(toDate(18869));

select toStartOfQuarter(toDateTime(1631705259));
select toStartOfQuarter(toDateTime(1621078059));
select toStartOfMonth(toDateTime(1631705259));
select toStartOfQuarter(toDate(18885));
select toStartOfQuarter(toDate(18762));
select toStartOfMonth(toDate(18885));

select toStartOfWeek(toDateTime(1632397739));
select toStartOfWeek(toDateTime(1632397739), 0);
select toStartOfWeek(toDateTime(1632397739), 1);
select toStartOfWeek(toDateTime(1632397739), 2);
select toStartOfWeek(toDateTime(1632397739), 3);
select toStartOfWeek(toDateTime(1632397739), 4);
select toStartOfWeek(toDateTime(1632397739), 5);
select toStartOfWeek(toDateTime(1632397739), 6);
select toStartOfWeek(toDateTime(1632397739), 7);
select toStartOfWeek(toDateTime(1632397739), 8);
select toStartOfWeek(toDateTime(1632397739), 9);
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
select addYears(toDateTime(1582970400), cast(50, INT8)); -- 2020-2-29T10:00:00 + 50 years
select addYears(toDateTime(1582970400), cast(-50, INT8)); -- 2020-2-29T10:00:00 - 50 years
select '===addYears===';

select '===subtractMonths===';
select subtractMonths(toDate(18321), cast(13, INT16)); -- 2020-2-29 - 13 months
select toDate(18321) -  interval '13' month;

select subtractMonths(toDateTime(1582970400), cast(122, INT16)); -- 2020-2-29T10:00:00 - (12*10 + 2) months
select toDateTime(1582970400) -  interval '122' month;
select '===subtractMonths===';

select '===addDays===';
select addDays(toDate(18321), cast(1, INT16)); -- 2020-2-29 + 1 day
select toDate(18321) + interval '1' day;

select addDays(toDateTime(1582970400), cast(-1, INT16)); -- 2020-2-29T10:00:00 - 1 day
select toDateTime(1582970400) + interval '-1' day;
select '===addDays===';

select '===addHours===';
select addHours(toDateTime(1582970400), cast(25, INT32)); -- 2020-2-29T10:00:00 + 25 hours
select toDateTime(1582970400) + interval '25' hour;
select addHours(toDate(18321), cast(1.2, Float32));
select '===addHours===';

select '===subtractMinutes===';
select subtractMinutes(toDateTime(1582970400), cast(1, INT32)); -- 2020-2-29T10:00:00 - 1 minutes
select '===subtractMinutes===';

select '===addSeconds===';
select addSeconds(toDateTime(1582970400), cast(61, INT32)); -- 2020-2-29T10:00:00 + 61 seconds
select '===addSeconds===';

select '===toMonth===';
select toMonth(toDateTime(1633081817));
select toMonth(toDate(18901));
select toMonth(toDateTime(1633081817))  =  10;
select toMonth(toDate(18901))  =  10;
select '===toMonth===';

select '===toDayOfYear===';
select toDayOfYear(toDateTime(1633173324));
select toDayOfYear(toDate(18902));
select toDayOfYear(toDateTime(1633173324))  =  275;
select toDayOfYear(toDate(18902))  =  275;
select '===toDayOfYear===';

select '===toDayOfMonth===';
select toDayOfMonth(toDateTime(1633173324));
select toDayOfMonth(toDate(18902));
select toDayOfMonth(toDateTime(1633173324))  =  2;
select toDayOfMonth(toDate(18902))  =  2;
select '===toDayOfMonth===';

select '===toDayOfWeek===';
select toDayOfWeek(toDateTime(1633173324));
select toDayOfWeek(toDate(18902));
select toDayOfWeek(toDateTime(1633173324))  =  6;
select toDayOfWeek(toDate(18902))  =  6;
select '===toDayOfWeek===';

select '===toHour===';
select toHour(toDateTime(1634551542))  =  10;
select '===toHour===';

select '===toMinute===';
select toMinute(toDateTime(1634551542))  =  5;
select '===toMinute===';

select '===toSecond===';
select toSecond(toDateTime(1634551542))  =  42;
select '===toSecond===';
select '===toMonday===';
select toMonday(toDateTime(1634614318))  =  toDate('2021-10-18');
select '===toMonday===';
