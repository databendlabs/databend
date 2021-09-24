SELECT today() >= 18869;
SELECT now() >= 1630295616;
select  toDateTime(1630320462), toUInt32(toDateTime(1630320462))  = 1630320462;
select  toDate(18869), toUInt32(toDate(18869))  = 18869;

select toTypeName(today() + 3) = 'Date16';
select toTypeName(today() - 3) = 'Date16';
select toTypeName(now() - 3) = 'DateTime32';
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
select '===addHours===';

select '===subtractMinutes===';
select subtractMinutes(toDateTime(1582970400), cast(1, INT32)); -- 2020-2-29T10:00:00 - 1 minutes
select '===subtractMinutes===';

select '===addSeconds===';
select addSeconds(toDateTime(1582970400), cast(61, INT32)); -- 2020-2-29T10:00:00 + 61 seconds
select '===addSeconds===';