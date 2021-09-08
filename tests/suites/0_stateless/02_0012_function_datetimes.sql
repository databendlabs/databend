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


