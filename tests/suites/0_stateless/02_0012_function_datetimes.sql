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

select toYYYYMM(toDateTime(1630320462));
select toYYYYMM(today() - 30) = toYYYYMM(today()) - 1;
select toYYYYMM(today() + 30) = toYYYYMM(today()) + 1;
select toYYYYMM(today()) - toYYYYMM(today() - 30) = 1;
select toYYYYMM(today() + 30) - toYYYYMM(today()) = 1;
