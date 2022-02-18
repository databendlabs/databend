SELECT toTypeName(CAST(number AS float)) FROM numbers_mt(1);
SELECT toTypeName(CAST(number AS float32)) FROM numbers_mt(1);
SELECT toTypeName(CAST(number AS UInt64)) FROM numbers_mt(1);
SELECT toTypeName(toint8('8')) FROM numbers_mt(1);
SELECT toTypeName(toint16('16')) FROM numbers_mt(1);
SELECT toTypeName(toint32('32')) FROM numbers_mt(1);
SELECT toTypeName(toint64('64')) FROM numbers_mt(1);
SELECT toTypeName(toUInt32('64')) FROM numbers_mt(1);

SELECT CAST(1 + 1, Float64);
SELECT CAST(CAST(1 + 1 + 1, String) AS Int8);

SELECT CAST(Null as Int64); -- {ErrorCode 1010}
SELECT CAST(Null as Varchar); -- {ErrorCode 1010}

-- Null can only be cast successfully to type boolean(false)
SELECT CAST(Null as Boolean);
SELECT CAST('33' as signed) = 33;
SELECT CAST('33' as unsigned) = 33;
SELECT CAST('-33aa' as signed) = 33; -- {ErrorCode 1010}
SELECT CAST('33 aa' as unsigned) = 33; -- {ErrorCode 1010}
SELECT CAST('-33' as unsigned) = 0; -- {ErrorCode 1010}
SELECT CAST('aa' as unsigned) = 0; -- {ErrorCode 1010}
SELECT CAST('aa' as Float64) = 0; -- {ErrorCode 1010}

SELECT '===DATE/DATETIME===';
SELECT  toDateTime('2021-03-05 01:01:01') + 1 = toDateTime('2021-03-05 01:01:02');
SELECT  toDate('2021-03-05') + 1 = toDate('2021-03-06');
SELECT  toString(toDate('2021-03-05') + 1) = '2021-03-06';
SELECT toDateTime(toDate('2021-03-05')) = toDateTime('2021-03-05 00:00:00');
SELECT toDate(toDateTime('2021-03-05 01:00:00')) = toDate('2021-03-05');
SELECT toString(toDateTime64(1640019661000)) = '2021-12-20 17:01:01.000';
SELECT toDate(toDateTime64(1640019661000)) = toDate('2021-12-20');
SELECT toDateTime(toDateTime64(1640019661000)) = toDateTime('2021-12-20 17:01:01');
