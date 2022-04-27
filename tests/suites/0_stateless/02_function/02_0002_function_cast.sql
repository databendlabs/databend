SELECT typeof(CAST(number AS float)) FROM numbers_mt(1);
SELECT typeof(CAST(number AS float32)) FROM numbers_mt(1);
SELECT typeof(CAST(number AS UInt64)) FROM numbers_mt(1);
SELECT typeof(toint8('8')) FROM numbers_mt(1);
SELECT typeof(toint16('16')) FROM numbers_mt(1);
SELECT typeof(toint32('32')) FROM numbers_mt(1);
SELECT typeof(toint64('64')) FROM numbers_mt(1);
SELECT typeof(toUInt32('64')) FROM numbers_mt(1);
SELECT typeof(number::float) FROM numbers_mt(1);
SELECT typeof(number::float64) FROM numbers_mt(1);
SELECT typeof(number::UInt64) FROM numbers_mt(1);

SELECT CAST(1 + 1, Float64);
SELECT CAST(CAST(1 + 1 + 1, String) AS Int8);

SELECT CAST(Null as Int64); -- {ErrorCode 1010}
SELECT CAST(Null as Varchar); -- {ErrorCode 1010}

-- Null can only be cast successfully to type boolean(false)
SELECT CAST(Null as Boolean); -- {ErrorCode 1010};
SELECT CAST('33' as signed) = 33;
SELECT CAST('33' as unsigned) = 33;
SELECT CAST('-33aa' as signed) = 33; -- {ErrorCode 1010}
SELECT CAST('33 aa' as unsigned) = 33; -- {ErrorCode 1010}
SELECT CAST('-33' as unsigned) = 0; -- {ErrorCode 1010}
SELECT CAST('aa' as unsigned) = 0; -- {ErrorCode 1010}
SELECT CAST('aa' as Float64) = 0; -- {ErrorCode 1010}
SELECT '33'::signed = 33;
SELECT '33'::unsigned = 33;
SELECT '-33aa'::signed = 33; -- {ErrorCode 1010}
SELECT 33::string = '33';

select "truE"::boolean;
select not "FalSe"::boolean;
select "false"::boolean = not "true"::boolean;
select "FalSex"::boolean; -- {ErrorCode 1010}


SELECT '===DATE/DATETIME===';
SELECT  toDateTime('2021-03-05 01:01:01') + 1 = toDateTime('2021-03-05 01:01:01.000001');
SELECT  toDate('2021-03-05') + 1 = toDate('2021-03-06');
SELECT  toString(toDate('2021-03-05') + 1) = '2021-03-06';
SELECT toDateTime(toDate('2021-03-05')) = toDateTime('2021-03-05 00:00:00');
SELECT toDate(toDateTime('2021-03-05 01:00:00')) = toDate('2021-03-05');
SELECT toString(toDateTime(1640019661000000)) = '2021-12-20 17:01:01.000000';
SELECT toDate(toDateTime(1640019661000000)) = toDate('2021-12-20');
SELECT toDateTime(toDateTime(1640019661000000)) = toDateTime('2021-12-20 17:01:01.000000');

SELECT '===Variant===';
SELECT parse_json(true)::boolean;
SELECT parse_json(false)::boolean;
SELECT parse_json('"true"')::boolean;
SELECT parse_json('"false"')::boolean;
SELECT parse_json('"test"')::boolean; -- {ErrorCode 1010}
SELECT parse_json(1)::boolean; -- {ErrorCode 1010}
SELECT parse_json('null')::boolean; -- {ErrorCode 1010}
SELECT parse_json(255)::uint8;
SELECT parse_json(65535)::uint16;
SELECT parse_json(4294967295)::uint32;
SELECT parse_json(18446744073709551615)::uint64;
SELECT parse_json(-128)::int8;
SELECT parse_json(127)::int8;
SELECT parse_json(-32768)::int16;
SELECT parse_json(32767)::int16;
SELECT parse_json(-2147483648)::int32;
SELECT parse_json(2147483647)::int32;
SELECT parse_json(-9223372036854775808)::int64;
SELECT parse_json(9223372036854775807)::int64;
SELECT parse_json('"255"')::uint8;
SELECT parse_json('"65535"')::uint16;
SELECT parse_json('"4294967295"')::uint32;
SELECT parse_json('"18446744073709551615"')::uint64;
SELECT parse_json('"-128"')::int8;
SELECT parse_json('"127"')::int8;
SELECT parse_json('"-32768"')::int16;
SELECT parse_json('"32767"')::int16;
SELECT parse_json('"-2147483648"')::int32;
SELECT parse_json('"2147483647"')::int32;
SELECT parse_json('"-9223372036854775808"')::int64;
SELECT parse_json('"9223372036854775807"')::int64;
SELECT parse_json('"test"')::uint64; -- {ErrorCode 1010}
SELECT parse_json('"test"')::int64; -- {ErrorCode 1010}
SELECT parse_json('null')::int64; -- {ErrorCode 1010}
SELECT parse_json(12.34)::float32;
SELECT parse_json(1234.5678)::float64;
SELECT parse_json('"12.34"')::float32;
SELECT parse_json('"1234.5678"')::float64;
SELECT parse_json('"test"')::float32; -- {ErrorCode 1010}
SELECT parse_json('"test"')::float64; -- {ErrorCode 1010}
SELECT parse_json('null')::float64; -- {ErrorCode 1010}
SELECT parse_json('"2022-01-01"')::date;
SELECT parse_json('"2022-01-01 01:01:01"')::datetime(0);
SELECT parse_json('"test"')::date; -- {ErrorCode 1010}
SELECT parse_json('"test"')::datetime; -- {ErrorCode 1010}
SELECT parse_json('null')::datetime; -- {ErrorCode 1010}
SELECT parse_json('[1,2,3]')::array;
SELECT parse_json(1)::array;
SELECT parse_json('"ab"')::array;
SELECT parse_json('null')::array; -- {ErrorCode 1010}
SELECT parse_json('{"a":1,"b":2}')::object;
SELECT parse_json('"abc"')::object; -- {ErrorCode 1010}
SELECT parse_json('[1,2,3]')::object; -- {ErrorCode 1010}
SELECT parse_json('null')::object; -- {ErrorCode 1010}
