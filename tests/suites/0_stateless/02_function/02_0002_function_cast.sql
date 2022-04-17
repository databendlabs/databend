SELECT toTypeName(CAST(number AS float)) FROM numbers_mt(1);
SELECT toTypeName(CAST(number AS float32)) FROM numbers_mt(1);
SELECT toTypeName(CAST(number AS UInt64)) FROM numbers_mt(1);
SELECT toTypeName(toint8('8')) FROM numbers_mt(1);
SELECT toTypeName(toint16('16')) FROM numbers_mt(1);
SELECT toTypeName(toint32('32')) FROM numbers_mt(1);
SELECT toTypeName(toint64('64')) FROM numbers_mt(1);
SELECT toTypeName(toUInt32('64')) FROM numbers_mt(1);
SELECT toTypeName(number::float) FROM numbers_mt(1);
SELECT toTypeName(number::float64) FROM numbers_mt(1);
SELECT toTypeName(number::UInt64) FROM numbers_mt(1);

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
SELECT '33'::signed = 33;
SELECT '33'::unsigned = 33;
SELECT '-33aa'::signed = 33; -- {ErrorCode 1010}
SELECT 33::string = '33';

SELECT '===DATE/DATETIME===';
SELECT  toDateTime('2021-03-05 01:01:01') + 1 = toDateTime('2021-03-05 01:01:02');
SELECT  toDate('2021-03-05') + 1 = toDate('2021-03-06');
SELECT  toString(toDate('2021-03-05') + 1) = '2021-03-06';
SELECT toDateTime(toDate('2021-03-05')) = toDateTime('2021-03-05 00:00:00');
SELECT toDate(toDateTime('2021-03-05 01:00:00')) = toDate('2021-03-05');
SELECT toString(toDateTime64(1640019661000)) = '2021-12-20 17:01:01.000';
SELECT toDate(toDateTime64(1640019661000)) = toDate('2021-12-20');
SELECT toDateTime(toDateTime64(1640019661000)) = toDateTime('2021-12-20 17:01:01');

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
SELECT parse_json('"2022-01-01"')::date16;
SELECT parse_json('"2022-01-01"')::date32;
SELECT parse_json('"2022-01-01 01:01:01"')::datetime32;
SELECT parse_json('"2022-01-01 01:01:01.123"')::datetime64;
SELECT parse_json('"test"')::date16; -- {ErrorCode 1010}
SELECT parse_json('"test"')::date32; -- {ErrorCode 1010}
SELECT parse_json('"test"')::datetime32; -- {ErrorCode 1010}
SELECT parse_json('"test"')::datetime64; -- {ErrorCode 1010}
SELECT parse_json('null')::datetime64; -- {ErrorCode 1010}
SELECT parse_json('[1,2,3]')::array;
SELECT parse_json(1)::array;
SELECT parse_json('"ab"')::array;
SELECT parse_json('null')::array; -- {ErrorCode 1010}
SELECT parse_json('{"a":1,"b":2}')::object;
SELECT parse_json('"abc"')::object; -- {ErrorCode 1010}
SELECT parse_json('[1,2,3]')::object; -- {ErrorCode 1010}
SELECT parse_json('null')::object; -- {ErrorCode 1010}
