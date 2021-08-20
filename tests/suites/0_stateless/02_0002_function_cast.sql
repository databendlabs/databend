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

SELECT CAST(Null as Int64);
SELECT CAST(Null as Boolean);
SELECT CAST(Null as Varchar);
