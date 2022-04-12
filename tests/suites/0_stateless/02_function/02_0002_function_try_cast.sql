SELECT try_cast(1 + 1, Float64);
SELECT try_cast(try_cast(1 + 1 + 1, String) AS Int8);

SELECT try_cast(Null as Int64);
SELECT try_cast(Null as Varchar);

SELECT try_cast(Null as Boolean);
SELECT try_cast('33' as signed) = 33;
SELECT try_cast('33' as unsigned) = 33;
SELECT try_cast('-33aa' as signed) is null;
SELECT try_cast('33 aa' as unsigned) is null;
SELECT try_cast('-33' as unsigned) is null;
SELECT try_cast('aa' as unsigned) is null;
SELECT try_cast('aa' as Float64) is null;

SELECT try_cast(parse_json('null') as float64) is null;

SELECT try_cast(parse_json('"test"') as int32) is null;
SELECT try_cast(parse_json('123') as int32)  = 123;

