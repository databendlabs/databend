CREATE FUNCTION test_alter_udf AS (p) -> not(is_null(p));
ALTER FUNCTION test_alter_udf AS (d) -> not(is_not_null(d)) DESC = 'This is a new description';
ALTER FUNCTION test_alter_udf_unknown AS (d) -> not(is_not_null(d)); -- {ErrorCode 2602}
ALTER FUNCTION is_not_null AS (d) -> not(is_null(d)); -- {ErrorCode 2603}
