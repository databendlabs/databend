CREATE FUNCTION test_alter_udf AS (p) -> not(isnull(p));
ALTER FUNCTION test_alter_udf AS (d) -> not(isnotnull(d)) DESC = 'This is a new description';
ALTER FUNCTION test_alter_udf_unknown AS (d) -> not(isnotnull(d)); -- {ErrorCode 2602}
ALTER FUNCTION isnotnull AS (d) -> not(isnull(d)); -- {ErrorCode 2603}
