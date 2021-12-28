CREATE FUNCTION test_alter_udf AS not(isnull(@0));
ALTER FUNCTION test_alter_udf AS not(isnotnull(@0)) desc='This is a new description';
ALTER FUNCTION test_alter_udf_unknown AS not(isnotnull(@0)); -- {ErrorCode 4071}
ALTER FUNCTION isnotnull AS not(isnull(@0)); -- {ErrorCode 4072}
