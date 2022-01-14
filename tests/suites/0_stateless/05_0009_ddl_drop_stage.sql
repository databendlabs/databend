DROP STAGE IF EXISTS test_stage;

CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z');
DROP STAGE test_stage;

DROP STAGE IF EXISTS test_stage;

DROP STAGE test_stage; -- {ErrorCode 2501}
