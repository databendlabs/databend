-- TENANT1
SUDO USE TENANT 'tenant1';

-- stage check.
CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test';

-- TENANT2
SUDO USE TENANT 'tenant2';
-- TODO: Add show stages
DROP STAGE test_stage; -- {ErrorCode 2501}


-- TENANT1
SUDO USE TENANT 'tenant1';
DROP STAGE test_stage;
