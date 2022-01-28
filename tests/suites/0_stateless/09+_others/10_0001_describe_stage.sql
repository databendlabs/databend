CREATE STAGE IF NOT EXISTS test_desc_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',');
desc stage test_desc_stage;
