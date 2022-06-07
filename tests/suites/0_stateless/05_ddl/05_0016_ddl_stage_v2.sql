set enable_planner_v2 = 1;
CREATE STAGE test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z');
CREATE STAGE if not exists test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' aws_secret_key='4x5y6z');
CREATE STAGE test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z');  -- {ErrorCode 2502}

CREATE STAGE test_stage_internal file_format=(type=csv compression=AUTO record_delimiter=NONE) comments='test';


LIST @test_stage_internal;
desc stage test_stage_internal;
SHOW STAGES;


DROP STAGE test_stage;
DROP STAGE test_stage_internal;

desc stage test_stage_internal; -- {ErrorCode 2501}
SHOW STAGES;

set enable_planner_v2 = 0;
