// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_exception::Result;
use common_meta_types::Compression;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::Format;
use common_meta_types::StageParams;
use databend_query::sql::statements::DfCreateStage;
use databend_query::sql::statements::DfDropStage;
use databend_query::sql::*;

use crate::sql::sql_parser::*;

#[test]
fn create_stage_test() -> Result<()> {
    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: FileFormat::default(),
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format: FileFormat::default(),
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',')",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { compression: Compression::Gzip, record_delimiter: ",".to_string(),..Default::default()},
            comments: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',') comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { compression: Compression::Gzip, record_delimiter: ",".to_string(),..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=Parquet compression=AUTO) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Parquet, compression: Compression::Auto ,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Csv, compression: Compression::Auto,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=json) comments='test'",
        DfStatement::CreateStage(DfCreateStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            stage_params: StageParams::new("s3://load/files/", Credentials { access_key_id: "1a2b3c".to_string(), secret_access_key: "4x5y6z".to_string() }),
            file_format:  FileFormat { format: Format::Json,..Default::default()},
            comments: "test".to_string(),
        }),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Missing URL"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' password=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Expected end of statement, found: password"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s4://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Not supported storage"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Invalid credentials options: unknown field `access_key`, expected `access_key_id` or `secret_access_key`"),
    )?;

    expect_parse_err(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' aecret_access_key='4x5y6z') file_format=(FORMAT=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("sql parser error: Invalid credentials options: unknown field `aecret_access_key`, expected `access_key_id` or `secret_access_key`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(type=csv compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("unknown field `type`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv compression=AUTO1 record_delimiter=NONE) comments='test'",
        String::from("unknown variant `auto1`"),
    )?;

    expect_parse_err_contains(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(format=csv1 compression=AUTO record_delimiter=NONE) comments='test'",
        String::from("unknown variant `csv1`"),
    )?;

    Ok(())
}

#[test]
fn drop_stage_test() -> Result<()> {
    expect_parse_ok(
        "DROP STAGE test_stage",
        DfStatement::DropStage(DfDropStage {
            if_exists: false,
            stage_name: "test_stage".to_string(),
        }),
    )?;

    expect_parse_ok(
        "DROP STAGE IF EXISTS test_stage",
        DfStatement::DropStage(DfDropStage {
            if_exists: true,
            stage_name: "test_stage".to_string(),
        }),
    )?;

    Ok(())
}
