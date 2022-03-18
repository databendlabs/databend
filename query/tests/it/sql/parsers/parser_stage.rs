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

use std::collections::HashMap;

use common_exception::Result;
use databend_query::sql::statements::DfCreateUserStage;
use databend_query::sql::statements::DfList;
use databend_query::sql::*;

use crate::sql::sql_parser::*;

#[test]
fn create_stage_test() -> Result<()> {
    expect_parse_ok(
        "CREATE STAGE test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateUserStage {
            if_not_exists: false,
            stage_name: "test_stage".to_string(),
            location: "s3://load/files/".to_string(),
            credential_options: HashMap::from([
                ("aws_key_id".to_string(), "1a2b3c".to_string()),
                ("aws_secret_key".to_string(), "4x5y6z".to_string())
            ]),
          ..Default::default()
        }),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z')",
        DfStatement::CreateStage(DfCreateUserStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            location: "s3://load/files/".to_string(),
            credential_options: HashMap::from([
                ("aws_key_id".to_string(), "1a2b3c".to_string()),
                ("aws_secret_key".to_string(), "4x5y6z".to_string())
            ]),
          ..Default::default()}),
    )?;

    expect_parse_ok(
        "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',')",
        DfStatement::CreateStage(DfCreateUserStage {
            if_not_exists: true,
            stage_name: "test_stage".to_string(),
            location: "s3://load/files/".to_string(),
            credential_options: HashMap::from([
                ("aws_key_id".to_string(), "1a2b3c".to_string()),
                ("aws_secret_key".to_string(), "4x5y6z".to_string())
            ]),
            file_format_options: HashMap::from([
                ("format".to_string(), "CSV".to_string()),
                ("compression".to_string(), "GZIP".to_string()),
                ("record_delimiter".to_string(), ",".to_string()),
            ]),
          ..Default::default()}),
    )?;

    expect_parse_ok(
        "list @abc pattern = '*.csv'",
        DfStatement::List(DfList {
            location: "@abc".to_string(),
            pattern: "*.csv".to_string(),
        }),
    )?;
    Ok(())
}
