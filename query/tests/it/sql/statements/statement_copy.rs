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

use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::sql::statements::AnalyzableStatement;
use databend_query::sql::statements::AnalyzedResult;
use databend_query::sql::DfParser;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_statement_copy() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
        err: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "copy-external-passed",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: r#"Copy into system.configs ,stage_plan:UserStagePlan { stage_info: UserStageInfo { stage_name: "", stage_type: External, stage_params: StageParams { location: "s3://mybucket/data/files", storage: S3(StageS3Storage { credentials_aws_key_id: "my_key_id", credentials_aws_secret_key: "my_key_id", encryption_master_key: "my_master_key" }) }, file_format: Csv, copy_option: CopyOptions, comment: "" } }"#,
            err: "",
        },
        TestCase {
            name: "copy-external-table-not-found-failed",
            query: "copy into mytable
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: "",
            err: "Code: 1025, displayText = Unknown table: 'mytable'.",
        },
        TestCase {
            name: "copy-external-uri-not-found-failed",
            query: "copy into system.configs
        from '//mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: "",
            err: "Code: 1005, displayText = File location scheme must be specified.",
        },
        TestCase {
            name: "copy-internal-passed",
            query: "copy into system.configs
        from '@mystage'
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: r#"Copy into system.configs ,stage_plan:UserStagePlan { stage_info: UserStageInfo { stage_name: "", stage_type: Internal, stage_params: StageParams { location: "", storage: S3(StageS3Storage { credentials_aws_key_id: "", credentials_aws_secret_key: "", encryption_master_key: "" }) }, file_format: Csv, copy_option: CopyOptions, comment: "" } }"#,
            err: "",
        },
    ];

    for test in &tests {
        let ctx = create_query_context()?;
        let (mut statements, _) = DfParser::parse_sql(test.query)?;
        let statement = statements.remove(0);
        if test.err.is_empty() {
            let result = statement.analyze(ctx).await?;
            match result {
                AnalyzedResult::SimpleQuery(v) => {
                    assert_eq!(test.expect, format!("{:?}", v), "{}", test.name)
                }
                _ => {
                    return Err(ErrorCode::LogicalError(format!(
                        "Query analyzed must be return SimpleQuery: {:}",
                        test.name
                    )));
                }
            }
        } else {
            let result = statement.analyze(ctx).await;
            assert_eq!(
                test.err,
                format!("{:}", result.err().unwrap()),
                "{}",
                test.name
            )
        }
    }

    Ok(())
}
