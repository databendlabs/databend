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
            name: "copy-external-ok",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: r#"Copy into system.configs, ReadDataSourcePlan { catalog: "default", source_info: S3StageSource(UserStageInfo { stage_name: "s3://mybucket/data/files", stage_type: External, stage_params: StageParams { storage: S3(StageS3Storage { bucket: "mybucket", path: "/data/files", credentials_aws_key_id: "my_key_id", credentials_aws_secret_key: "my_secret_key", encryption_master_key: "my_master_key" }) }, file_format_options: FileFormatOptions { format: Csv, skip_header: 1, field_delimiter: "|", record_delimiter: "", compression: None }, copy_options: CopyOptions { on_error: None, size_limit: 0 }, comment: "" }), scan_fields: None, parts: [], statistics: Statistics { read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0, is_exact: false }, description: "", tbl_args: None, push_downs: None } ,validation_mode:None"#,
            err: "",
        },

        TestCase {
            name: "copy-external-validation-mode-ok",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)
        VALIDATION_MODE = RETURN_13_ROWS
        ",
            expect: r#"Copy into system.configs, ReadDataSourcePlan { catalog: "default", source_info: S3StageSource(UserStageInfo { stage_name: "s3://mybucket/data/files", stage_type: External, stage_params: StageParams { storage: S3(StageS3Storage { bucket: "mybucket", path: "/data/files", credentials_aws_key_id: "my_key_id", credentials_aws_secret_key: "my_secret_key", encryption_master_key: "my_master_key" }) }, file_format_options: FileFormatOptions { format: Csv, skip_header: 1, field_delimiter: "|", record_delimiter: "", compression: None }, copy_options: CopyOptions { on_error: None, size_limit: 0 }, comment: "" }), scan_fields: None, parts: [], statistics: Statistics { read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0, is_exact: false }, description: "", tbl_args: None, push_downs: None } ,validation_mode:ReturnNRows(13)"#,
            err: "",
        },

        TestCase {
            name: "copy-external-files-and-validation-mode-ok",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        files = ('file1.csv', 'file2.csv')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)
        VALIDATION_MODE = RETURN_13_ROWS
        ",
            expect: r#"Copy into system.configs, ReadDataSourcePlan { catalog: "default", source_info: S3StageSource(UserStageInfo { stage_name: "s3://mybucket/data/files", stage_type: External, stage_params: StageParams { storage: S3(StageS3Storage { bucket: "mybucket", path: "/data/files", credentials_aws_key_id: "my_key_id", credentials_aws_secret_key: "my_secret_key", encryption_master_key: "my_master_key" }) }, file_format_options: FileFormatOptions { format: Csv, skip_header: 1, field_delimiter: "|", record_delimiter: "", compression: None }, copy_options: CopyOptions { on_error: None, size_limit: 0 }, comment: "" }), scan_fields: None, parts: [], statistics: Statistics { read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0, is_exact: false }, description: "", tbl_args: None, push_downs: None } ,files:["file1.csv", "file2.csv"] ,validation_mode:ReturnNRows(13)"#,
            err: "",
        },

        TestCase {
            name: "copy-external-copy-options-ok",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        files = ('file1.csv', 'file2.csv')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)
        on_error = CONTINUE size_limit = 10
        VALIDATION_MODE = RETURN_13_ROWS
        ",
            expect: r#"Copy into system.configs, ReadDataSourcePlan { catalog: "default", source_info: S3StageSource(UserStageInfo { stage_name: "s3://mybucket/data/files", stage_type: External, stage_params: StageParams { storage: S3(StageS3Storage { bucket: "mybucket", path: "/data/files", credentials_aws_key_id: "my_key_id", credentials_aws_secret_key: "my_secret_key", encryption_master_key: "my_master_key" }) }, file_format_options: FileFormatOptions { format: Csv, skip_header: 1, field_delimiter: "|", record_delimiter: "", compression: None }, copy_options: CopyOptions { on_error: Continue, size_limit: 10 }, comment: "" }), scan_fields: None, parts: [], statistics: Statistics { read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0, is_exact: false }, description: "", tbl_args: None, push_downs: None } ,files:["file1.csv", "file2.csv"] ,validation_mode:ReturnNRows(13)"#,
            err: "",
        },

        TestCase {
            name: "copy-external-size-limit-error",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        files = ('file1.csv', 'file2.csv')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)
        on_error = CONTINUE size_limit = x0
        VALIDATION_MODE = RETURN_13_ROWS
        ",
            expect: "",
            err: "Code: 1005, displayText = size_limit must be number, got: x0.",
        },

        TestCase {
            name: "copy-external-validation-mode-error",
            query: "copy into system.configs
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        files = ('file1.csv', 'file2.csv')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)
        on_error = CONTINUE size_limit = 10
        VALIDATION_MODE = RETURN_1x_ROWS
        ",
            expect: "",
            err: r#"Code: 1005, displayText = Unknown validation mode:"RETURN_1X_ROWS", must one of { RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS}."#,
        },

        TestCase {
            name: "copy-external-table-not-found-error",
            query: "copy into mytable
        from 's3://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: "",
            err: "Code: 1025, displayText = Unknown table 'mytable'.",
        },
        TestCase {
            name: "copy-external-uri-not-found-error",
            query: "copy into system.configs
        from '//mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: "",
            err: "Code: 1005, displayText = File location uri must be specified, for example: 's3://<bucket>[/<path>]'.",
        },
        TestCase {
            name: "copy-external-location-unsupported-error",
            query: "copy into system.configs
        from 's4://mybucket/data/files'
        credentials=(aws_key_id='my_key_id' aws_secret_key='my_secret_key')
        encryption=(master_key = 'my_master_key')
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: "",
            err: "Code: 1005, displayText = File location uri unsupported, must be one of [s3, @stage].",
        },
        TestCase {
            name: "copy-internal-ok",
            query: "copy into system.configs
        from '@mystage'
        file_format = (type = csv field_delimiter = '|' skip_header = 1)",
            expect: r#""#,
            err: "Code: 2501, displayText = Unknown stage mystage.",
        },
    ];

    for test in &tests {
        let ctx = create_query_context().await?;
        let (mut statements, _) =
            DfParser::parse_sql(test.query, ctx.get_current_session().get_type())?;
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
