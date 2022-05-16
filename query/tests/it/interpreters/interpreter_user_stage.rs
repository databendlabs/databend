// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::*;
use futures::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_stage_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    // add
    {
        let query =
            "CREATE STAGE test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z')";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
    }

    // desc
    {
        let query = "DESC STAGE test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DescribeUserStageInterpreter");

        let mut stream = executor.execute(None).await?;
        let mut blocks = vec![];

        while let Some(block) = stream.next().await {
            blocks.push(block?);
        }

        common_datablocks::assert_blocks_eq(
            vec![
                "+------------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
                "| name       | stage_type | stage_params                                                                                                                                                                                                       | copy_options                                  | file_format_options                                                                                                | comment |",
                "+------------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
                r#"| test_stage | External   | StageParams { storage: S3(StorageS3Config { endpoint_url: "https://s3.amazonaws.com", region: "", bucket: "load", root: "/files/", access_key_id: "******b3c", secret_access_key: "******y6z", master_key: "" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |"#,
                "+------------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
            ],
            &blocks,
        );
    }

    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();
    let stage = user_mgr.get_stage(&tenant, "test_stage").await;
    assert!(stage.is_ok());

    // drop
    {
        let query = "DROP STAGE if exists test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "DropUserStageInterpreter");

        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
    }

    let stage = user_mgr.get_stage(&tenant, "test_stage").await;
    assert!(stage.is_err());
    Ok(())
}
