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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::Compression;
use common_meta_types::FileFormat;
use common_planners::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_stage_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;

    static TEST_QUERY: &str = "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',') comments='test'";
    if let PlanNode::CreateUserStage(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
        let executor = CreatStageInterpreter::try_create(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreatStageInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
        let stage = ctx
            .get_sessions_manager()
            .get_user_manager()
            .get_stage("test_stage")
            .await?;

        assert_eq!(
            stage.file_format,
            Some(FileFormat::Csv {
                compression: Compression::Gzip,
                record_delimiter: ",".to_string()
            })
        );
        assert_eq!(stage.comments, String::from("test"))
    } else {
        panic!()
    }

    if let PlanNode::CreateUserStage(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
        let executor = CreatStageInterpreter::try_create(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreatStageInterpreter");
        let is_err = executor.execute(None).await.is_err();
        assert!(!is_err);
        let stage = ctx
            .get_sessions_manager()
            .get_user_manager()
            .get_stage("test_stage")
            .await?;

        assert_eq!(
            stage.file_format,
            Some(FileFormat::Csv {
                compression: Compression::Gzip,
                record_delimiter: ",".to_string()
            })
        );
        assert_eq!(stage.comments, String::from("test"))
    } else {
        panic!()
    }

    static TEST_QUERY1: &str = "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter=',') comments='test'";
    if let PlanNode::CreateUserStage(plan) = PlanParser::parse(TEST_QUERY1, ctx.clone()).await? {
        let executor = CreatStageInterpreter::try_create(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreatStageInterpreter");
        let is_err = executor.execute(None).await.is_err();
        assert!(is_err);
        let stage = ctx
            .get_sessions_manager()
            .get_user_manager()
            .get_stage("test_stage")
            .await?;

        assert_eq!(
            stage.file_format,
            Some(FileFormat::Csv {
                compression: Compression::Gzip,
                record_delimiter: ",".to_string()
            })
        );
        assert_eq!(stage.comments, String::from("test"))
    } else {
        panic!()
    }
    Ok(())
}
