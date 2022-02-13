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
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::Compression;
use common_meta_types::FileFormat;
use common_meta_types::Format;
use databend_query::interpreters::*;
use databend_query::sql::*;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_stage_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;
    let tenant = ctx.get_tenant();

    let query = "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test'";
    {
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        executor.execute(None).await?;
        let stage = ctx
            .get_user_manager()
            .get_stage(&tenant, "test_stage")
            .await?;

        assert_eq!(stage.file_format, FileFormat {
            format: Format::Csv,
            compression: Compression::Gzip,
            ..Default::default()
        });
        assert_eq!(stage.comments, String::from("test"));
    }

    // IF NOT EXISTS.
    {
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        executor.execute(None).await?;

        let stage = ctx
            .get_user_manager()
            .get_stage(&tenant, "test_stage")
            .await?;

        assert_eq!(stage.file_format, FileFormat {
            format: Format::Csv,
            compression: Compression::Gzip,
            ..Default::default()
        });
        assert_eq!(stage.comments, String::from("test"));
    }

    {
        let query = "CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        let r = executor.execute(None).await;
        assert!(r.is_err());
        let e = r.err();
        assert_eq!(e.unwrap().code(), ErrorCode::stage_already_exists_code());

        let stage = ctx
            .get_user_manager()
            .get_stage(&tenant, "test_stage")
            .await?;

        assert_eq!(stage.file_format, FileFormat {
            format: Format::Csv,
            compression: Compression::Gzip,
            ..Default::default()
        });
        assert_eq!(stage.comments, String::from("test"));
    }
    Ok(())
}
