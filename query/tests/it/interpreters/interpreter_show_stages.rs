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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::StreamExt;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_stages_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    {
        let query =
            "CREATE STAGE test url='s3://load/files/' credentials=(aws_key_id='1a2b3c' aws_secret_key='4x5y6z')";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
    }

    // show stages.
    {
        let plan = PlanParser::parse(ctx.clone(), "show stages").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowStagesInterpreter");

        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------+------------+-----------------+--------------------+---------+",
            "| name | stage_type | number_of_files | creator            | comment |",
            "+------+------------+-----------------+--------------------+---------+",
            "| test | External   | NULL            | 'root'@'127.0.0.1' |         |",
            "+------+------------+-----------------+--------------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
