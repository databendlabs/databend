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
use common_planners::*;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::tests::parse_query;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_create_udf_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    static CREATE_UDF: &str =
        "CREATE FUNCTION IF NOT EXISTS isnotempty='not(isnull(@0))' desc='This is a description'";

    if let PlanNode::CreateUDF(plan) = PlanParser::parse(CREATE_UDF, ctx.clone()).await? {
        let executor = CreatUDFInterpreter::try_create(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreatUDFInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
        let udf = ctx
            .get_sessions_manager()
            .get_user_manager()
            .get_udf("isnotempty")
            .await?;

        assert_eq!(udf.name, "isnotempty");
        assert_eq!(udf.definition, "not(isnull(@0))");
        assert_eq!(udf.description, "This is a description")
    } else {
        panic!()
    }

    if let PlanNode::ShowUDF(plan) = parse_query("show function isnotnull", &ctx)? {
        let executor = ShowUDFInterpreter::try_create(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowUDFInterpreter");
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------------+-----------------+-----------------------+",
            "| name       | definition      | description           |",
            "+------------+-----------------+-----------------------+",
            "| isnotempty | not(isnull(@0)) | This is a description |",
            "+------------+-----------------+-----------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    } else {
        panic!();
    }

    Ok(())
}
