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
use databend_query::sql::Planner;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_engines_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // show engines.
    {
        let query = "show engines";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "SelectInterpreterV2");
        let stream = executor.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+--------+-----------------------------+",
            "| Engine | Comment                     |",
            "+--------+-----------------------------+",
            "| FUSE   | FUSE Storage Engine         |",
            "| MEMORY | MEMORY Storage Engine       |",
            "| NULL   | NULL Storage Engine         |",
            "| RANDOM | RANDOM Storage Engine       |",
            "| VIEW   | VIEW STORAGE (LOGICAL VIEW) |",
            "+--------+-----------------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
