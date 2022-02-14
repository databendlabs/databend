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
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_engines_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    // show engines.
    {
        let plan = PlanParser::parse(ctx.clone(), "show engines").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowEnginesInterpreter");
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+--------+-----------------------+",
            "| Engine | Comment               |",
            "+--------+-----------------------+",
            "| FUSE   | FUSE Storage Engine   |",
            "| GITHUB | GITHUB Storage Engine |",
            "| MEMORY | MEMORY Storage Engine |",
            "| NULL   | NULL Storage Engine   |",
            "+--------+-----------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
