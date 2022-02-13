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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_users_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    {
        let query = "CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // show users.
    {
        let plan = PlanParser::parse(ctx.clone(), "show users").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowUsersInterpreter");

        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
                "+------+-----------+-----------------+------------------------------------------------------------------+",
                "| name | hostname  | auth_type       | auth_string                                                      |",
                "+------+-----------+-----------------+------------------------------------------------------------------+",
                "| test | localhost | sha256_password | 5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 |",
                "+------+-----------+-----------------+------------------------------------------------------------------+",
            ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
