// Copyright 2020 Datafuse Labs.
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
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_user_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::CreateUser(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'")?
    {
        let executor = CreatUserInterpreter::try_create(ctx, plan.clone())?;
        assert_eq!(executor.name(), "CreateUserInterpreter");
        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        panic!()
    }

    Ok(())
}
