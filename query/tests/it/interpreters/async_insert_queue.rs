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

use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::TrySpawn;
use common_exception::Result;
use common_planners::PlanNode::Insert;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;

use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_async_insert_queue() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();
    let session_manager = SessionManagerBuilder::create_with_conf(conf.clone()).build()?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // Insert into table
    {
        let query = "insert into default.test values(1, 'aaa');";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let insert_plan = match plan {
            Insert(insert_plan) => insert_plan,
            _ => unreachable!(),
        };

        let async_insert_queue = session_manager
            .clone()
            .get_async_insert_queue()
            .read()
            .clone()
            .unwrap();

        {
            {
                let mut queue = async_insert_queue.session_mgr.write();
                *queue = Some(session_manager.clone());
            }
            async_insert_queue.clone().start().await;
        }

        {
            let queue = session_manager
                .get_async_insert_queue()
                .read()
                .clone()
                .unwrap();
            queue
                .clone()
                .push(Arc::new(insert_plan.to_owned()), ctx.clone())
                .await?;
            queue
                .clone()
                .wait_for_processing_insert(
                    ctx.get_id(),
                    tokio::time::Duration::from_secs(
                        ctx.get_config().query.wait_for_async_insert_timeout,
                    ),
                )
                .await?;
        }
    }

    // Parallel insert into table
    {
        let async_insert_queue = session_manager
            .clone()
            .get_async_insert_queue()
            .read()
            .clone()
            .unwrap();

        {
            let mut queue = async_insert_queue.session_mgr.write();
            *queue = Some(session_manager.clone());
            async_insert_queue.clone().start().await;
        }

        let context1 = ctx.clone();
        let context2 = ctx.clone();
        let sessions1 = session_manager.clone();
        let sessions2 = session_manager.clone();

        let handler1 = context1.get_storage_runtime().spawn(async move {
            let query = "insert into default.test(a) values(1);";
            let plan = PlanParser::parse(context1.clone(), query).await?;
            let insert_plan = match plan {
                Insert(insert_plan) => insert_plan,
                _ => unreachable!(),
            };
            let queue = sessions1
                .clone()
                .get_async_insert_queue()
                .read()
                .clone()
                .unwrap();
            queue
                .clone()
                .push(Arc::new(insert_plan.to_owned()), context1.clone())
                .await?;
            queue
                .clone()
                .wait_for_processing_insert(
                    context1.get_id(),
                    tokio::time::Duration::from_secs(
                        context1.get_config().query.wait_for_async_insert_timeout,
                    ),
                )
                .await
        });

        let handler2 = context2.clone().get_storage_runtime().spawn(async move {
            let query = "insert into default.test(b) values('bbbb');";
            let plan = PlanParser::parse(context2.clone(), query).await?;
            let insert_plan = match plan {
                Insert(insert_plan) => insert_plan,
                _ => unreachable!(),
            };
            let queue = sessions2
                .clone()
                .get_async_insert_queue()
                .read()
                .clone()
                .unwrap();
            queue
                .clone()
                .push(Arc::new(insert_plan.to_owned()), context2.clone())
                .await?;
            queue
                .clone()
                .wait_for_processing_insert(
                    context2.get_id(),
                    tokio::time::Duration::from_secs(
                        context2.get_config().query.wait_for_async_insert_timeout,
                    ),
                )
                .await
        });

        handler1.await.unwrap()?;
        handler2.await.unwrap()?;
    }

    // Select
    {
        let query = "select * from default.test";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---+------+",
            "| a | b    |",
            "+---+------+",
            "| 0 | bbbb |",
            "| 1 |      |",
            "| 1 | aaa  |",
            "+---+------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
