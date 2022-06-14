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
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use common_base::base::tokio;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertPlan;
use common_planners::PlanNode::Insert;
use databend_query::interpreters::*;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sql::*;
use futures::TryStreamExt;

use crate::tests::SessionManagerBuilder;

pub async fn build_async_insert_queue(
    max_data_size: Option<u64>,
    busy_timeout: Option<u64>,
    stale_timeout: Option<u64>,
) -> Result<(Arc<SessionManager>, Arc<AsyncInsertQueue>)> {
    let mut conf = crate::tests::ConfigBuilder::create().config();
    if let Some(max_data_size) = max_data_size {
        conf.query.async_insert_max_data_size = max_data_size
    }
    if let Some(busy_timeout) = busy_timeout {
        conf.query.async_insert_busy_timeout = busy_timeout;
    }
    if let Some(stale_timeout) = stale_timeout {
        conf.query.async_insert_stale_timeout = stale_timeout;
    }
    let session_manager = SessionManagerBuilder::create_with_conf(conf.clone()).build()?;

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

    Ok((session_manager.clone(), async_insert_queue.clone()))
}

pub async fn build_insert_plan(sql: &str, ctx: Arc<QueryContext>) -> Result<InsertPlan> {
    let plan = PlanParser::parse(ctx.clone(), sql).await?;
    match plan {
        Insert(insert_plan) => Ok(insert_plan),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_async_insert_queue() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(None, None, None).await?;
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
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
        queue
            .clone()
            .wait_for_processing_insert(
                ctx.get_id(),
                tokio::time::Duration::from_secs(
                    ctx.get_settings().get_wait_for_async_insert_timeout()?,
                ),
            )
            .await?;
    }

    // Parallel insert into table
    {
        let context1 = ctx.clone();
        let context2 = ctx.clone();
        let queue1 = queue.clone();
        let queue2 = queue.clone();

        let handler1 = context1.get_storage_runtime().spawn(async move {
            let insert_plan =
                build_insert_plan("insert into default.test(a) values(1);", context1.clone())
                    .await?;
            queue1
                .clone()
                .push(Arc::new(insert_plan.to_owned()), context1.clone())
                .await?;
            queue1
                .clone()
                .wait_for_processing_insert(
                    context1.get_id(),
                    tokio::time::Duration::from_secs(
                        context1
                            .get_settings()
                            .get_wait_for_async_insert_timeout()?,
                    ),
                )
                .await
        });

        let handler2 = context2.clone().get_storage_runtime().spawn(async move {
            let insert_plan = build_insert_plan(
                "insert into default.test(b) values('bbbb');",
                context2.clone(),
            )
            .await?;
            queue2
                .clone()
                .push(Arc::new(insert_plan.to_owned()), context2.clone())
                .await?;
            queue2
                .clone()
                .wait_for_processing_insert(
                    context2.get_id(),
                    tokio::time::Duration::from_secs(
                        context2
                            .get_settings()
                            .get_wait_for_async_insert_timeout()?,
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

#[tokio::test]
async fn test_async_insert_queue_max_data_size() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(Some(1), None, None).await?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
        queue
            .clone()
            .wait_for_processing_insert(
                ctx.get_id(),
                tokio::time::Duration::from_secs(
                    ctx.get_settings().get_wait_for_async_insert_timeout()?,
                ),
            )
            .await?;
    }

    let execution_time = SystemTime::now().duration_since(now).unwrap().as_millis();

    assert!(execution_time < 200);

    Ok(())
}

#[tokio::test]
async fn test_async_insert_queue_busy_timeout() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(None, Some(900), None).await?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
        queue
            .clone()
            .wait_for_processing_insert(
                ctx.get_id(),
                tokio::time::Duration::from_secs(
                    ctx.get_settings().get_wait_for_async_insert_timeout()?,
                ),
            )
            .await?;
    }

    let execution_time = SystemTime::now().duration_since(now).unwrap().as_millis();

    assert!(execution_time < 1000);

    Ok(())
}

#[tokio::test]
async fn test_async_insert_queue_stale_timeout() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(None, Some(900), Some(300)).await?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
        queue
            .clone()
            .wait_for_processing_insert(
                ctx.get_id(),
                tokio::time::Duration::from_secs(
                    ctx.get_settings().get_wait_for_async_insert_timeout()?,
                ),
            )
            .await?;
    }

    let execution_time = SystemTime::now().duration_since(now).unwrap().as_millis();

    assert!(execution_time > 300 && execution_time < 1000);

    Ok(())
}

#[tokio::test]
async fn test_async_insert_queue_wait_timeout() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(None, Some(2000), None).await?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;
        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
        let res = queue
            .clone()
            .wait_for_processing_insert(ctx.get_id(), tokio::time::Duration::from_secs(1))
            .await;

        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::AsyncInsertTimeoutError("").code()
        );
    }

    let execution_time = SystemTime::now().duration_since(now).unwrap().as_millis();

    assert!(execution_time > 1000 && execution_time < 2000);

    Ok(())
}

#[tokio::test]
async fn test_async_insert_queue_no_wait() -> Result<()> {
    let (session_manager, queue) = build_async_insert_queue(None, None, None).await?;
    let ctx = crate::tests::create_query_context_with_session(session_manager.clone()).await?;

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;
        queue
            .clone()
            .push(Arc::new(insert_plan.to_owned()), ctx.clone())
            .await?;
    }

    let execution_time = SystemTime::now().duration_since(now).unwrap().as_millis();

    assert!(execution_time < 200);

    thread::sleep(Duration::from_millis(300));

    // Select
    {
        let query = "select * from default.test";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| 1 | aaa |",
            "+---+-----+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
