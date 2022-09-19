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
use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::InsertPlan;
use common_legacy_planners::PlanNode::Insert;
use databend_query::interpreters::*;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::*;
use futures::TryStreamExt;

use crate::tests::ConfigBuilder;

pub async fn build_insert_plan(sql: &str, ctx: Arc<QueryContext>) -> Result<InsertPlan> {
    let plan = PlanParser::parse(ctx.clone(), sql).await?;
    match plan {
        Insert(insert_plan) => Ok(insert_plan),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_async_insert_queue() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;

    AsyncInsertManager::instance().start().await;
    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
            .await?;
        AsyncInsertManager::instance()
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
        let queue1 = AsyncInsertManager::instance();
        let queue2 = AsyncInsertManager::instance();

        let handler1 = GlobalIORuntime::instance().spawn(async move {
            let insert_plan =
                build_insert_plan("insert into default.test(a) values(1);", context1.clone())
                    .await?;
            queue1
                .clone()
                .push(context1.clone(), Arc::new(insert_plan.to_owned()))
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

        let handler2 = GlobalIORuntime::instance().spawn(async move {
            let insert_plan = build_insert_plan(
                "insert into default.test(b) values('bbbb');",
                context2.clone(),
            )
            .await?;
            queue2
                .clone()
                .push(context2.clone(), Arc::new(insert_plan.to_owned()))
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
        let stream = executor.execute(ctx.clone()).await?;
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
    let (_guard, ctx) = crate::tests::create_query_context_with_config(
        ConfigBuilder::create()
            .async_insert_max_data_size(1)
            .build(),
        None,
    )
    .await?;

    AsyncInsertManager::instance().start().await;

    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
            .await?;
        AsyncInsertManager::instance()
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
    let (_guard, ctx) = crate::tests::create_query_context_with_config(
        ConfigBuilder::create()
            .async_insert_busy_timeout(900)
            .build(),
        None,
    )
    .await?;

    AsyncInsertManager::instance().start().await;
    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
            .await?;
        AsyncInsertManager::instance()
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
    let (_guard, ctx) = crate::tests::create_query_context_with_config(
        ConfigBuilder::create()
            .async_insert_busy_timeout(900)
            .async_insert_stale_timeout(300)
            .build(),
        None,
    )
    .await?;

    AsyncInsertManager::instance().start().await;
    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;

        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
            .await?;
        AsyncInsertManager::instance()
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
    let (_guard, ctx) = crate::tests::create_query_context_with_config(
        ConfigBuilder::create()
            .async_insert_busy_timeout(2000)
            .build(),
        None,
    )
    .await?;

    AsyncInsertManager::instance().start().await;
    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;
        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
            .await?;
        let res = AsyncInsertManager::instance()
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
    let (_guard, ctx) = crate::tests::create_query_context().await?;

    AsyncInsertManager::instance().start().await;
    let mut planner = Planner::new(ctx.clone());

    // Create table
    {
        let query = "create table default.test(a int, b String) Engine = Memory;";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    let now = SystemTime::now();

    // Insert into table
    {
        let insert_plan =
            build_insert_plan("insert into default.test values(1, 'aaa');", ctx.clone()).await?;
        AsyncInsertManager::instance()
            .clone()
            .push(ctx.clone(), Arc::new(insert_plan.to_owned()))
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
        let stream = executor.execute(ctx.clone()).await?;
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
