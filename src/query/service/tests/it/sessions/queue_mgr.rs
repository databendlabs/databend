// Copyright 2021 Datafuse Labs
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
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryEntry;
use databend_query::sessions::QueueData;
use databend_query::sessions::QueueManager;
use databend_query::test_kits::TestFixture;
use log::error;

#[derive(Debug)]
struct TestData<const PASSED: bool = false>(String);

impl<const PASSED: bool> QueueData for TestData<PASSED> {
    type Key = String;

    fn get_key(&self) -> Self::Key {
        self.0.clone()
    }

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode {
        ErrorCode::Internal(format!("{:?}", key))
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(1000)
    }

    fn need_acquire_to_queue(&self) -> bool {
        !PASSED
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_passed_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData<true>>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData::<true>(format!("TestData{}", index)))
                    .await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<()>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() < Duration::from_secs(test_count as u64));
    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_serial_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<()>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() >= Duration::from_secs(test_count as u64));
    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(2);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;

                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<()>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() >= Duration::from_secs((test_count / 2) as u64));
    assert!(instant.elapsed() < Duration::from_secs((test_count) as u64));

    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;

                tokio::time::sleep(Duration::from_secs(10)).await;
                Result::<()>::Ok(())
            })
        })
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.length(), test_count - 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_heavy_actions() -> Result<()> {
    struct Query {
        sql: &'static str,
        add_to_queue: bool,
    }

    let queries = [
        Query {
            sql: "create table t1(a int)", // DDL
            add_to_queue: false,
        },
        Query {
            sql: "select 1",
            add_to_queue: false,
        },
        Query {
            sql: "select version()", // FUNCTION
            add_to_queue: false,
        },
        Query {
            sql: "select * from numbers(10)", // Table Function
            add_to_queue: false,
        },
        Query {
            sql: "begin", // BEGIN
            add_to_queue: false,
        },
        Query {
            sql: "kill query 9", // KILL
            add_to_queue: false,
        },
        Query {
            sql: "select * from system.one", // SYSTEM
            add_to_queue: false,
        },
        Query {
            sql: "explain select * from system.one", // EXPLAIN SYSTEM
            add_to_queue: false,
        },
        Query {
            sql: "explain select * from t1", // EXPLAIN SELECT
            add_to_queue: false,
        },
        Query {
            sql: "insert into t1 values(1)", // INSERT
            add_to_queue: true,
        },
        Query {
            sql: "copy into t1 from @s1", // COPY INTO
            add_to_queue: true,
        },
        Query {
            sql: "copy into @s1 from (select * from t1)", // COPY INTO
            add_to_queue: true,
        },
        Query {
            sql: "update t1 set a = 2", // UPDATE
            add_to_queue: true,
        },
        Query {
            sql: "delete from t1", // DELETE
            add_to_queue: true,
        },
        Query {
            sql: "replace into t1 on(a) values(1)", // REPLACE INTO
            add_to_queue: true,
        },
        Query {
            sql: "optimize table t1 compact",
            add_to_queue: true,
        },
        Query {
            sql: "vacuum table t",
            add_to_queue: true,
        },
        Query {
            sql: "vacuum temporary files",
            add_to_queue: true,
        },
        Query {
            sql: "truncate table t",
            add_to_queue: true,
        },
        Query {
            sql: "drop table t",
            add_to_queue: false,
        },
        Query {
            sql: "drop table t all",
            add_to_queue: true,
        },
        Query {
            sql: "merge into t1 using (select * from t2) as t2 on t1.a = t2.a when matched then delete",
            // MERGE INTO
            add_to_queue: true,
        },
    ];

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Create table and stage.
    {
        let mut planner = Planner::new(ctx.clone());

        {
            let sql = "create table t1(a int)";
            let (plan, _extras) = planner.plan_sql(sql).await?;
            let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
            let _ = interpreter.execute(ctx.clone()).await?;
        }
        {
            let sql = "create table t2(a int)";
            let (plan, _extras) = planner.plan_sql(sql).await?;
            let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
            let _ = interpreter.execute(ctx.clone()).await?;
        }
        {
            let sql = "create stage s1";
            let (plan, _extras) = planner.plan_sql(sql).await?;
            let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
            let _ = interpreter.execute(ctx.clone()).await?;
        }
    }

    // Check.
    for query in queries.iter() {
        let mut planner = Planner::new(ctx.clone());
        let (plan, extras) = planner.plan_sql(query.sql).await?;

        let query_entry = QueryEntry::create(&ctx, &plan, &extras)?;
        if query.add_to_queue != query_entry.need_acquire_to_queue() {
            error!(
                "query: {:?}, query-entry: {:?}",
                query.sql,
                query_entry.need_acquire_to_queue()
            );
        }
        assert_eq!(query.add_to_queue, query_entry.need_acquire_to_queue());
    }

    Ok(())
}
