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

use databend_common_base::base::WatchNotify;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryEntry;
use databend_query::sessions::QueueData;
use databend_query::sessions::QueueManager;
use databend_query::test_kits::TestFixture;
use log::error;

struct TestData<const PASSED: bool = false> {
    lock_id: String,
    acquire_id: String,
    abort_notify: Arc<WatchNotify>,
    test_timeout: Duration,
}

impl<const PASSED: bool> std::fmt::Debug for TestData<PASSED> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestData")
            .field("lock_id", &self.lock_id)
            .field("acquire_id", &self.acquire_id)
            .field("test_timeout", &self.test_timeout)
            .finish()
    }
}

impl<const PASSED: bool> TestData<PASSED> {
    fn new(lock_id: String, acquire_id: String) -> Self {
        Self {
            lock_id,
            acquire_id,
            abort_notify: Arc::new(WatchNotify::new()),
            test_timeout: Duration::from_secs(1000),
        }
    }

    fn with_timeout(lock_id: String, acquire_id: String, timeout: Duration) -> Self {
        Self {
            lock_id,
            acquire_id,
            abort_notify: Arc::new(WatchNotify::new()),
            test_timeout: timeout,
        }
    }
}

impl<const PASSED: bool> QueueData for TestData<PASSED> {
    type Key = String;

    fn get_key(&self) -> Self::Key {
        self.acquire_id.clone()
    }

    fn get_lock_key(&self) -> String {
        self.lock_id.clone()
    }

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode {
        ErrorCode::Internal(format!("{:?}", key))
    }

    fn lock_ttl(&self) -> Duration {
        Duration::from_secs(3)
    }

    fn timeout(&self) -> Duration {
        self.test_timeout
    }

    fn need_acquire_to_queue(&self) -> bool {
        !PASSED
    }

    fn get_abort_notify(&self) -> Arc<WatchNotify> {
        self.abort_notify.clone()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_passed_acquire() -> Result<()> {
    for is_global in [true, false] {
        let metastore = create_meta_store().await?;
        let test_count = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 5) as usize
            + 5;

        let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
        let queue = QueueManager::<TestData<true>>::create(1, metastore, is_global);
        let mut join_handles = Vec::with_capacity(test_count);

        let instant = Instant::now();
        for index in 0..test_count {
            join_handles.push({
                let queue = queue.clone();
                let barrier = barrier.clone();
                databend_common_base::runtime::spawn(async move {
                    barrier.wait().await;
                    let _guard = queue
                        .acquire(TestData::<true>::new(
                            String::from("test_passed_acquire"),
                            format!("TestData{}", index),
                        ))
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
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_serial_acquire() -> Result<()> {
    for is_global in [true, false] {
        let metastore = create_meta_store().await?;
        let test_count = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 5) as usize
            + 5;

        let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
        let queue = QueueManager::<TestData>::create(1, metastore, is_global);
        let mut join_handles = Vec::with_capacity(test_count);

        let instant = Instant::now();
        for index in 0..test_count {
            join_handles.push({
                let queue = queue.clone();
                let barrier = barrier.clone();
                databend_common_base::runtime::spawn(async move {
                    barrier.wait().await;

                    // Time based semaphore is sensitive to time accuracy.
                    // Lower timestamp semaphore being inserted after higher timestamp semaphore results in both acquired.
                    // Thus, we have to make the gap between timestamp large enough.
                    tokio::time::sleep(Duration::from_millis(500 * index as u64)).await;

                    let _guard = queue
                        .acquire(TestData::new(
                            String::from("test_serial_acquire"),
                            format!("TestData{}", index),
                        ))
                        .await?;

                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                    Result::<()>::Ok(())
                })
            })
        }

        for join_handle in join_handles {
            let _ = join_handle.await;
        }

        let elapsed = instant.elapsed();
        let expected = Duration::from_secs(test_count as u64);
        assert!(
            elapsed >= expected,
            "expect: elapsed: {:?} >= {:?}, ",
            elapsed,
            expected,
        );
        assert_eq!(queue.length(), 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_acquire() -> Result<()> {
    for is_global in [true, false] {
        let metastore = create_meta_store().await?;
        let test_count = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 5) as usize
            + 5;

        let ctx = format!("count={test_count}");

        let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
        let queue = QueueManager::<TestData>::create(2, metastore, is_global);
        let mut join_handles = Vec::with_capacity(test_count);

        let instant = Instant::now();
        for index in 0..test_count {
            join_handles.push({
                let queue = queue.clone();
                let barrier = barrier.clone();
                databend_common_base::runtime::spawn(async move {
                    barrier.wait().await;

                    // Time based semaphore is sensitive to time accuracy.
                    // Lower timestamp semaphore being inserted after higher timestamp semaphore results in both acquired.
                    // Thus, we have to make the gap between timestamp large enough.
                    tokio::time::sleep(Duration::from_millis(300 * index as u64)).await;

                    let _guard = queue
                        .acquire(TestData::new(
                            String::from("test_concurrent_acquire"),
                            format!("TestData{}", index),
                        ))
                        .await?;

                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Result::<()>::Ok(())
                })
            })
        }

        for join_handle in join_handles {
            let _ = join_handle.await;
        }

        let elapsed = instant.elapsed();
        let total = Duration::from_secs((test_count) as u64);
        assert!(
            elapsed >= total / 2,
            "{ctx}: expect: elapsed: {:?} >= {:?}, ",
            elapsed,
            total / 2,
        );
        let delta = Duration::from_millis(300) * test_count as u32;
        assert!(
            elapsed < total + delta,
            "{ctx}: expect: elapsed: {:?} < {:?} + {:?}, ",
            elapsed,
            total,
            delta,
        );

        assert_eq!(queue.length(), 0);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_acquire() -> Result<()> {
    for is_global in [true, false] {
        let metastore = create_meta_store().await?;
        let test_count = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 5) as usize
            + 5;

        let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
        let queue = QueueManager::<TestData>::create(1, metastore, is_global);
        let mut join_handles = Vec::with_capacity(test_count);

        for index in 0..test_count {
            join_handles.push({
                let queue = queue.clone();
                let barrier = barrier.clone();
                databend_common_base::runtime::spawn(async move {
                    barrier.wait().await;

                    // Time based semaphore is sensitive to time accuracy.
                    // Lower timestamp semaphore being inserted after higher timestamp semaphore results in both acquired.
                    // Thus, we have to make the gap between timestamp large enough.
                    tokio::time::sleep(Duration::from_millis(800 * index as u64)).await;

                    let _guard = queue
                        .acquire(TestData::new(
                            String::from("test_list_acquire"),
                            format!("TestData{}", index),
                        ))
                        .await?;

                    tokio::time::sleep(Duration::from_secs(15)).await;
                    Result::<()>::Ok(())
                })
            })
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(queue.length(), test_count - 1);
    }

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
        Query {
            sql: "CREATE TABLE test_heavy_create AS SELECT 1",
            add_to_queue: true,
        }
    ];

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_enable_table_lock(0)?;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_immediate_abort() -> Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);
    
    let test_data1 = TestData::new(
        "test_immediate_abort".to_string(),
        "test_acquire_1".to_string(),
    );
    let test_data2 = TestData::new(
        "test_immediate_abort".to_string(),
        "test_acquire_2".to_string(),
    );
    
    let _guard1 = queue.acquire(test_data1).await?;
    
    test_data2.abort_notify.notify_waiters();
    
    let result = queue.acquire(test_data2).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_abort_during_wait() -> Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);
    
    let test_data1 = TestData::new(
        "test_abort_during_wait".to_string(),
        "test_acquire_1".to_string(),
    );
    let test_data2 = TestData::new(
        "test_abort_during_wait".to_string(),
        "test_acquire_2".to_string(),
    );
    
    let _guard1 = queue.acquire(test_data1).await?;
    
    let abort_notify = test_data2.abort_notify.clone();
    let queue_clone = queue.clone();
    let acquire_handle = databend_common_base::runtime::spawn(async move {
        queue_clone.acquire(test_data2).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    abort_notify.notify_waiters();
    
    let result = acquire_handle.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_race_condition() -> Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);
    
    let test_data = TestData::new(
        "test_race_condition".to_string(),
        "test_acquire_1".to_string(),
    );
    
    let abort_notify = test_data.abort_notify.clone();
    let queue_clone = queue.clone();
    
    let acquire_handle = databend_common_base::runtime::spawn(async move {
        queue_clone.acquire(test_data).await
    });
    
    let abort_handle = databend_common_base::runtime::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        abort_notify.notify_waiters();
    });
    
    let (acquire_result, _) = tokio::join!(acquire_handle, abort_handle);
    let result = acquire_result.unwrap();
    
    match result {
        Ok(_guard) => {
            assert_eq!(queue.length(), 0);
        }
        Err(err) => {
            assert_eq!(err.code(), ErrorCode::ABORTED_QUERY);
        }
    }
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_multiple_waiters() -> Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);
    
    let test_data1 = TestData::new(
        "test_multiple_waiters".to_string(),
        "test_acquire_1".to_string(),
    );
    let test_data2 = TestData::new(
        "test_multiple_waiters".to_string(),
        "test_acquire_2".to_string(),
    );
    let test_data3 = TestData::new(
        "test_multiple_waiters".to_string(),
        "test_acquire_3".to_string(),
    );
    
    let _guard1 = queue.acquire(test_data1).await?;
    
    let abort_notify2 = test_data2.abort_notify.clone();
    let abort_notify3 = test_data3.abort_notify.clone();
    let queue_clone2 = queue.clone();
    let queue_clone3 = queue.clone();
    
    let acquire_handle2 = databend_common_base::runtime::spawn(async move {
        queue_clone2.acquire(test_data2).await
    });
    
    let acquire_handle3 = databend_common_base::runtime::spawn(async move {
        queue_clone3.acquire(test_data3).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    abort_notify2.notify_waiters();
    
    let result2 = acquire_handle2.await.unwrap();
    assert!(result2.is_err());
    assert_eq!(result2.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
    
    abort_notify3.notify_waiters();
    
    let result3 = acquire_handle3.await.unwrap();
    assert!(result3.is_err());
    assert_eq!(result3.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_timeout_vs_abort() -> Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);
    
    let test_data1 = TestData::new(
        "test_timeout_vs_abort".to_string(),
        "test_acquire_1".to_string(),
    );
    let test_data2 = TestData::with_timeout(
        "test_timeout_vs_abort".to_string(),
        "test_acquire_2".to_string(),
        Duration::from_millis(200),
    );
    
    let _guard1 = queue.acquire(test_data1).await?;
    
    let abort_notify = test_data2.abort_notify.clone();
    let queue_clone = queue.clone();
    
    let acquire_handle = databend_common_base::runtime::spawn(async move {
        queue_clone.acquire(test_data2).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    abort_notify.notify_waiters();
    
    let result = acquire_handle.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ABORTED_QUERY);
    
    Ok(())
}

async fn create_meta_store() -> Result<MetaStore> {
    Ok(MetaStoreProvider::new(Default::default())
        .create_meta_store()
        .await
        .unwrap())
}
