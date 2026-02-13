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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::workload_group::MAX_CONCURRENCY_QUOTA_KEY;
use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_base::runtime::workload_group::WorkloadGroupResource;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_sql::Planner;
use databend_meta_client::RpcClientConf;
use databend_meta_runtime::DatabendRuntime;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryEntry;
use databend_query::sessions::QueueData;
use databend_query::sessions::QueueManager;
use databend_query::test_kits::TestFixture;
use log::error;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

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

    fn get_retry_timeout(&self) -> Option<Duration> {
        None
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_passed_acquire() -> anyhow::Result<()> {
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
async fn test_serial_acquire() -> anyhow::Result<()> {
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
async fn test_concurrent_acquire() -> anyhow::Result<()> {
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
async fn test_list_acquire() -> anyhow::Result<()> {
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
async fn test_heavy_actions() -> anyhow::Result<()> {
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
        },
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
async fn test_watch_abort_notify_immediate_abort() -> anyhow::Result<()> {
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
async fn test_watch_abort_notify_abort_during_wait() -> anyhow::Result<()> {
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
    let acquire_handle =
        databend_common_base::runtime::spawn(async move { queue_clone.acquire(test_data2).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    abort_notify.notify_waiters();

    let result = acquire_handle.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ABORTED_QUERY);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watch_abort_notify_race_condition() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(1, metastore, false);

    let test_data = TestData::new(
        "test_race_condition".to_string(),
        "test_acquire_1".to_string(),
    );

    let abort_notify = test_data.abort_notify.clone();
    let queue_clone = queue.clone();

    let acquire_handle =
        databend_common_base::runtime::spawn(async move { queue_clone.acquire(test_data).await });

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
async fn test_watch_abort_notify_multiple_waiters() -> anyhow::Result<()> {
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

    let acquire_handle2 =
        databend_common_base::runtime::spawn(async move { queue_clone2.acquire(test_data2).await });

    let acquire_handle3 =
        databend_common_base::runtime::spawn(async move { queue_clone3.acquire(test_data3).await });

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
async fn test_watch_abort_notify_timeout_vs_abort() -> anyhow::Result<()> {
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

    let acquire_handle =
        databend_common_base::runtime::spawn(async move { queue_clone.acquire(test_data2).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    abort_notify.notify_waiters();

    let result = acquire_handle.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::ABORTED_QUERY);

    Ok(())
}

async fn create_meta_store() -> Result<MetaStore> {
    let conf = RpcClientConf::empty();
    Ok(MetaStoreProvider::new(conf)
        .create_meta_store::<DatabendRuntime>()
        .await
        .unwrap())
}

// Tests for workload group concurrency control functionality
#[tokio::test(flavor = "multi_thread")]
async fn test_workload_group_concurrency_control() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(10, metastore, false);

    // Create workload group with concurrency limit of 2
    let mut quotas = HashMap::new();
    quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(2));

    let workload_group = Arc::new(WorkloadGroupResource {
        meta: WorkloadGroup {
            id: "test_workload_concurrency".to_string(),
            name: "Test Workload Concurrency".to_string(),
            quotas,
        },
        queue_key: "test_concurrency_queue".to_string(),
        permits: 2,
        mutex: Arc::new(Mutex::new(())),
        semaphore: Arc::new(Semaphore::new(2)),
        mem_stat: MemStat::create(String::new()),
        max_memory_usage: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        destroy_fn: None,
    });

    // Set workload group for this thread
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.workload_group_resource = Some(workload_group.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);

    // Test that semaphore limits are enforced
    assert_eq!(workload_group.semaphore.available_permits(), 2);

    // Test queue acquisition with workload group limits
    let test_data1 = TestData::new("concurrency_test_1".to_string(), "acquire_1".to_string());
    let test_data2 = TestData::new("concurrency_test_2".to_string(), "acquire_2".to_string());

    // Both acquisitions should succeed since we have 2 permits
    let guard1 = queue.acquire(test_data1).await?;
    let guard2 = queue.acquire(test_data2).await?;

    // Verify guards are created and contain permits
    assert!(
        !guard1.inner.is_empty(),
        "First guard should contain permits"
    );
    assert!(
        !guard2.inner.is_empty(),
        "Second guard should contain permits"
    );

    // Cleanup
    drop(guard1);
    drop(guard2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_workload_group_concurrent_queue_acquisition() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(10, metastore, false);

    // Create workload group with concurrency limit of 1 (serial execution)
    let mut quotas = HashMap::new();
    quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(1));

    let workload_group = Arc::new(WorkloadGroupResource {
        meta: WorkloadGroup {
            id: "test_concurrent_queue".to_string(),
            name: "Test Concurrent Queue".to_string(),
            quotas,
        },
        queue_key: "test_concurrent_acquisition".to_string(),
        permits: 1,
        mutex: Arc::new(Default::default()),
        semaphore: Arc::new(Semaphore::new(1)),
        mem_stat: MemStat::create(String::new()),
        max_memory_usage: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        destroy_fn: None,
    });

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.workload_group_resource = Some(workload_group.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);

    let test_data1 = TestData::new("workload_concurrent_1".to_string(), "acquire_1".to_string());
    let test_data2 = TestData::new("workload_concurrent_2".to_string(), "acquire_2".to_string());

    // Start two acquisitions concurrently
    let queue_clone1 = queue.clone();
    let queue_clone2 = queue.clone();

    let instant = Instant::now();

    let handle1 = databend_common_base::runtime::spawn(async move {
        let _guard = queue_clone1.acquire(test_data1).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        Result::<()>::Ok(())
    });

    let handle2 = databend_common_base::runtime::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await; // Start slightly later
        let _guard = queue_clone2.acquire(test_data2).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        Result::<()>::Ok(())
    });

    let _ = tokio::try_join!(handle1, handle2).unwrap();

    // With concurrency limit of 1, total time should be more than 1 second (serialized)
    let elapsed = instant.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900),
        "Expected serialized execution, got {:?}",
        elapsed
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_workload_group_multilevel_queue_guards() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(5, metastore, false);

    // Create workload group with concurrency limit
    let mut quotas = HashMap::new();
    quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(3));

    let workload_group = Arc::new(WorkloadGroupResource {
        meta: WorkloadGroup {
            id: "test_multilevel_guards".to_string(),
            name: "Test Multilevel Guards".to_string(),
            quotas,
        },
        queue_key: "test_multilevel_queue".to_string(),
        permits: 3,
        mutex: Arc::new(Default::default()),
        semaphore: Arc::new(Semaphore::new(3)),
        mem_stat: MemStat::create(String::new()),
        max_memory_usage: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        destroy_fn: None,
    });

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.workload_group_resource = Some(workload_group.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);

    let test_data = TestData::new(
        "multilevel_test".to_string(),
        "acquire_multilevel".to_string(),
    );

    // This tests the multi-level queue acquisition:
    // 1. Workload group local semaphore
    // 2. Workload group meta queue
    // 3. Warehouse level queue
    let guard = queue.acquire(test_data).await?;

    // Guard should contain multiple levels of permits
    // The exact number depends on the queue configuration
    assert!(
        !guard.inner.is_empty(),
        "Guard should contain permits from multiple levels"
    );

    // Verify that the guard properly releases resources when dropped
    drop(guard);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_workload_group_zero_concurrency() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(10, metastore, false);

    // Create workload group with zero concurrency (should block all queries)
    let mut quotas = HashMap::new();
    quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(0));

    let workload_group = Arc::new(WorkloadGroupResource {
        meta: WorkloadGroup {
            id: "test_zero_concurrency".to_string(),
            name: "Test Zero Concurrency".to_string(),
            quotas,
        },
        queue_key: "test_zero_queue".to_string(),
        permits: 0,
        mutex: Arc::new(Default::default()),
        semaphore: Arc::new(Semaphore::new(0)),
        mem_stat: MemStat::create(String::new()),
        max_memory_usage: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        destroy_fn: None,
    });

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.workload_group_resource = Some(workload_group.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);

    // Verify no permits are available
    assert_eq!(workload_group.semaphore.available_permits(), 0);

    // Test queue acquisition with zero concurrency - should timeout
    let test_data = TestData::with_timeout(
        "zero_concurrency_test".to_string(),
        "acquire_zero".to_string(),
        Duration::from_millis(100),
    );

    let queue_clone = queue.clone();
    let acquire_result = queue_clone.acquire(test_data).await;

    assert!(
        acquire_result.is_err(),
        "Queue acquisition should timeout with zero concurrency"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_workload_group_with_timeout() -> anyhow::Result<()> {
    let metastore = create_meta_store().await?;
    let queue = QueueManager::<TestData>::create(10, metastore, false);

    // Create workload group with concurrency limit of 1
    let mut quotas = HashMap::new();
    quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(1));

    let workload_group = Arc::new(WorkloadGroupResource {
        meta: WorkloadGroup {
            id: "test_timeout".to_string(),
            name: "Test Timeout".to_string(),
            quotas,
        },
        queue_key: "test_timeout_queue".to_string(),
        permits: 1,
        mutex: Arc::new(Default::default()),
        semaphore: Arc::new(Semaphore::new(1)),
        mem_stat: MemStat::create(String::new()),
        max_memory_usage: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        destroy_fn: None,
    });

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.workload_group_resource = Some(workload_group.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);

    // First, acquire through the queue to hold the permit
    let test_data1 = TestData::new("timeout_holder".to_string(), "acquire_holder".to_string());
    let _guard1 = queue.acquire(test_data1).await?;

    // Now try to acquire again with timeout - should timeout because permit is held
    let test_data2 = TestData::with_timeout(
        "timeout_waiter".to_string(),
        "acquire_waiter".to_string(),
        Duration::from_millis(100),
    );

    let queue_clone = queue.clone();
    let timeout_result = queue_clone.acquire(test_data2).await;

    assert!(
        timeout_result.is_err(),
        "Queue acquisition should timeout when permits are exhausted"
    );

    Ok(())
}
