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
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::dump_backtrace;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> anyhow::Result<()> {
    let counter = Arc::new(Mutex::new(0));

    let runtime = Runtime::with_default_worker_threads(Some("runtime-test".to_string()))?;
    let runtime_counter = Arc::clone(&counter);
    let runtime_header = runtime.spawn(async move {
        let rt1 =
            Runtime::with_default_worker_threads(Some("runtime-test-rt1".to_string())).unwrap();
        let rt1_counter = Arc::clone(&runtime_counter);
        let rt1_header = rt1.spawn(async move {
            let rt2 =
                Runtime::with_worker_threads(1, Some("runtime-test-rt2".to_string())).unwrap();
            let rt2_counter = Arc::clone(&rt1_counter);
            let rt2_header = rt2.spawn(async move {
                let rt3 =
                    Runtime::with_default_worker_threads(Some("runtime-test-rt3".to_string()))
                        .unwrap();
                let rt3_counter = Arc::clone(&rt2_counter);
                let rt3_header = rt3.spawn(async move {
                    let mut num = rt3_counter.lock().unwrap();
                    *num += 1;
                });
                rt3_header.await.unwrap();

                let mut num = rt2_counter.lock().unwrap();
                *num += 1;
            });
            rt2_header.await.unwrap();

            let mut num = rt1_counter.lock().unwrap();
            *num += 1;
        });
        rt1_header.await.unwrap();

        let mut num = runtime_counter.lock().unwrap();
        *num += 1;
    });
    runtime_header.await.unwrap();

    let result = *counter.lock().unwrap();
    assert_eq!(result, 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_long_run_runtime() -> anyhow::Result<()> {
    let runtime = Runtime::with_default_worker_threads(Some("runtime-shutdown-test".to_string()))?;

    runtime.spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
    });

    let instant = Instant::now();
    drop(runtime);
    assert!(instant.elapsed() < Duration::from_secs(6));

    Ok(())
}

struct RuntimeOwner {
    _runtime: Arc<Runtime>,
}

#[test]
fn test_runtime_can_be_dropped_from_own_worker() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(
        1,
        Some("drop-runtime-from-own-worker".to_string()),
    )?);
    let owner = Arc::new(RuntimeOwner {
        _runtime: runtime.clone(),
    });
    let owner_for_task = owner.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner_for_task);
        let _ = done_tx.send(());
    });

    drop(owner);
    drop(runtime);
    release_tx.send(())?;

    done_rx.recv_timeout(Duration::from_secs(2))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_runtime_task_dump_contains_runtime_id() -> anyhow::Result<()> {
    let runtime = Runtime::with_worker_threads(1, Some("task-marker-runtime".to_string()))?;
    let runtime_marker = format!("[task-marker-runtime id={}]", runtime.inner().id());
    let (started_tx, started_rx) = std::sync::mpsc::channel();

    let named_started_tx = started_tx.clone();
    let named_spawn_line = line!() + 1;
    runtime.spawn_named(
        async move {
            named_started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        },
        "task-marker-test".to_string(),
    );
    let global_spawn_line = line!() + 1;
    runtime.spawn(async move {
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started_rx.recv_timeout(Duration::from_secs(1))?;
    started_rx.recv_timeout(Duration::from_secs(1))?;

    let dump = dump_backtrace(false);
    assert!(dump.contains(&format!(
        "{} task-marker-test at {}:{}:",
        runtime_marker,
        file!(),
        named_spawn_line
    )));
    assert!(dump.contains(&format!(
        "{} Global spawn task at {}:{}:",
        runtime_marker,
        file!(),
        global_spawn_line
    )));
    assert!(!dump.contains("task-marker-test at src/common/base/src/runtime/runtime.rs"));
    assert!(!dump.contains("Global spawn task at src/common/base/src/runtime/runtime.rs"));

    Ok(())
}

#[test]
fn test_free_spawn_task_dump_contains_runtime_id() -> anyhow::Result<()> {
    let runtime = Runtime::with_worker_threads(1, Some("free-spawn-runtime-test".to_string()))?;
    let runtime_marker = format!("[free-spawn-runtime-test id={}]", runtime.inner().id());

    runtime.block_on(async {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let free_spawn_line = line!() + 1;
        databend_common_base::runtime::spawn(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.map_err(|err| {
            databend_common_exception::ErrorCode::Internal(format!(
                "free spawn marker task did not start: {err}"
            ))
        })?;

        let dump = dump_backtrace(false);
        assert!(dump.lines().any(|line| {
            line.contains(&runtime_marker)
                && line.contains(&format!(" at {}:{}:", file!(), free_spawn_line))
        }));
        assert!(!dump.contains("[spawn-thread="));

        Ok::<(), databend_common_exception::ErrorCode>(())
    })?;

    Ok(())
}

static START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

// println can more clearly know if they are parallel
async fn mock_get_page(i: usize) -> Vec<usize> {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] > get_page({}) will complete in {} ms, {:?}",
        START_TIME.elapsed().as_millis(),
        i,
        millis,
        std::thread::current().id(),
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] < get_page({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );

    (i..(i + 1)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime_try_spawn_batch() -> anyhow::Result<()> {
    let runtime = Runtime::with_default_worker_threads(Some("runtime-batch-test".to_string()))?;

    let mut futs = vec![];
    for i in 0..20 {
        futs.push(mock_get_page(i));
    }

    let max_concurrency = Semaphore::new(3);
    let handlers = runtime.try_spawn_batch(max_concurrency, futs).await?;
    let result = futures::future::try_join_all(handlers).await.unwrap();
    assert_eq!(result.len(), 20);
    Ok(())
}
