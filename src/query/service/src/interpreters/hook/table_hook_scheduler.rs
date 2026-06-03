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

// Logs from this module will show up as "[TABLE-HOOK-SCHEDULER] ...".
databend_common_tracing::register_module_tag!("[TABLE-HOOK-SCHEDULER]");

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use dashmap::DashSet;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use log::info;
use log::warn;
use tokio::sync::Semaphore;

use crate::interpreters::hook::analyze_hook::AnalyzeDesc;
use crate::interpreters::hook::analyze_hook::execute_analyze_hook;
use crate::interpreters::hook::compact_hook::CompactHookTraceCtx;
use crate::interpreters::hook::compact_hook::CompactTargetTableDescription;
use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::compact_hook::execute_compact_hook;
use crate::interpreters::hook::refresh_hook::RefreshDesc;
use crate::interpreters::hook::refresh_hook::execute_refresh_hook;
use crate::sessions::QueryContext;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TableHookTaskKey {
    catalog: String,
    table_id: u64,
}

pub struct TableHookTask {
    pub ctx: Arc<QueryContext>,
    pub table_id: u64,
    pub compact_target: CompactTargetTableDescription,
    pub lock_opt: LockTableOption,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

impl TableHookTask {
    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            catalog: self.compact_target.catalog.clone(),
            table_id: self.table_id,
        }
    }
}

trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;
}

impl SchedulerTask for TableHookTask {
    fn key(&self) -> TableHookTaskKey {
        self.key()
    }
}

type SchedulerFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type SchedulerRunner<T> = Arc<dyn Fn(T) -> SchedulerFuture + Send + Sync>;
type SchedulerSpawner = Arc<dyn Fn(SchedulerFuture, String) + Send + Sync>;

pub struct TableHookScheduler {
    core: Arc<SchedulerCore<TableHookTask>>,
}

struct SchedulerCore<T> {
    sender: Sender<T>,
    inflight: Arc<DashSet<TableHookTaskKey>>,
}

impl TableHookScheduler {
    pub fn init(max_concurrency: usize) -> Result<()> {
        let runner: SchedulerRunner<TableHookTask> =
            Arc::new(|task| Box::pin(Self::execute_task(task)));
        let spawner: SchedulerSpawner = Arc::new(|future, name| {
            GlobalIORuntime::instance().spawn_named(future, name);
        });
        let core = SchedulerCore::create(max_concurrency, runner, spawner);
        let scheduler = Arc::new(Self { core });

        GlobalInstance::set(scheduler.clone());
        Ok(())
    }

    pub fn try_instance() -> Option<Arc<TableHookScheduler>> {
        GlobalInstance::try_get()
    }

    pub fn is_async_enabled() -> bool {
        GlobalConfig::try_get_instance()
            .map(|config| config.query.common.table_hook_mode == "async")
            .unwrap_or(false)
    }

    pub fn enqueue(&self, task: TableHookTask) {
        self.core.enqueue(task);
    }

    async fn execute_task(task: TableHookTask) {
        info!(
            "Start async table hook job for operation {}, table_id={}, table={}",
            task.operation_name, task.table_id, task.compact_target.table
        );

        Self::execute_compact(&task).await;
        Self::execute_refresh(&task).await;
        Self::execute_analyze(&task).await;

        info!(
            "Async table hook job completed for operation {}, table_id={}, table={}",
            task.operation_name, task.table_id, task.compact_target.table
        );
    }

    async fn execute_compact(task: &TableHookTask) {
        if !compact_after_write_enabled(&task.ctx) {
            return;
        }

        execute_compact_hook(
            task.ctx.clone(),
            task.compact_target.clone(),
            CompactHookTraceCtx {
                start: task.main_operation_start,
                operation_name: task.operation_name.clone(),
            },
            task.lock_opt.clone(),
        )
        .await
        .ok();
    }

    async fn execute_refresh(task: &TableHookTask) {
        execute_refresh_hook(task.ctx.clone(), RefreshDesc {
            catalog: task.compact_target.catalog.clone(),
            database: task.compact_target.database.clone(),
            table: task.compact_target.table.clone(),
        })
        .await
        .ok();
    }

    async fn execute_analyze(task: &TableHookTask) {
        execute_analyze_hook(task.ctx.clone(), AnalyzeDesc {
            catalog: task.compact_target.catalog.clone(),
            database: task.compact_target.database.clone(),
            table: task.compact_target.table.clone(),
        })
        .await
        .ok();
    }
}

impl<T: SchedulerTask> SchedulerCore<T> {
    fn create(
        max_concurrency: usize,
        runner: SchedulerRunner<T>,
        spawner: SchedulerSpawner,
    ) -> Arc<Self> {
        let (sender, receiver) = async_channel::unbounded();
        let scheduler = Arc::new(Self {
            sender,
            inflight: Arc::new(DashSet::new()),
        });

        spawner(
            Box::pin(Self::dispatch(
                receiver,
                scheduler.inflight.clone(),
                Arc::new(Semaphore::new(max_concurrency)),
                runner,
                spawner.clone(),
            )),
            "table-hook-scheduler".to_string(),
        );

        scheduler
    }

    fn enqueue(&self, task: T) {
        let key = task.key();
        if !self.inflight.insert(key.clone()) {
            info!(
                "Skip async table hook because table {} in catalog {} already has a pending job",
                key.table_id, key.catalog
            );
            return;
        }

        if self.sender.try_send(task).is_err() {
            self.inflight.remove(&key);
            warn!(
                "Skip async table hook because scheduler is closed, table_id={}, catalog={}",
                key.table_id, key.catalog
            );
        }
    }

    async fn dispatch(
        receiver: Receiver<T>,
        inflight: Arc<DashSet<TableHookTaskKey>>,
        semaphore: Arc<Semaphore>,
        runner: SchedulerRunner<T>,
        spawner: SchedulerSpawner,
    ) {
        while let Ok(task) = receiver.recv().await {
            let key = task.key();
            let inflight = inflight.clone();
            let runner = runner.clone();
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    inflight.remove(&key);
                    warn!(
                        "Skip async table hook because scheduler semaphore is closed, table_id={}, catalog={}",
                        key.table_id, key.catalog
                    );
                    continue;
                }
            };

            spawner(
                Box::pin(async move {
                    let _permit = permit;
                    runner(task).await;
                    inflight.remove(&key);
                }),
                "table-hook-job".to_string(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::sleep;
    use tokio::time::timeout;

    use super::*;

    #[derive(Clone)]
    struct TestTask {
        catalog: String,
        table_id: u64,
    }

    impl SchedulerTask for TestTask {
        fn key(&self) -> TableHookTaskKey {
            TableHookTaskKey {
                catalog: self.catalog.clone(),
                table_id: self.table_id,
            }
        }
    }

    fn test_task(table_id: u64) -> TestTask {
        TestTask {
            catalog: "default".to_string(),
            table_id,
        }
    }

    fn tokio_spawner() -> SchedulerSpawner {
        Arc::new(|future, _name| {
            tokio::spawn(future);
        })
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..100 {
            if predicate() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(predicate());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_max_concurrency_limits_running_jobs() {
        let first_started = Arc::new(Notify::new());
        let second_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let second_started = second_started.clone();
            let release_first = release_first.clone();
            let completed = completed.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let second_started = second_started.clone();
                let release_first = release_first.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    match task.table_id {
                        1 => {
                            first_started.notify_one();
                            release_first.notified().await;
                        }
                        2 => {
                            second_started.notify_one();
                        }
                        _ => {}
                    }
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task(1));
        scheduler.enqueue(test_task(2));

        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");
        assert!(
            timeout(Duration::from_millis(100), second_started.notified())
                .await
                .is_err(),
            "second task should wait for the concurrency permit"
        );

        release_first.notify_one();
        timeout(Duration::from_secs(1), second_started.notified())
            .await
            .expect("second task should start after first task completes");
        wait_until(|| completed.load(Ordering::SeqCst) == 2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_duplicate_table_is_skipped_while_pending() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let started = started.clone();
            let completed = completed.clone();

            Arc::new(move |_task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let started = started.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    first_started.notify_one();
                    release_first.notified().await;
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task(1));
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");

        scheduler.enqueue(test_task(1));
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "duplicate table task should be skipped while the first task is pending"
        );

        release_first.notify_one();
        wait_until(|| completed.load(Ordering::SeqCst) == 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_completed_task_allows_same_table_to_enqueue_again() {
        let started = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let started = started.clone();

            Arc::new(move |_task| {
                let started = started.clone();
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task(1));
        wait_until(|| started.load(Ordering::SeqCst) == 1 && scheduler.inflight.is_empty()).await;

        scheduler.enqueue(test_task(1));
        wait_until(|| started.load(Ordering::SeqCst) == 2 && scheduler.inflight.is_empty()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_queue_accepts_tasks_beyond_concurrency_limit() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let running = running.clone();
            let max_running = max_running.clone();
            let completed = completed.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let running = running.clone();
                let max_running = max_running.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max_running.fetch_max(current, Ordering::SeqCst);

                    if task.table_id == 1 {
                        first_started.notify_one();
                        release_first.notified().await;
                    }

                    running.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());

        for table_id in 1..=32 {
            scheduler.enqueue(test_task(table_id));
        }

        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");
        assert_eq!(
            scheduler.inflight.len(),
            32,
            "all unique table hook tasks should be accepted while concurrency is saturated"
        );
        assert_eq!(
            max_running.load(Ordering::SeqCst),
            1,
            "only one task should run while max concurrency is one"
        );

        release_first.notify_one();
        wait_until(|| completed.load(Ordering::SeqCst) == 32 && scheduler.inflight.is_empty())
            .await;
        assert_eq!(
            max_running.load(Ordering::SeqCst),
            1,
            "queued jobs should continue respecting the concurrency limit"
        );
    }
}
