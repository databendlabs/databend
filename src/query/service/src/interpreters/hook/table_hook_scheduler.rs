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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use log::info;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
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
use crate::sessions::TableContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;

pub struct TableHookTask {
    pub ctx: Arc<QueryContext>,
    pub table_id: u64,
    pub compact_target: CompactTargetTableDescription,
    pub hook_settings: TableHookTaskSettings,
    pub lock_opt: LockTableOption,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

#[derive(Clone, Copy)]
pub struct TableHookTaskSettings {
    pub compact_after_write: bool,
    pub refresh_aggregating_index_after_write: bool,
}

impl TableHookTaskSettings {
    pub fn create(ctx: &Arc<QueryContext>) -> Self {
        Self {
            compact_after_write: compact_after_write_enabled(ctx),
            refresh_aggregating_index_after_write: ctx
                .get_settings()
                .get_enable_refresh_aggregating_index_after_write()
                .unwrap_or_else(|e| {
                    warn!(
                        "Failed to retrieve refresh aggregating index settings, continuing without refresh aggregating index: {}",
                        e
                    );
                    false
                }),
        }
    }

    fn merge(&mut self, other: Self) {
        self.compact_after_write |= other.compact_after_write;
        self.refresh_aggregating_index_after_write |= other.refresh_aggregating_index_after_write;
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TableHookTaskKey {
    catalog: String,
    table_id: u64,
}

trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;

    fn merge(&mut self, other: Self);
}

impl SchedulerTask for TableHookTask {
    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            catalog: self.compact_target.catalog.clone(),
            table_id: self.table_id,
        }
    }

    fn merge(&mut self, mut other: Self) {
        other.hook_settings.merge(self.hook_settings);

        other
            .ctx
            .written_segment_locations()
            .extend(self.ctx.written_segment_locations().list());

        let compaction_num_block_hint = self
            .ctx
            .get_compaction_num_block_hint(&self.compact_target.table)
            .saturating_add(
                other
                    .ctx
                    .get_compaction_num_block_hint(&other.compact_target.table),
            );
        if compaction_num_block_hint > 0 {
            other.ctx.set_compaction_num_block_hint(
                &other.compact_target.table,
                compaction_num_block_hint,
            );
        }

        if self.ctx.get_enable_auto_analyze() {
            other.ctx.set_enable_auto_analyze(true);
        }

        other.main_operation_start = self.main_operation_start;
        other.operation_name = self.operation_name.clone();

        *self = other;
    }
}

type SchedulerFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type SchedulerRunner<T> = Arc<dyn Fn(T) -> SchedulerFuture + Send + Sync>;
type SchedulerSpawner = Arc<dyn Fn(SchedulerFuture, String) + Send + Sync>;

pub struct TableHookScheduler {
    core: Arc<SchedulerCore<TableHookTask>>,
}

struct SchedulerCore<T> {
    sender: Sender<TableHookTaskKey>,
    states: Mutex<HashMap<TableHookTaskKey, TableHookState<T>>>,
    runner: SchedulerRunner<T>,
    spawner: SchedulerSpawner,
    semaphore: Arc<Semaphore>,
}

enum TableHookState<T> {
    Queued(T),
    Running { pending: Option<T> },
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
        if !task.hook_settings.compact_after_write {
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
            table_id: Some(task.table_id),
            enable_refresh_aggregating_index_after_write: Some(
                task.hook_settings.refresh_aggregating_index_after_write,
            ),
        })
        .await
        .ok();
    }

    async fn execute_analyze(task: &TableHookTask) {
        execute_analyze_hook(task.ctx.clone(), AnalyzeDesc {
            catalog: task.compact_target.catalog.clone(),
            database: task.compact_target.database.clone(),
            table: task.compact_target.table.clone(),
            table_id: Some(task.table_id),
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
            states: Mutex::new(HashMap::new()),
            runner,
            spawner: spawner.clone(),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
        });

        spawner(
            Box::pin(Self::dispatch(receiver, scheduler.clone())),
            "table-hook-scheduler".to_string(),
        );

        scheduler
    }

    fn enqueue(&self, task: T) {
        let key = task.key();
        let should_schedule = {
            let mut states = self.states.lock();
            match states.get_mut(&key) {
                Some(TableHookState::Queued(pending)) => {
                    pending.merge(task);
                    false
                }
                Some(TableHookState::Running { pending }) => {
                    match pending {
                        Some(pending) => pending.merge(task),
                        None => *pending = Some(task),
                    }
                    false
                }
                None => {
                    states.insert(key.clone(), TableHookState::Queued(task));
                    true
                }
            }
        };

        if should_schedule {
            self.try_schedule_key(key);
        }
    }

    async fn dispatch(receiver: Receiver<TableHookTaskKey>, scheduler: Arc<Self>) {
        while let Ok(key) = receiver.recv().await {
            let permit = match scheduler.semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    warn!(
                        "Skip async table hook because scheduler semaphore is closed, table_id={}, catalog={}",
                        key.table_id, key.catalog,
                    );
                    continue;
                }
            };

            (scheduler.spawner)(
                Box::pin(Self::run_table_job(scheduler.clone(), key, permit)),
                "table-hook-job".to_string(),
            );
        }
    }

    async fn run_table_job(
        scheduler: Arc<Self>,
        key: TableHookTaskKey,
        permit: OwnedSemaphorePermit,
    ) {
        let Some(task) = scheduler.take_pending_task(&key) else {
            drop(permit);
            return;
        };

        (scheduler.runner)(task).await;
        drop(permit);

        scheduler.finish_table_job(key);
    }

    fn take_pending_task(&self, key: &TableHookTaskKey) -> Option<T> {
        let mut states = self.states.lock();
        let state = states.remove(key)?;

        match state {
            TableHookState::Queued(task) => {
                states.insert(key.clone(), TableHookState::Running { pending: None });
                Some(task)
            }
            TableHookState::Running { .. } => {
                states.insert(key.clone(), state);
                None
            }
        }
    }

    fn finish_table_job(&self, key: TableHookTaskKey) {
        let should_reschedule = {
            let mut states = self.states.lock();
            let Some(state) = states.get_mut(&key) else {
                return;
            };

            match state {
                TableHookState::Running { pending } => {
                    if let Some(task) = pending.take() {
                        *state = TableHookState::Queued(task);
                        true
                    } else {
                        states.remove(&key);
                        false
                    }
                }
                TableHookState::Queued(_) => false,
            }
        };

        if should_reschedule {
            self.try_schedule_key(key);
        }
    }

    fn try_schedule_key(&self, key: TableHookTaskKey) {
        if self.sender.try_send(key.clone()).is_ok() {
            return;
        }

        self.mark_schedule_failed(&key);
        warn!(
            "Skip async table hook because scheduler is closed, table_id={}, catalog={}",
            key.table_id, key.catalog,
        );
    }

    fn mark_schedule_failed(&self, key: &TableHookTaskKey) {
        let mut states = self.states.lock();
        if matches!(states.get(key), Some(TableHookState::Queued(_))) {
            states.remove(key);
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
    use crate::interpreters::hook::resolve_current_table_name_by_id;
    use crate::sessions::TableContextTableAccess;
    use crate::test_kits::TestFixture;

    #[derive(Clone)]
    struct TestTask {
        catalog: String,
        database: String,
        table_id: u64,
        payload: Vec<u64>,
    }

    impl SchedulerTask for TestTask {
        fn key(&self) -> TableHookTaskKey {
            TableHookTaskKey {
                catalog: self.catalog.clone(),
                table_id: self.table_id,
            }
        }

        fn merge(&mut self, other: Self) {
            self.payload.extend(other.payload);
        }
    }

    fn test_task(table_id: u64) -> TestTask {
        test_task_with_payload(table_id, table_id)
    }

    fn test_task_with_payload(table_id: u64, payload: u64) -> TestTask {
        TestTask {
            catalog: "default".to_string(),
            database: "default".to_string(),
            table_id,
            payload: vec![payload],
        }
    }

    fn tokio_spawner() -> SchedulerSpawner {
        Arc::new(|future, _name| {
            databend_common_base::runtime::spawn(future);
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
    async fn test_same_table_payloads_are_merged_while_running() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let runs = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let runs = runs.clone();
            let completed = completed.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let runs = runs.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    runs.lock().push(task.payload);
                    let completed_index = completed.fetch_add(1, Ordering::SeqCst) + 1;
                    if completed_index == 1 {
                        first_started.notify_one();
                        release_first.notified().await;
                    }
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task_with_payload(1, 1));
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");

        scheduler.enqueue(test_task_with_payload(1, 2));
        scheduler.enqueue(test_task_with_payload(1, 3));
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "same-table payloads should be merged while a job is running"
        );

        release_first.notify_one();
        wait_until(|| completed.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            runs.lock().clone(),
            vec![vec![1], vec![2, 3]],
            "payloads enqueued while the first same-table job is running should be merged into one follow-up job"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_same_table_key_is_stable_across_database_renames() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let runs = Arc::new(Mutex::new(Vec::<(String, Vec<u64>)>::new()));
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let runs = runs.clone();
            let completed = completed.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let runs = runs.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    runs.lock().push((task.database, task.payload));
                    let completed_index = completed.fetch_add(1, Ordering::SeqCst) + 1;
                    if completed_index == 1 {
                        first_started.notify_one();
                        release_first.notified().await;
                    }
                })
            })
        };

        let scheduler = SchedulerCore::create(2, runner, tokio_spawner());
        let mut old_db_task = test_task_with_payload(1, 1);
        old_db_task.database = "old_db".to_string();
        scheduler.enqueue(old_db_task);
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");

        let mut new_db_task = test_task_with_payload(1, 2);
        new_db_task.database = "new_db".to_string();
        scheduler.enqueue(new_db_task);

        let mut new_db_task = test_task_with_payload(1, 3);
        new_db_task.database = "new_db".to_string();
        scheduler.enqueue(new_db_task);
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "same table id should not run concurrently after database rename"
        );

        release_first.notify_one();
        wait_until(|| completed.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            runs.lock().clone(),
            vec![
                ("old_db".to_string(), vec![1]),
                ("new_db".to_string(), vec![2, 3])
            ],
            "same table id with a renamed database should share one mailbox"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_same_table_payloads_are_merged_while_waiting_for_concurrency() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let second_started = Arc::new(Notify::new());
        let runs = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let second_started = second_started.clone();
            let runs = runs.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let second_started = second_started.clone();
                let runs = runs.clone();

                Box::pin(async move {
                    let table_id = task.table_id;
                    runs.lock().push(task.payload);
                    match table_id {
                        1 => {
                            first_started.notify_one();
                            release_first.notified().await;
                        }
                        2 => {
                            second_started.notify_one();
                        }
                        _ => {}
                    }
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task_with_payload(1, 1));
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");

        scheduler.enqueue(test_task_with_payload(2, 2));
        scheduler.enqueue(test_task_with_payload(2, 3));
        scheduler.enqueue(test_task_with_payload(2, 4));
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            runs.lock().clone(),
            vec![vec![1]],
            "second table should wait for the global concurrency permit"
        );

        release_first.notify_one();
        timeout(Duration::from_secs(1), second_started.notified())
            .await
            .expect("second table should start after first task completes");
        wait_until(|| runs.lock().len() == 2).await;
        assert_eq!(
            runs.lock().clone(),
            vec![vec![1], vec![2, 3, 4]],
            "same-table payloads queued behind the concurrency limit should be merged into one job"
        );
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
        wait_until(|| started.load(Ordering::SeqCst) == 1).await;

        scheduler.enqueue(test_task(1));
        wait_until(|| started.load(Ordering::SeqCst) == 2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_same_table_queue_is_coalesced_beyond_concurrency_limit() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let runs = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let completed = Arc::new(AtomicUsize::new(0));

        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let runs = runs.clone();
            let completed = completed.clone();

            Arc::new(move |task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let runs = runs.clone();
                let completed = completed.clone();

                Box::pin(async move {
                    runs.lock().push(task.payload);
                    let completed_index = completed.fetch_add(1, Ordering::SeqCst) + 1;
                    if completed_index == 1 {
                        first_started.notify_one();
                        release_first.notified().await;
                    }
                })
            })
        };

        let scheduler = SchedulerCore::create(1, runner, tokio_spawner());
        scheduler.enqueue(test_task_with_payload(1, 1));
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first task should start");

        for payload in 2..=32 {
            scheduler.enqueue(test_task_with_payload(1, payload));
        }

        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "same-table payloads should not create more running jobs while the first job is active"
        );

        release_first.notify_one();
        wait_until(|| completed.load(Ordering::SeqCst) == 2).await;

        let runs = runs.lock().clone();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0], vec![1]);
        assert_eq!(
            runs[1],
            (2..=32).collect::<Vec<_>>(),
            "same-table queued payloads should be coalesced into a single follow-up job"
        );
    }

    fn table_hook_task(ctx: Arc<QueryContext>, table: &str) -> TableHookTask {
        let hook_settings = TableHookTaskSettings::create(&ctx);
        TableHookTask {
            ctx,
            table_id: 1,
            compact_target: CompactTargetTableDescription {
                catalog: "default".to_string(),
                database: "default".to_string(),
                table: table.to_string(),
                table_id: Some(1),
            },
            hook_settings,
            lock_opt: LockTableOption::NoLock,
            operation_name: "insert".to_string(),
            main_operation_start: Instant::now(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_accumulates_compaction_hint() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx.set_compaction_num_block_hint("t", 7);
        latest_ctx.set_compaction_num_block_hint("t", 5);
        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert_eq!(task.ctx.get_compaction_num_block_hint("t"), 12);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_preserves_hook_settings() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "1".to_string())?;
        old_ctx.get_settings().set_setting(
            "enable_refresh_aggregating_index_after_write".to_string(),
            "1".to_string(),
        )?;
        latest_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "0".to_string())?;
        latest_ctx.get_settings().set_setting(
            "enable_refresh_aggregating_index_after_write".to_string(),
            "0".to_string(),
        )?;

        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert!(task.hook_settings.compact_after_write);
        assert!(task.hook_settings.refresh_aggregating_index_after_write);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resolve_current_table_name_by_id_after_rename() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        fixture
            .execute_command("DROP DATABASE IF EXISTS hook_resolve_rename")
            .await?;
        fixture
            .execute_command("CREATE DATABASE hook_resolve_rename")
            .await?;
        fixture
            .execute_command("CREATE TABLE hook_resolve_rename.t(a INT)")
            .await?;

        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "hook_resolve_rename", "t").await?;
        let table_id = table.get_id();

        fixture
            .execute_command("ALTER TABLE hook_resolve_rename.t RENAME TO t_renamed")
            .await?;

        let resolved = resolve_current_table_name_by_id(
            &ctx,
            "test",
            "default",
            "hook_resolve_rename",
            "t",
            Some(table_id),
        )
        .await?;

        assert_eq!(
            resolved,
            Some(("hook_resolve_rename".to_string(), "t_renamed".to_string()))
        );

        fixture
            .execute_command("DROP DATABASE IF EXISTS hook_resolve_rename")
            .await?;
        Ok(())
    }
}
