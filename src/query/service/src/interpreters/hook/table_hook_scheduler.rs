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
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::interpreters::hook::table_hook_task::SchedulerTask;
use crate::interpreters::hook::table_hook_task::TableHookRequest;
use crate::interpreters::hook::table_hook_task::TableHookTaskKey;

type SchedulerFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type SchedulerRunner<T> =
    Arc<dyn Fn(T) -> Pin<Box<dyn Future<Output = Option<T>> + Send>> + Send + Sync>;
type SchedulerSpawner = Arc<dyn Fn(SchedulerFuture, String) + Send + Sync>;

pub struct TableHookScheduler {
    core: Arc<SchedulerCore<TableHookRequest>>,
}

struct SchedulerCore<T> {
    queue: Mutex<MailboxQueue<T>>,
    runner: SchedulerRunner<T>,
    spawner: SchedulerSpawner,
    semaphore: Arc<Semaphore>,
    wake: Notify,
    capacity: Notify,
    max_tables: usize,
}

struct MailboxQueue<T> {
    states: HashMap<TableHookTaskKey, TableHookState<T>>,
    ready: VecDeque<TableHookTaskKey>,
}

enum TableHookState<T> {
    Queued(T),
    Running { pending: Option<T> },
}

impl TableHookScheduler {
    pub fn init(max_concurrency: usize) -> Result<()> {
        let runner: SchedulerRunner<TableHookRequest> =
            Arc::new(|request| Box::pin(request.execute()));
        let spawner: SchedulerSpawner = Arc::new(|future, name| {
            GlobalIORuntime::instance().spawn_named(future, name);
        });
        let core = SchedulerCore::create(
            max_concurrency,
            GlobalConfig::instance()
                .query
                .common
                .table_maintenance_max_pending_tables,
            runner,
            spawner,
        );
        GlobalInstance::set(Arc::new(Self { core }));
        Ok(())
    }

    pub fn try_instance() -> Option<Arc<Self>> {
        GlobalInstance::try_get()
    }

    /// Apply backpressure only to a new table key. Values waiting here own no caller context
    /// or table lock. Never silently discard the only notification when the mailbox is full.
    pub(super) async fn enqueue(&self, mut request: TableHookRequest) {
        loop {
            let capacity = self.core.capacity.notified();
            tokio::pin!(capacity);
            capacity.as_mut().enable();
            match self.core.try_enqueue(request) {
                Ok(()) => return,
                Err(pending) => request = pending,
            }
            warn!(table_id = request.target.table_id; "Table maintenance mailbox full; waiting for capacity");
            capacity.await;
        }
    }
}

impl<T: SchedulerTask> SchedulerCore<T> {
    fn create(
        max_concurrency: usize,
        max_tables: usize,
        runner: SchedulerRunner<T>,
        spawner: SchedulerSpawner,
    ) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            queue: Mutex::new(MailboxQueue {
                states: HashMap::new(),
                ready: VecDeque::new(),
            }),
            runner,
            spawner: spawner.clone(),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            wake: Notify::new(),
            capacity: Notify::new(),
            max_tables,
        });
        spawner(
            Box::pin(Self::dispatch(scheduler.clone())),
            "table-hook-scheduler".to_string(),
        );
        scheduler
    }

    fn try_enqueue(&self, task: T) -> std::result::Result<(), T> {
        let key = task.key();
        let mut queue = self.queue.lock();
        match queue.states.get_mut(&key) {
            Some(TableHookState::Queued(pending)) => pending.merge(task),
            Some(TableHookState::Running { pending }) => match pending {
                Some(pending) => pending.merge(task),
                None => *pending = Some(task),
            },
            None => {
                if queue.states.len() >= self.max_tables {
                    return Err(task);
                }
                queue
                    .states
                    .insert(key.clone(), TableHookState::Queued(task));
                queue.ready.push_back(key);
            }
        }
        drop(queue);
        self.wake.notify_one();
        Ok(())
    }

    async fn dispatch(scheduler: Arc<Self>) {
        loop {
            let notified = scheduler.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Ok(permit) = scheduler.semaphore.clone().try_acquire_owned() {
                let (task, deadline) = scheduler.take_ready_task();
                if let Some((key, task)) = task {
                    (scheduler.spawner)(
                        Box::pin(Self::run_table_job(scheduler.clone(), key, task, permit)),
                        "table-hook-job".to_string(),
                    );
                    continue;
                }
                drop(permit);
                if let Some(deadline) = deadline {
                    tokio::select! {
                        _ = &mut notified => {},
                        _ = tokio::time::sleep_until(deadline.into()) => {},
                    }
                    continue;
                }
            }
            notified.await;
        }
    }

    fn take_ready_task(&self) -> (Option<(TableHookTaskKey, T)>, Option<Instant>) {
        let mut queue = self.queue.lock();
        let now = Instant::now();
        let mut earliest: Option<Instant> = None;
        for _ in 0..queue.ready.len() {
            let key = queue.ready.pop_front().expect("ready queue length checked");
            let Some(TableHookState::Queued(task)) = queue.states.get(&key) else {
                continue;
            };
            let due = task.ready_at();
            if due > now {
                earliest = Some(earliest.map_or(due, |previous| previous.min(due)));
                queue.ready.push_back(key);
                continue;
            }
            let Some(TableHookState::Queued(task)) = queue
                .states
                .insert(key.clone(), TableHookState::Running { pending: None })
            else {
                unreachable!()
            };
            return (Some((key, task)), earliest);
        }
        (None, earliest)
    }

    async fn run_table_job(
        scheduler: Arc<Self>,
        key: TableHookTaskKey,
        task: T,
        permit: OwnedSemaphorePermit,
    ) {
        let result = CatchUnwindFuture::create(async { (scheduler.runner)(task).await }).await;
        let continuation = match result {
            Ok(continuation) => continuation,
            Err(e) => {
                warn!(table_id = key.table_id, catalog = key.catalog.as_str(); "Async table hook panicked: {e}");
                None
            }
        };
        // A round/batch ends here. Neither retry delays nor the rest of a long logical job
        // holds a concurrency permit, a query context, a memory reservation or table claims.
        scheduler.finish_table_job(key, continuation);
        drop(permit);
        scheduler.wake.notify_one();
    }

    fn finish_table_job(&self, key: TableHookTaskKey, mut continuation: Option<T>) {
        let mut queue = self.queue.lock();
        let Some(TableHookState::Running { pending }) = queue.states.remove(&key) else {
            return;
        };
        if let Some(pending) = pending {
            match &mut continuation {
                Some(task) => task.merge(pending),
                None => continuation = Some(pending),
            }
        }
        if let Some(task) = continuation {
            queue
                .states
                .insert(key.clone(), TableHookState::Queued(task));
            queue.ready.push_back(key);
        } else {
            self.capacity.notify_waiters();
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
        not_before: Instant,
    }

    impl SchedulerTask for TestTask {
        fn ready_at(&self) -> Instant {
            self.not_before
        }

        fn key(&self) -> TableHookTaskKey {
            TableHookTaskKey {
                tenant: "test".to_string(),
                catalog: self.catalog.clone(),
                table_id: self.table_id,
            }
        }

        fn merge(&mut self, other: Self) {
            self.payload.extend(other.payload);
        }
    }

    type TestRunner = Arc<dyn Fn(TestTask) -> SchedulerFuture + Send + Sync>;

    fn create_test_scheduler(
        concurrency: usize,
        runner: TestRunner,
    ) -> Arc<SchedulerCore<TestTask>> {
        SchedulerCore::create(
            concurrency,
            32,
            Arc::new(move |task| {
                let future = runner(task);
                Box::pin(async move {
                    future.await;
                    None
                })
            }),
            tokio_spawner(),
        )
    }

    impl SchedulerCore<TestTask> {
        fn enqueue(&self, task: TestTask) {
            assert!(self.try_enqueue(task).is_ok());
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
            not_before: Instant::now(),
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

        let runner: TestRunner = {
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

        let scheduler = create_test_scheduler(1, runner);
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

        let runner: TestRunner = {
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

        let scheduler = create_test_scheduler(1, runner);
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

        let runner: TestRunner = {
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

        let scheduler = create_test_scheduler(2, runner);
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

        let runner: TestRunner = {
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

        let scheduler = create_test_scheduler(1, runner);
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

        let runner: TestRunner = {
            let started = started.clone();

            Arc::new(move |_task| {
                let started = started.clone();
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                })
            })
        };

        let scheduler = create_test_scheduler(1, runner);
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

        let runner: TestRunner = {
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

        let scheduler = create_test_scheduler(1, runner);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delayed_continuation_yields_and_mailboxes_are_bounded() {
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let runner: SchedulerRunner<TestTask> = {
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            let runs = runs.clone();
            Arc::new(move |mut task| {
                let first_started = first_started.clone();
                let release_first = release_first.clone();
                let runs = runs.clone();
                Box::pin(async move {
                    runs.lock().push(task.table_id);
                    if task.table_id == 1 && task.payload == vec![1] {
                        first_started.notify_one();
                        release_first.notified().await;
                        task.payload.push(99);
                        task.not_before = Instant::now() + Duration::from_millis(150);
                        Some(task)
                    } else {
                        None
                    }
                })
            })
        };
        let scheduler = SchedulerCore::create(1, 2, runner, tokio_spawner());
        assert!(scheduler.try_enqueue(test_task(1)).is_ok());
        timeout(Duration::from_secs(2), first_started.notified())
            .await
            .unwrap();
        assert!(scheduler.try_enqueue(test_task(2)).is_ok());
        assert!(
            scheduler.try_enqueue(test_task(3)).is_err(),
            "new keys must obey mailbox capacity"
        );
        release_first.notify_one();
        wait_until(|| runs.lock().len() >= 2).await;
        assert_eq!(
            &runs.lock()[..2],
            &[1, 2],
            "a deferred continuation must release the only execution slot"
        );
        wait_until(|| runs.lock().len() == 3).await;
        assert_eq!(*runs.lock(), vec![1, 2, 1]);
        wait_until(|| scheduler.queue.lock().states.is_empty()).await;
        assert!(scheduler.try_enqueue(test_task(3)).is_ok());
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
