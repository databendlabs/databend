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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::catch_unwind;
use databend_common_metrics::table_maintenance::set_mailbox;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const HIGH_PRIORITY_BURST: u8 = 4;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TableKey {
    pub tenant: String,
    pub catalog: String,
    pub table_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TaskKind {
    Refresh,
    Analyze,
    Recluster,
}

impl TaskKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Refresh => "refresh",
            Self::Analyze => "analyze",
            Self::Recluster => "recluster",
        }
    }

    fn is_high(self) -> bool {
        !matches!(self, Self::Recluster)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TaskKey {
    pub table: TableKey,
    pub kind: TaskKind,
}

pub(crate) trait SchedulerTask: Send + 'static {
    type ExecutionKey: Clone + Eq + Send + Sync;

    fn key(&self) -> TaskKey;
    fn execution_key(&self) -> Self::ExecutionKey;
    // Called under a mailbox-local lock, never the scheduler lock. No IO or await.
    fn merge(&mut self, other: Self);
    fn ready_at(&self) -> Instant;
}

pub(crate) type SchedulerFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub(crate) type SchedulerRunner<T> = Arc<
    dyn Fn(T, CancellationToken, usize) -> Pin<Box<dyn Future<Output = RunResult<T>> + Send>>
        + Send
        + Sync,
>;
pub(crate) type SchedulerSpawner = Arc<dyn Fn(SchedulerFuture, String) + Send + Sync>;

pub(crate) enum RunResult<T> {
    Completed,
    Continue(T),
    Stopped,
}

pub(crate) struct Rejected<T> {
    pub task: T,
    pub reason: &'static str,
}

pub(crate) struct SchedulerCore<T: SchedulerTask> {
    queue: Mutex<MailboxQueue<T>>,
    runner: SchedulerRunner<T>,
    spawner: SchedulerSpawner,
    semaphore: Arc<Semaphore>,
    max_concurrency: usize,
    wake: Notify,
    max_tables: usize,
    stopped: Notify,
    cancellation: CancellationToken,
}

struct MailboxQueue<T: SchedulerTask> {
    states: HashMap<Arc<TaskKey>, Entry<T>>,
    tables: HashMap<TableKey, TableState>,
    // Capacity covers every registered key, including running/finalizing keys.
    // Returning a unit to this queue never allocates.
    ready: VecDeque<Arc<TaskKey>>,
    running: usize,
    high_streak: u8,
    closed: bool,
    dispatch_stopped: bool,
}

struct Entry<T: SchedulerTask> {
    mailbox: Arc<Mailbox<T>>,
    ready_at: Instant,
    running: bool,
}

type ReadyTask<T> = Option<(Arc<TaskKey>, Arc<Mailbox<T>>)>;

struct Mailbox<T: SchedulerTask> {
    execution_key: T::ExecutionKey,
    pending: Mutex<Pending<T>>,
}

struct Pending<T> {
    task: Option<T>,
    retired: bool,
}

#[derive(Default)]
struct TableState {
    keys: usize,
    busy: bool,
    last_was_low: bool,
    // Recomputed during selection, without allocating under the scheduler lock.
    high_due: bool,
}

// Owns every dispatch allocation until both execution and finalization have finished.
// In particular, a panic in a task merge must not strand the table gate or counters.
struct RunningGuard<T: SchedulerTask> {
    scheduler: Arc<SchedulerCore<T>>,
    key: Arc<TaskKey>,
    mailbox: Arc<Mailbox<T>>,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
}

impl<T: SchedulerTask> RunningGuard<T> {
    fn finish(&mut self, deadline: Option<Instant>) {
        self.scheduler
            .finish_job(&self.key, deadline, &mut self.permit, &mut self.finished);
    }
}

// Keep the registration and execution qualifications until payload destruction ends,
// even if destruction unwinds. No payload is destroyed under the global queue lock.
struct FinalizeGuard<'a, T: SchedulerTask>(&'a mut RunningGuard<T>);

impl<T: SchedulerTask> Drop for FinalizeGuard<'_, T> {
    fn drop(&mut self) {
        self.0.finish(None);
    }
}

impl<T: SchedulerTask> Drop for RunningGuard<T> {
    fn drop(&mut self) {
        if !self.finished {
            let finalizer = FinalizeGuard(self);
            let discarded = {
                let mut pending = finalizer.0.mailbox.pending.lock();
                pending.retired = true;
                pending.task.take()
            };
            // Guard Drop can itself run during unwinding (e.g. a failed spawn).
            // Catch a payload destructor panic before it becomes a double panic.
            let result = catch_unwind(|| drop(discarded));
            drop(finalizer);
            if let Err(error) = result {
                log::warn!(error :? = error; "Maintenance payload cleanup panicked");
            }
        }
    }
}

struct DispatchGuard<T: SchedulerTask>(Arc<SchedulerCore<T>>);

impl<T: SchedulerTask> Drop for DispatchGuard<T> {
    fn drop(&mut self) {
        {
            let mut queue = self.0.queue.lock();
            queue.closed = true;
            queue.dispatch_stopped = true;
            self.0.semaphore.close();
        }
        self.0.cancellation.cancel();
        self.0.stopped.notify_waiters();
        self.0.close();
    }
}

impl<T: SchedulerTask> SchedulerCore<T> {
    pub fn create(
        max_concurrency: usize,
        max_tables: usize,
        runner: SchedulerRunner<T>,
        spawner: SchedulerSpawner,
    ) -> Arc<Self> {
        set_mailbox(0, 0);
        let scheduler = Arc::new(Self {
            queue: Mutex::new(MailboxQueue {
                states: HashMap::new(),
                tables: HashMap::new(),
                ready: VecDeque::new(),
                running: 0,
                high_streak: 0,
                closed: false,
                dispatch_stopped: false,
            }),
            runner,
            spawner: spawner.clone(),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            max_concurrency,
            wake: Notify::new(),
            max_tables,
            stopped: Notify::new(),
            cancellation: CancellationToken::new(),
        });
        // The guard also covers an unpolled/dropped dispatch future.
        let dispatch_guard = DispatchGuard(scheduler.clone());
        spawner(
            Box::pin(Self::dispatch(dispatch_guard)),
            "table-maintenance-scheduler".to_string(),
        );
        scheduler
    }

    pub fn try_enqueue(&self, task: T) -> std::result::Result<(), Rejected<T>> {
        // Prepare owned values before publishing any registration. In particular,
        // allocation failure cannot destroy a business payload under the queue lock.
        let key = Arc::new(task.key());
        let table_key = key.table.clone();
        let execution_key = task.execution_key();
        let ready_at = task.ready_at();
        let mut candidate = Arc::new(Mailbox {
            execution_key,
            pending: Mutex::new(Pending {
                task: Some(task),
                retired: false,
            }),
        });
        let result = self.enqueue_mailbox(key, table_key, ready_at, &mut candidate);
        result.map_err(|reason| Rejected {
            task: Arc::get_mut(&mut candidate)
                .expect("rejected mailbox was not published")
                .pending
                .get_mut()
                .task
                .take()
                .expect("rejected notification"),
            reason,
        })
    }

    fn enqueue_mailbox(
        &self,
        key: Arc<TaskKey>,
        table_key: TableKey,
        ready_at: Instant,
        candidate: &mut Arc<Mailbox<T>>,
    ) -> std::result::Result<(), &'static str> {
        let mailbox = {
            let mut queue = self.queue.lock();
            if queue.closed {
                return Err("shutdown");
            }
            if let Some(entry) = queue.states.get(&key) {
                if entry.mailbox.execution_key != candidate.execution_key {
                    return Err("incompatible_execution");
                }
                entry.mailbox.clone()
            } else {
                let new_table = !queue.tables.contains_key(&key.table);
                if new_table && queue.tables.len() >= self.max_tables {
                    return Err("capacity");
                }
                let ready_slots = queue.states.len() + 1 - queue.ready.len();
                if (new_table && queue.tables.try_reserve(1).is_err())
                    || queue.states.try_reserve(1).is_err()
                    || queue.ready.try_reserve(ready_slots).is_err()
                {
                    return Err("metadata_capacity");
                }
                // All fallible preparation is complete. Arc clones and inserts
                // into the reserved containers cannot allocate.
                queue.tables.entry(table_key).or_default().keys += 1;
                queue.states.insert(key.clone(), Entry {
                    mailbox: candidate.clone(),
                    ready_at,
                    running: false,
                });
                queue.ready.push_back(key);
                drop(queue);
                self.wake.notify_one();
                self.record_metrics();
                return Ok(());
            }
        };
        // Finalization can include slow destruction. Never spin or block a
        // committed write waiting for a retired mailbox to be removed.
        let mut pending = mailbox.pending.lock();
        if pending.retired {
            return Err("finalizing");
        }
        let task = Arc::get_mut(candidate)
            .expect("unpublished mailbox")
            .pending
            .get_mut()
            .task
            .take()
            .expect("unmerged notification");
        match pending.task.as_mut() {
            Some(previous) => previous.merge(task),
            None => pending.task = Some(task),
        }
        drop(pending);
        self.wake.notify_one();
        Ok(())
    }

    async fn dispatch(guard: DispatchGuard<T>) {
        if let Err(error) = CatchUnwindFuture::create(Self::dispatch_loop(&guard.0)).await {
            log::error!(error :? = error; "Maintenance dispatcher failed; closing admission");
        }
    }

    async fn dispatch_loop(scheduler: &Arc<Self>) {
        loop {
            let notified = scheduler.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if scheduler.semaphore.is_closed() {
                return;
            }
            if let Ok(permit) = scheduler.semaphore.clone().try_acquire_owned() {
                let (task, deadline) = scheduler.take_ready_task();
                if let Some((key, mailbox)) = task {
                    // Construct before spawning, so dropping an unpolled future also cleans up.
                    let guard = RunningGuard {
                        scheduler: scheduler.clone(),
                        key,
                        mailbox,
                        permit: Some(permit),
                        finished: false,
                    };
                    scheduler.record_metrics();
                    (scheduler.spawner)(
                        Box::pin(Self::run_job(guard)),
                        "table-maintenance-unit".to_string(),
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

    pub fn close(self: &Arc<Self>) {
        {
            let mut queue = self.queue.lock();
            queue.closed = true;
            self.semaphore.close();
        }
        self.cancellation.cancel();
        self.wake.notify_one();
        loop {
            let finalizer = {
                let mut queue = self.queue.lock();
                queue.ready.pop_front().map(|key| RunningGuard {
                    mailbox: queue.states[&key].mailbox.clone(),
                    scheduler: self.clone(),
                    key,
                    permit: None,
                    finished: false,
                })
            };
            let Some(finalizer) = finalizer else {
                break;
            };
            // Keep queued registrations visible until destruction ends, without
            // blocking the caller's shutdown grace on a synchronous payload Drop.
            // An unpolled/rejected cleanup future still owns its finalizer guard.
            if let Err(error) = catch_unwind(|| {
                (self.spawner)(
                    Box::pin(async move { drop(finalizer) }),
                    "table-maintenance-finalizer".to_string(),
                );
            }) {
                log::warn!(error :? = error; "Maintenance finalizer spawn failed");
            }
        }
        self.record_metrics();
    }

    pub async fn shutdown(self: &Arc<Self>) {
        self.close();
        loop {
            let stopped = self.stopped.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            {
                let queue = self.queue.lock();
                if queue.dispatch_stopped && queue.states.is_empty() {
                    return;
                }
            }
            stopped.await;
        }
    }

    fn take_ready_task(&self) -> (ReadyTask<T>, Option<Instant>) {
        let mut queue = self.queue.lock();
        if queue.closed {
            return (None, None);
        }
        let now = Instant::now();
        let mut high = None;
        let mut earliest: Option<Instant> = None;
        let MailboxQueue {
            states,
            tables,
            ready,
            ..
        } = &mut *queue;
        for table in tables.values_mut() {
            table.high_due = false;
        }
        for (index, key) in ready.iter().enumerate() {
            let entry = &states[key];
            if entry.ready_at > now {
                earliest = Some(earliest.map_or(entry.ready_at, |old| old.min(entry.ready_at)));
                continue;
            }
            let table = tables.get_mut(&key.table).expect("registered table");
            if !table.busy && key.kind.is_high() {
                table.high_due = true;
                high.get_or_insert(index);
            }
        }
        let low = ready.iter().position(|key| {
            let table = &tables[&key.table];
            !key.kind.is_high()
                && states[key].ready_at <= now
                && !table.busy
                // Other tables can consume the High burst while this table runs Low.
                // Do not let that Low immediately reacquire its gate ahead of a due High.
                && !(table.last_was_low && table.high_due)
        });
        // Share dispatch opportunities, not CPU time or memory. An empty class never
        // wastes a slot; idle polling never advances the counter. No class reserves slots.
        let selected = if queue.high_streak < HIGH_PRIORITY_BURST {
            high.or(low)
        } else {
            low.or(high)
        };
        let Some(selected) = selected else {
            return (None, earliest);
        };
        let key = queue.ready.remove(selected).expect("selected ready entry");
        queue.high_streak = if key.kind.is_high() {
            queue.high_streak.saturating_add(1).min(HIGH_PRIORITY_BURST)
        } else {
            0
        };
        let table = queue.tables.get_mut(&key.table).expect("registered table");
        table.busy = true;
        table.last_was_low = !key.kind.is_high();
        queue.running += 1;
        let entry = queue.states.get_mut(&key).expect("registered task");
        entry.running = true;
        let mailbox = entry.mailbox.clone();
        drop(queue);
        (Some((key, mailbox)), earliest)
    }

    async fn run_job(mut guard: RunningGuard<T>) {
        let result = CatchUnwindFuture::create(async {
            let task = guard.mailbox.pending.lock().task.take();
            let result = match task {
                Some(task) if !guard.scheduler.cancellation.is_cancelled() => {
                    let demand = {
                        let queue = guard.scheduler.queue.lock();
                        let now = Instant::now();
                        // A table gate permits only one kind at a time. Multiple ready
                        // kinds on that table are not independent resource demand;
                        // take_ready_task still determines their priority and order.
                        // Admission clamps demand to max_concurrency; stop once
                        // further tables cannot change the resulting grant.
                        let needed = guard
                            .scheduler
                            .max_concurrency
                            .saturating_sub(queue.running);
                        let mut ready_tables = HashSet::new();
                        for key in &queue.ready {
                            if ready_tables.len() == needed {
                                break;
                            }
                            if !queue.tables[&key.table].busy && queue.states[key].ready_at <= now {
                                ready_tables.insert(&key.table);
                            }
                        }
                        queue.running + ready_tables.len()
                    };
                    (guard.scheduler.runner)(task, guard.scheduler.cancellation.clone(), demand)
                        .await
                }
                _ => RunResult::Stopped,
            };
            let stopped = matches!(result, RunResult::Stopped);
            let mut continuation = match result {
                RunResult::Continue(task) => Some(task),
                _ => None,
            };
            let mut pending = guard.mailbox.pending.lock();
            if !pending.retired && !stopped {
                if let Some(new) = pending.task.take() {
                    match &mut continuation {
                        Some(task) => task.merge(new),
                        None => continuation = Some(new),
                    }
                }
            } else {
                continuation = None;
            }
            let deadline = continuation.as_ref().map(SchedulerTask::ready_at);
            pending.retired = continuation.is_none();
            pending.task = continuation;
            drop(pending);
            guard.finish(deadline);
        })
        .await;
        if let Err(error) = result {
            log::warn!(table_id = guard.key.table.table_id; "Maintenance unit panicked: {error}");
        }
    }

    #[cfg(test)]
    fn finish_table_job(&self, key: &Arc<TaskKey>, deadline: Option<Instant>) {
        self.finish_job(key, deadline, &mut None, &mut false);
    }

    fn finish_job(
        &self,
        key: &Arc<TaskKey>,
        deadline: Option<Instant>,
        permit: &mut Option<OwnedSemaphorePermit>,
        finished: &mut bool,
    ) {
        let removed = {
            let mut queue = self.queue.lock();
            if *finished || (deadline.is_some() && queue.closed) {
                // On close the caller must discard its continuation before releasing
                // the registration. RunningGuard performs that finalization on Drop.
                return;
            }
            if queue.states.get(key).expect("registered task").running {
                let table = queue.tables.get_mut(&key.table).expect("running table");
                table.busy = false;
                queue.running -= 1;
            }
            let removed = if let Some(deadline) = deadline {
                let entry = queue.states.get_mut(key).expect("running task");
                entry.ready_at = deadline;
                entry.running = false;
                queue.ready.push_back(key.clone());
                None
            } else {
                queue.remove(key)
            };
            // No allocations, callbacks or metrics occur in this transition. Publish
            // guard ownership together with counters/gate/permit, not afterwards.
            *finished = true;
            drop(permit.take());
            removed
        };
        self.wake.notify_one();
        self.stopped.notify_waiters();
        drop(removed);
        self.record_metrics();
    }

    fn record_metrics(&self) {
        // These gauges were initialized at create(). Serialize their atomic stores
        // with state observers, but keep them outside every ownership transition.
        let queue = self.queue.lock();
        set_mailbox(queue.tables.len(), queue.running);
    }
}

impl<T: SchedulerTask> MailboxQueue<T> {
    fn remove(&mut self, key: &TaskKey) -> Option<Entry<T>> {
        let entry = self.states.remove(key)?;
        let table = self.tables.get_mut(&key.table).expect("registered table");
        table.keys -= 1;
        if table.keys == 0 {
            self.tables.remove(&key.table);
        }
        Some(entry)
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
        database: String,
        table_id: u64,
        payload: Vec<u64>,
        not_before: Instant,
        kind: TaskKind,
        identity: u64,
        _finalizer: Option<Arc<TestFinalizer>>,
    }

    #[derive(Default)]
    struct DropBarrier {
        started: Notify,
        released: std::sync::Mutex<bool>,
        changed: std::sync::Condvar,
    }

    struct TestFinalizer(Arc<DropBarrier>);

    impl Drop for TestFinalizer {
        fn drop(&mut self) {
            self.0.started.notify_one();
            // Bound even a failing assertion's teardown; do not strand a test worker.
            drop(self.0.changed.wait_timeout_while(
                self.0.released.lock().unwrap(),
                Duration::from_secs(30),
                |released| !*released,
            ));
        }
    }

    impl SchedulerTask for TestTask {
        type ExecutionKey = u64;

        fn execution_key(&self) -> u64 {
            self.identity
        }

        fn ready_at(&self) -> Instant {
            self.not_before
        }

        fn key(&self) -> TaskKey {
            TaskKey {
                table: TableKey {
                    tenant: "test".to_string(),
                    catalog: self.catalog.clone(),
                    table_id: self.table_id,
                },
                kind: self.kind,
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
            Arc::new(move |task, _cancellation, _demand| {
                let future = runner(task);
                Box::pin(async move {
                    future.await;
                    RunResult::Completed
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
            kind: TaskKind::Refresh,
            identity: 1,
            _finalizer: None,
        }
    }

    fn tokio_spawner() -> SchedulerSpawner {
        Arc::new(|future, _name| {
            databend_common_base::runtime::spawn(future);
        })
    }

    fn idle_spawner() -> SchedulerSpawner {
        Arc::new(|future, _| {
            databend_common_base::runtime::spawn(async move {
                std::future::pending::<()>().await;
                future.await;
            });
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
        wait_until(|| scheduler.queue.lock().states.is_empty()).await;
        assert_eq!(started.load(Ordering::SeqCst), 1);

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
            Arc::new(move |mut task, _cancellation, _demand| {
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
                        RunResult::Continue(task)
                    } else {
                        RunResult::Completed
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
        timeout(Duration::from_secs(1), scheduler.shutdown())
            .await
            .expect("shutdown must quiesce the dispatcher and any running unit");
        assert!(scheduler.queue.lock().states.is_empty());
        assert!(scheduler.try_enqueue(test_task(4)).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shutdown_cancels_running_units_and_discards_followups() {
        let started = Arc::new(Notify::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let runner: SchedulerRunner<TestTask> = {
            let started = started.clone();
            let runs = runs.clone();
            Arc::new(move |task, cancellation, _demand| {
                let started = started.clone();
                let runs = runs.clone();
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    cancellation.cancelled().await;
                    RunResult::Continue(task)
                })
            })
        };
        let scheduler = SchedulerCore::create(1, 2, runner, tokio_spawner());
        scheduler.enqueue(test_task(1));
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        scheduler.enqueue(test_task(1));
        scheduler.enqueue(test_task(2));
        scheduler.close();
        assert!(scheduler.try_enqueue(test_task(3)).is_err());
        timeout(Duration::from_secs(1), scheduler.shutdown())
            .await
            .unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(scheduler.queue.lock().states.is_empty());
        assert_eq!(scheduler.semaphore.available_permits(), 1);
        timeout(Duration::from_secs(1), scheduler.shutdown())
            .await
            .unwrap();

        let panic_started = Arc::new(Notify::new());
        let panic_now = Arc::new(Notify::new());
        let runner: SchedulerRunner<TestTask> = {
            let panic_started = panic_started.clone();
            let panic_now = panic_now.clone();
            Arc::new(move |_, _, _| {
                let panic_started = panic_started.clone();
                let panic_now = panic_now.clone();
                Box::pin(async move {
                    panic_started.notify_one();
                    panic_now.notified().await;
                    panic!("injected fake runner panic");
                })
            })
        };
        let panicking = SchedulerCore::create(1, 1, runner, tokio_spawner());
        panicking.enqueue(test_task(1));
        timeout(Duration::from_secs(10), panic_started.notified())
            .await
            .unwrap();
        let barrier = Arc::new(DropBarrier::default());
        let mut pending = test_task(1);
        pending._finalizer = Some(Arc::new(TestFinalizer(barrier.clone())));
        panicking.enqueue(pending);
        panic_now.notify_one();
        // A process-wide panic hook may take time collecting a backtrace.
        timeout(Duration::from_secs(30), barrier.started.notified())
            .await
            .unwrap();
        assert_eq!(panicking.queue.lock().running, 1);
        assert_eq!(panicking.queue.lock().tables.len(), 1);
        assert!(panicking.try_enqueue(test_task(2)).is_err());
        assert_eq!(
            panicking.try_enqueue(test_task(1)).err().unwrap().reason,
            "finalizing"
        );
        assert!(
            timeout(Duration::from_millis(20), panicking.shutdown())
                .await
                .is_err(),
            "shutdown must wait for payload finalization"
        );
        *barrier.released.lock().unwrap() = true;
        barrier.changed.notify_all();
        timeout(Duration::from_secs(30), panicking.shutdown())
            .await
            .unwrap();
        assert_eq!(panicking.semaphore.available_permits(), 1);
        assert_eq!(panicking.queue.lock().running, 0);
        assert!(panicking.queue.lock().tables.is_empty());

        // A spawn failure is outside the unit's catch boundary. The dispatcher
        // must fail closed and acknowledge shutdown, even for an unpolled unit.
        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        let failed = SchedulerCore::create(
            1,
            1,
            runner,
            Arc::new(|future, name| {
                if name == "table-maintenance-unit" {
                    panic!("injected maintenance spawn failure");
                }
                databend_common_base::runtime::spawn(future);
            }),
        );
        failed.enqueue(test_task(1));
        timeout(Duration::from_secs(30), async {
            loop {
                let stopped = failed.stopped.notified();
                tokio::pin!(stopped);
                stopped.as_mut().enable();
                if failed.queue.lock().dispatch_stopped {
                    break;
                }
                stopped.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            failed.try_enqueue(test_task(2)).err().unwrap().reason,
            "shutdown"
        );
        timeout(Duration::from_secs(1), failed.shutdown())
            .await
            .unwrap();
        assert_eq!(failed.semaphore.available_permits(), 1);
        assert_eq!(failed.queue.lock().running, 0);

        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        let dropped = SchedulerCore::create(1, 1, runner, Arc::new(|_, _| {}));
        assert_eq!(
            dropped.try_enqueue(test_task(1)).err().unwrap().reason,
            "shutdown"
        );
        timeout(Duration::from_secs(1), dropped.shutdown())
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_mailbox_still_merges_existing_keys() {
        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        // Do not dispatch: admission alone must be synchronous even with no capacity release.
        let scheduler = SchedulerCore::create(1, 1, runner, idle_spawner());
        assert!(scheduler.try_enqueue(test_task_with_payload(1, 1)).is_ok());
        assert!(scheduler.try_enqueue(test_task(2)).is_err());
        assert!(scheduler.try_enqueue(test_task_with_payload(1, 3)).is_ok());
        {
            let queue = scheduler.queue.lock();
            assert_eq!(queue.states.len(), 1);
            let entry = queue.states.get(&test_task(1).key()).unwrap();
            assert_eq!(
                entry.mailbox.pending.lock().task.as_ref().unwrap().payload,
                vec![1, 3]
            );
        }
        let mut other_identity = test_task(1);
        other_identity.identity = 2;
        let rejected = scheduler.try_enqueue(other_identity).err().unwrap();
        assert_eq!(rejected.reason, "incompatible_execution");
        assert_eq!(rejected.task.identity, 2);
        for kind in [TaskKind::Analyze, TaskKind::Recluster] {
            let mut task = test_task(1);
            task.kind = kind;
            assert!(scheduler.try_enqueue(task).is_ok());
        }
        assert_eq!(scheduler.queue.lock().states.len(), 3);
        {
            let queue = scheduler.queue.lock();
            assert_eq!(queue.tables.len(), 1);
            assert!(queue.ready.capacity() >= queue.states.len());
        }
        assert!(scheduler.try_enqueue(test_task(2)).is_err());

        // Drive dispatch selection directly: no wall-clock ordering or worker races.
        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        let policy = SchedulerCore::create(2, 32, runner, idle_spawner());
        for id in 1..=11 {
            let mut task = test_task(id);
            if id == 1 || id == 11 {
                task.kind = TaskKind::Recluster;
            }
            policy.enqueue(task);
        }
        for expected_high in [
            true, true, true, true, false, true, true, true, true, false, true,
        ] {
            let (Some((key, _)), _) = policy.take_ready_task() else {
                panic!("eligible task");
            };
            assert_eq!(key.kind.is_high(), expected_high);
            let mut finished = false;
            let mut permit = None;
            policy.finish_job(&key, None, &mut permit, &mut finished);
            assert!(finished);
            // A repeated cleanup cannot decrement counters or remove a new owner.
            policy.finish_job(&key, None, &mut permit, &mut finished);
            assert_eq!(policy.queue.lock().running, 0);
        }
        assert!(policy.queue.lock().tables.is_empty());
        let mut low = test_task(11);
        low.kind = TaskKind::Recluster;
        policy.enqueue(low);
        let (Some((key, mailbox)), _) = policy.take_ready_task() else {
            panic!("low candidate");
        };
        assert_eq!(key.kind, TaskKind::Recluster);
        assert_eq!(policy.queue.lock().running, 1);
        let mut another_low = test_task(12);
        another_low.kind = TaskKind::Recluster;
        policy.enqueue(another_low);
        let (Some((borrowed, _)), _) = policy.take_ready_task() else {
            panic!("Recluster must be able to borrow an otherwise idle slot");
        };
        assert_eq!(borrowed.kind, TaskKind::Recluster);
        assert_eq!(policy.queue.lock().running, 2);
        policy.finish_table_job(&borrowed, None);
        policy.finish_table_job(&key, None);
        while let Some((key, _)) = policy.take_ready_task().0 {
            policy.finish_table_job(&key, None);
        }
        assert!(policy.queue.lock().tables.is_empty());
        // Use a fresh controlled queue for the same-table boundary and shared capacity.
        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        let handoff = SchedulerCore::create(2, 32, runner, idle_spawner());
        let mut task = test_task(1);
        task.kind = TaskKind::Recluster;
        handoff.enqueue(task);
        let (Some((key, _)), _) = handoff.take_ready_task() else {
            panic!("low start");
        };
        handoff.enqueue(test_task(1));
        handoff.enqueue(test_task(2));
        let (Some((other, _)), _) = handoff.take_ready_task() else {
            panic!("unrelated high");
        };
        assert_ne!(key.table, other.table);
        handoff.finish_table_job(&other, None);
        // Other tables can exhaust the global High burst while this Low is running.
        handoff.queue.lock().high_streak = HIGH_PRIORITY_BURST;
        let capacity = handoff.queue.lock().ready.capacity();
        handoff.finish_table_job(&key, Some(Instant::now()));
        assert_eq!(handoff.queue.lock().ready.capacity(), capacity);
        let (Some((next, _)), _) = handoff.take_ready_task() else {
            panic!("same-table high");
        };
        assert_eq!(next.table, key.table);
        assert!(next.kind.is_high());
        handoff.finish_table_job(&next, None);
        let (Some((last, _)), _) = handoff.take_ready_task() else {
            panic!("Low resumes after handoff");
        };
        assert_eq!(last, key);
        handoff.finish_table_job(&last, None);
        assert!(handoff.queue.lock().tables.is_empty());
        drop(mailbox);
    }
}
