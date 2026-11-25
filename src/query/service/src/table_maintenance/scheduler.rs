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
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::catch_unwind;
use databend_common_metrics::table_maintenance::set_mailbox_tables;
use parking_lot::Mutex;
use tokio::sync::Notify;
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
    Recluster,
}

impl TaskKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Refresh => "refresh",
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
    type ExecutionKey: Eq + Send + Sync;

    fn key(&self) -> TaskKey;
    fn execution_key(&self) -> Self::ExecutionKey;
    fn can_merge(
        _kind: TaskKind,
        existing: &Self::ExecutionKey,
        incoming: &Self::ExecutionKey,
    ) -> bool {
        existing == incoming
    }
    fn definition_version(&self) -> u32 {
        0
    }
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
    /// Ignore notifications that arrived during the last unit unless they target a newer definition.
    CompletedUnlessNewer(u32),
    Continue(T),
    /// Drop the exhausted task. Retry only notifications pending from this execution unit.
    RetryPendingAt(Instant),
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

type ReadyTask<T> = Option<(Arc<TaskKey>, Arc<Mailbox<T>>, usize)>;

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
    // Highest-priority due kind on this idle table, recomputed at dispatch.
    due: Option<TaskKind>,
}

// Owns every dispatch allocation until both execution and finalization have finished.
// In particular, a panic in a task merge must not strand the table gate or counters.
struct RunningGuard<T: SchedulerTask> {
    scheduler: Arc<SchedulerCore<T>>,
    key: Arc<TaskKey>,
    mailbox: Arc<Mailbox<T>>,
    finished: bool,
}

impl<T: SchedulerTask> RunningGuard<T> {
    fn finish(&mut self, deadline: Option<Instant>) {
        self.scheduler
            .finish_job(&self.key, deadline, &mut self.finished);
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
        self.0.close();
        self.0.queue.lock().dispatch_stopped = true;
        self.0.stopped.notify_waiters();
    }
}

impl<T: SchedulerTask> SchedulerCore<T> {
    pub fn create(
        max_concurrency: usize,
        max_tables: usize,
        runner: SchedulerRunner<T>,
        spawner: SchedulerSpawner,
    ) -> Arc<Self> {
        set_mailbox_tables(0);
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
                if !T::can_merge(
                    key.kind,
                    &entry.mailbox.execution_key,
                    &candidate.execution_key,
                ) {
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
            if scheduler.cancellation.is_cancelled() {
                return;
            }
            let (task, deadline) = scheduler.take_ready_task();
            if let Some((key, mailbox, demand)) = task {
                // Construct before spawning, so dropping an unpolled future also cleans up.
                let guard = RunningGuard {
                    scheduler: scheduler.clone(),
                    key,
                    mailbox,
                    finished: false,
                };
                (scheduler.spawner)(
                    Box::pin(Self::run_job(guard, demand)),
                    "table-maintenance-unit".to_string(),
                );
            } else if let Some(deadline) = deadline {
                tokio::select! {
                    _ = &mut notified => {},
                    _ = tokio::time::sleep_until(deadline.into()) => {},
                }
            } else {
                notified.await;
            }
        }
    }

    pub fn close(self: &Arc<Self>) {
        {
            let mut queue = self.queue.lock();
            if queue.closed {
                return;
            }
            queue.closed = true;
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

    #[cfg(test)]
    pub async fn shutdown(self: &Arc<Self>) {
        self.close();
        self.wait_for_shutdown().await;
    }

    pub async fn wait_for_shutdown(&self) {
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
        if queue.closed || queue.running >= self.max_concurrency {
            return (None, None);
        }
        let now = Instant::now();
        let mut due_tables = 0;
        let mut high = None;
        let mut earliest: Option<Instant> = None;
        let MailboxQueue {
            states,
            tables,
            ready,
            ..
        } = &mut *queue;
        for table in tables.values_mut() {
            table.due = None;
        }
        for (index, key) in ready.iter().enumerate() {
            let entry = &states[key];
            if entry.ready_at > now {
                earliest = Some(earliest.map_or(entry.ready_at, |old| old.min(entry.ready_at)));
                continue;
            }
            let table = tables.get_mut(&key.table).expect("registered table");
            if !table.busy {
                due_tables += usize::from(table.due.is_none());
                if key.kind.is_high() {
                    table.due = Some(key.kind);
                    high.get_or_insert(index);
                } else {
                    table.due.get_or_insert(key.kind);
                }
            }
        }
        let low = ready.iter().position(|key| {
            let table = &tables[&key.table];
            !key.kind.is_high()
                && states[key].ready_at <= now
                && !table.busy
                // Other tables can consume the High burst while this table runs Low.
                // Do not let that Low immediately reacquire its gate ahead of a due High.
                && !(table.last_was_low && table.due.is_some_and(TaskKind::is_high))
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
        // Count each runnable table once, including the newly dispatched table.
        let demand = (queue.running + due_tables).min(self.max_concurrency);
        queue.running += 1;
        let entry = queue.states.get_mut(&key).expect("registered task");
        entry.running = true;
        let mailbox = entry.mailbox.clone();
        drop(queue);
        (Some((key, mailbox, demand)), earliest)
    }

    async fn run_job(mut guard: RunningGuard<T>, demand: usize) {
        let result = CatchUnwindFuture::create(async {
            let task = guard.mailbox.pending.lock().task.take();
            let result = match task {
                Some(task) if !guard.scheduler.cancellation.is_cancelled() => {
                    (guard.scheduler.runner)(task, guard.scheduler.cancellation.clone(), demand)
                        .await
                }
                _ => RunResult::Stopped,
            };
            let mut pending = guard.mailbox.pending.lock();
            let mut min_version_exclusive = None;
            let (mut continuation, retry_at, stopped) = match result {
                RunResult::Continue(task) => (Some(task), None, false),
                RunResult::RetryPendingAt(retry_at) => (None, Some(retry_at), false),
                RunResult::Completed => (None, None, false),
                RunResult::CompletedUnlessNewer(version) => {
                    // Ordinary key changes increase the ID. FLASHBACK can restore
                    // a smaller ID, whose pending notification may be dropped.
                    min_version_exclusive = Some(version);
                    (None, None, false)
                }
                RunResult::Stopped => (None, None, true),
            };
            if !pending.retired && !stopped && !guard.scheduler.cancellation.is_cancelled() {
                if let Some(new) = pending.task.take()
                    && min_version_exclusive
                        .is_none_or(|version| new.definition_version() > version)
                {
                    match &mut continuation {
                        Some(task) => task.merge(new),
                        None => continuation = Some(new),
                    }
                }
            } else {
                continuation = None;
            }
            let deadline = continuation.as_ref().map(|task| {
                let ready_at = task.ready_at();
                retry_at.map_or(ready_at, |retry_at| ready_at.max(retry_at))
            });
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
        self.finish_job(key, deadline, &mut false);
    }

    fn finish_job(&self, key: &Arc<TaskKey>, deadline: Option<Instant>, finished: &mut bool) {
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
            // guard ownership together with the counter and table gate.
            *finished = true;
            removed
        };
        self.wake.notify_one();
        self.stopped.notify_waiters();
        drop(removed);
        self.record_metrics();
    }

    fn record_metrics(&self) {
        // The gauge was initialized at create(). Publish the table count while holding
        // the queue lock so concurrent updates cannot report a stale count.
        let queue = self.queue.lock();
        set_mailbox_tables(queue.tables.len());
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
        table_id: u64,
        payload: Vec<u64>,
        not_before: Instant,
        kind: TaskKind,
        identity: u64,
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
                    catalog: "default".to_string(),
                    table_id: self.table_id,
                },
                kind: self.kind,
            }
        }

        fn merge(&mut self, other: Self) {
            self.payload.extend(other.payload);
        }
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
            table_id,
            payload: vec![payload],
            not_before: Instant::now(),
            kind: TaskKind::Refresh,
            identity: 1,
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
    async fn test_delayed_continuation_yields_to_coalesced_work() {
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
                    let first = task.table_id == 1 && task.payload == [1];
                    runs.lock().push((task.table_id, task.payload.clone()));
                    if first {
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
        scheduler.enqueue(test_task(2));
        scheduler.enqueue(test_task_with_payload(2, 3));
        release_first.notify_one();
        wait_until(|| runs.lock().len() == 3).await;
        assert_eq!(
            *runs.lock(),
            vec![(1, vec![1]), (2, vec![2, 3]), (1, vec![1, 99])],
            "queued writes merge and run before the deferred continuation"
        );
        timeout(Duration::from_secs(2), scheduler.shutdown())
            .await
            .unwrap();
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
        assert_eq!(scheduler.queue.lock().running, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mailbox_and_dispatch_demand_are_counted_per_table() {
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
        let mut task = test_task(1);
        task.kind = TaskKind::Recluster;
        scheduler.enqueue(task);
        assert_eq!(scheduler.queue.lock().tables.len(), 1);

        // Drive dispatch directly to verify per-table demand and priority handoff.
        let runner: SchedulerRunner<TestTask> = Arc::new(|_, _, _| unreachable!());
        let handoff = SchedulerCore::create(2, 32, runner, idle_spawner());
        let mut task = test_task(1);
        task.kind = TaskKind::Recluster;
        handoff.enqueue(task);
        let (Some((key, _, _)), _) = handoff.take_ready_task() else {
            panic!("low start");
        };
        handoff.enqueue(test_task(1));
        handoff.enqueue(test_task(2));
        let (Some((other, _, demand)), _) = handoff.take_ready_task() else {
            panic!("unrelated high");
        };
        assert_eq!(demand, 2);
        assert_ne!(key.table, other.table);
        handoff.finish_table_job(&other, None);
        // Other tables can exhaust the global High burst while this Low is running.
        handoff.queue.lock().high_streak = HIGH_PRIORITY_BURST;
        handoff.finish_table_job(&key, Some(Instant::now()));
        let (Some((next, _, demand)), _) = handoff.take_ready_task() else {
            panic!("same-table high");
        };
        assert_eq!(demand, 1, "two due kinds on the same table count once");
        assert_eq!(next.table, key.table);
        assert!(next.kind.is_high());
        handoff.finish_table_job(&next, None);
        let (Some((last, _, _)), _) = handoff.take_ready_task() else {
            panic!("Low resumes after handoff");
        };
        assert_eq!(last, key);
        handoff.finish_table_job(&last, None);
        assert!(handoff.queue.lock().tables.is_empty());
    }
}
