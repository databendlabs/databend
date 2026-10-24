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

// Included in python_worker: scheduling is separate from the IPC implementation.

/// A query-owned pool. Neither idle workers nor interpreter state cross queries.
pub struct PythonRuntime {
    function: PythonFunction,
    query: Arc<PythonQueryWorkers>,
    directory: Option<TempDir>,
    pool: Mutex<PoolState>,
    available: tokio::sync::Notify,
    failed: tokio::sync::Notify,
    idle_changed: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct PoolState {
    // Includes checked-out workers and startup reservations, but not reapers.
    // Reapers keep the physical quota separately until the child has exited.
    live: usize,
    idle: Vec<IdleWorker>,
    idle_reaper: Option<tokio::task::AbortHandle>,
    failed: bool,
}

struct IdleWorker {
    worker: Worker,
    idle_since: Instant,
}

impl Drop for PythonRuntime {
    fn drop(&mut self) {
        if let Some(reaper) = self.pool.get_mut().idle_reaper.take() {
            reaper.abort();
        }
    }
}

impl PythonRuntime {
    pub(crate) fn new(
        function: PythonFunction,
        query: Arc<PythonQueryWorkers>,
        directory: Option<TempDir>,
    ) -> Result<Arc<Self>> {
        query.register()?;
        let runtime = Arc::new(Self {
            function,
            query: query.clone(),
            directory,
            pool: Mutex::new(PoolState::default()),
            available: tokio::sync::Notify::new(),
            failed: tokio::sync::Notify::new(),
            idle_changed: Arc::new(tokio::sync::Notify::new()),
        });
        query.runtimes.lock().push(Arc::downgrade(&runtime));
        Ok(runtime)
    }

    fn fail(&self) {
        let idle = {
            let mut pool = self.pool.lock();
            pool.failed = true;
            if let Some(reaper) = pool.idle_reaper.take() {
                reaper.abort();
            }
            pool.live -= pool.idle.len();
            std::mem::take(&mut pool.idle)
        };
        drop(idle);
        self.failed.notify_waiters();
        self.available.notify_waiters();
    }

    fn retire_idle_extra(&self) -> bool {
        let retired = {
            let mut pool = self.pool.lock();
            let Some(index) = pool
                .idle
                .iter()
                .position(|idle| idle.worker.reservation.extra)
            else {
                return false;
            };
            pool.live -= 1;
            pool.idle.swap_remove(index)
        };
        drop(retired);
        self.idle_changed.notify_one();
        self.available.notify_waiters();
        true
    }

    /// One event-driven timer per pool, only while idle extras exist. The task
    /// holds a Weak runtime across sleeps, so it cannot prolong a query's life.
    fn start_idle_reaper(self: &Arc<Self>, pool: &mut PoolState) {
        if pool.idle_reaper.is_some() {
            return;
        }
        let runtime = Arc::downgrade(self);
        let changed = self.idle_changed.clone();
        let handle = self
            .query
            .manager
            .reaper_runtime
            .clone()
            .unwrap_or_else(tokio::runtime::Handle::current);
        let _entered = handle.enter();
        let task = databend_common_base::runtime::spawn(async move {
            loop {
                let change = changed.notified();
                tokio::pin!(change);
                change.as_mut().enable();
                let deadline = {
                    let Some(runtime) = runtime.upgrade() else {
                        return;
                    };
                    let timeout = runtime.query.manager.config.idle_timeout;
                    let now = Instant::now();
                    let (retired, deadline) = {
                        let mut pool = runtime.pool.lock();
                        let retired: Vec<_> = pool
                            .idle
                            .extract_if(.., |idle| {
                                idle.worker.reservation.extra
                                    && now.saturating_duration_since(idle.idle_since) >= timeout
                            })
                            .collect();
                        pool.live -= retired.len();
                        let deadline = pool
                            .idle
                            .iter()
                            .filter(|idle| idle.worker.reservation.extra)
                            .filter_map(|idle| idle.idle_since.checked_add(timeout))
                            .min();
                        if deadline.is_none() {
                            pool.idle_reaper.take();
                        }
                        (retired, deadline)
                    };
                    // Existing child reapers retain process quota and files
                    // until wait() completes. Never release them at idle removal.
                    if !retired.is_empty() {
                        drop(retired);
                        runtime.available.notify_waiters();
                    }
                    deadline
                };
                let Some(deadline) = deadline else {
                    return;
                };
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {},
                    _ = &mut change => {},
                }
            }
        });
        pool.idle_reaper = Some(task.abort_handle());
    }

    async fn checkout(self: &Arc<Self>, minimum: Arc<Reservation>) -> Result<WorkerLease> {
        loop {
            let available = self.available.notified();
            let capacity = self.query.manager.capacity_changed.notified();
            tokio::pin!(available, capacity);
            available.as_mut().enable();
            capacity.as_mut().enable();
            {
                let mut pool = self.pool.lock();
                if pool.failed || self.query.is_finished() {
                    return Err(udf_error("runtime failed or was cancelled"));
                }
                if let Some(idle) = pool.idle.pop() {
                    if idle.worker.reservation.extra {
                        self.idle_changed.notify_one();
                    }
                    return Ok(WorkerLease::new(self.clone(), Some(idle.worker), None));
                }
                let reservation = if pool.live == 0 {
                    // Every registered runtime has this guaranteed minimum.
                    Some(minimum.clone())
                } else if pool.live < self.query.parallelism {
                    // Never await incremental quota while holding live workers.
                    self.query.try_extra()
                } else {
                    None
                };
                if let Some(reservation) = reservation {
                    pool.live += 1;
                    return Ok(WorkerLease::new(self.clone(), None, Some(reservation)));
                }
            }
            // Do not strand extra capacity in an upstream operator that is now
            // idle while a downstream UDF needs parallelism. Its minimum worker
            // is never retired, so every admitted operator can still progress.
            self.query.reclaim_idle_extra(self);
            tokio::select! {
                _ = &mut available => {},
                _ = &mut capacity => {},
            }
        }
    }

    pub async fn call(self: &Arc<Self>, batch: &RecordBatch) -> Result<RecordBatch> {
        decode_batch(&self.request(&encode_batch(batch)?).await?)
    }

    /// Send one request frame to a pooled worker and return its response.
    pub(crate) async fn request(self: &Arc<Self>, request: &[u8]) -> Result<Vec<u8>> {
        let cancelled = self.query.cancel.notified();
        let failed = self.failed.notified();
        tokio::pin!(cancelled, failed);
        cancelled.as_mut().enable();
        failed.as_mut().enable();
        if self.query.is_finished() || self.pool.lock().failed {
            return Err(udf_error("runtime failed or was cancelled"));
        }
        let operation = async {
            let minimum = self.query.admit().await?;
            let _execution = self
                .query
                .execution_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(udf_error)?;
            let mut lease = self.checkout(minimum).await?;
            let response = tokio::time::timeout(
                self.query.manager.config.execution_timeout,
                lease.execute(request),
            )
            .await
            .map_err(|_| udf_error("execution timed out"))??;
            lease.complete()?;
            Ok(response)
        };
        tokio::select! {
            // Preserve the original error when dropping a failed lease also
            // wakes the pool's failure notification in this same poll.
            biased;
            result = operation => result,
            _ = &mut cancelled => Err(udf_error("query has finished")),
            _ = &mut failed => Err(udf_error("runtime failed or was cancelled")),
        }
    }
}

/// Own a worker across every await. Cancellation/error kills it and poisons the
/// pool, rather than reusing a half-read response or replaying user side effects.
struct WorkerLease {
    runtime: Arc<PythonRuntime>,
    worker: Option<Worker>,
    reservation: Option<Arc<Reservation>>,
    returned: bool,
}

impl WorkerLease {
    fn new(
        runtime: Arc<PythonRuntime>,
        worker: Option<Worker>,
        reservation: Option<Arc<Reservation>>,
    ) -> Self {
        Self {
            runtime,
            worker,
            reservation,
            returned: false,
        }
    }

    async fn execute(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        if self.worker.is_none() {
            let runtime = &self.runtime;
            self.worker = Some(
                Worker::start(
                    self.reservation.as_ref().unwrap().clone(),
                    runtime.directory.clone(),
                    &runtime.function,
                    runtime
                        .query
                        .manager
                        .reaper_runtime
                        .clone()
                        .unwrap_or_else(tokio::runtime::Handle::current),
                )
                .await?,
            );
            self.reservation.take();
        }
        let worker = self.worker.as_mut().unwrap();
        worker.write_frame(request).await?;
        worker.read_response().await
    }

    fn complete(mut self) -> Result<()> {
        let worker = self.worker.take().unwrap();
        let succeeded = {
            let mut pool = self.runtime.pool.lock();
            let succeeded = !pool.failed && !self.runtime.query.is_finished();
            let extra = worker.reservation.extra;
            let yield_capacity = extra && self.runtime.query.manager.has_waiting_admission();
            if succeeded && !yield_capacity {
                pool.idle.push(IdleWorker {
                    worker,
                    idle_since: Instant::now(),
                });
                self.returned = true;
                self.runtime.available.notify_waiters();
                if extra {
                    self.runtime.start_idle_reaper(&mut pool);
                    self.runtime.idle_changed.notify_one();
                    // Other UDF operators may now reclaim this idle capacity.
                    self.runtime.query.manager.capacity_changed.notify_waiters();
                }
                return Ok(());
            }
            pool.live -= 1;
            succeeded
        };
        self.returned = true;
        drop(worker);
        self.runtime.available.notify_waiters();
        if succeeded {
            Ok(())
        } else {
            Err(udf_error("runtime failed or was cancelled"))
        }
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        if !self.returned {
            self.runtime.fail();
            self.runtime.pool.lock().live -= 1;
            // Worker/ChildGuard reapers retain quota until wait() completes.
            self.worker.take();
            self.reservation.take();
            self.runtime.available.notify_waiters();
        }
    }
}
