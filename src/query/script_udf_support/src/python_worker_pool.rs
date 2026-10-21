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
}

#[derive(Default)]
struct PoolState {
    // Includes checked-out workers and startup reservations, but not reapers.
    // Reapers keep the physical quota separately until the child has exited.
    live: usize,
    idle: Vec<Worker>,
    failed: bool,
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
        });
        query.runtimes.lock().push(Arc::downgrade(&runtime));
        Ok(runtime)
    }

    fn fail(&self) {
        let idle = {
            let mut pool = self.pool.lock();
            pool.failed = true;
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
            let Some(index) = pool.idle.iter().position(|worker| worker.reservation.extra) else {
                return false;
            };
            pool.live -= 1;
            pool.idle.swap_remove(index)
        };
        drop(retired);
        self.available.notify_waiters();
        true
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
                if let Some(worker) = pool.idle.pop() {
                    return Ok(WorkerLease::new(self.clone(), Some(worker), None));
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
            let batch = tokio::time::timeout(
                self.query.manager.config.execution_timeout,
                lease.execute(batch),
            )
            .await
            .map_err(|_| udf_error("execution timed out"))??;
            lease.complete()?;
            Ok(batch)
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

    async fn execute(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
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
        worker.write_frame(&encode_batch(batch)?).await?;
        decode_batch(&worker.read_response().await?)
    }

    fn complete(mut self) -> Result<()> {
        let worker = self.worker.take().unwrap();
        let succeeded = {
            let mut pool = self.runtime.pool.lock();
            let succeeded = !pool.failed && !self.runtime.query.is_finished();
            let extra = worker.reservation.extra;
            let yield_capacity = extra && self.runtime.query.manager.has_waiting_admission();
            if succeeded && !yield_capacity {
                pool.idle.push(worker);
                self.returned = true;
                self.runtime.available.notify_waiters();
                if extra {
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
