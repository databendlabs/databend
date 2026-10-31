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
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Field;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::transform_udf_script::python_pool::RESTRICTED_PYTHON_CODE;
use super::transform_udf_script::venv::TempDir;

pub const PYTHON_WORKER_ARG: &str = "--internal-python-udf-worker";

fn udf_error(error: impl std::fmt::Display) -> ErrorCode {
    ErrorCode::UDFRuntimeError(format!("Python UDF worker: {error}"))
}

#[derive(Clone, Debug)]
pub struct PythonWorkerConfig {
    pub max_workers: usize,
    pub max_workers_per_tenant: usize,
    pub max_workers_per_query: usize,
    pub max_queued_queries: usize,
    pub queue_timeout: Duration,
    pub execution_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        Self {
            max_workers: 0,
            max_workers_per_tenant: 0,
            max_workers_per_query: 0,
            max_queued_queries: 128,
            queue_timeout: Duration::from_secs(60),
            execution_timeout: Duration::from_secs(300),
            idle_timeout: Duration::from_secs(300),
        }
    }
}

impl PythonWorkerConfig {
    /// Resolve node/tenant capacities, leaving a zero query limit automatic.
    pub fn resolve_limits(mut self, num_cpus: usize) -> Result<Self> {
        if self.max_workers == 0 {
            self.max_workers = num_cpus.max(1);
        }
        if self.max_workers_per_tenant == 0 {
            self.max_workers_per_tenant = self.max_workers;
        }
        if self.max_workers > u32::MAX as usize
            || self.max_workers > Semaphore::MAX_PERMITS
            || self.max_workers_per_tenant > self.max_workers
            || self.max_workers_per_query > self.max_workers_per_tenant
            || self.max_queued_queries == 0
            || self.max_queued_queries > Semaphore::MAX_PERMITS
            || self.queue_timeout.is_zero()
            || self.execution_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || Instant::now().checked_add(self.idle_timeout).is_none()
        {
            return Err(ErrorCode::InvalidConfig(
                "Python worker limits must satisfy query <= tenant <= node <= u32::MAX (0 = automatic); queue capacity and timeouts must be positive and the idle timeout must fit the timer range",
            ));
        }
        Ok(self)
    }
}

pub struct PythonWorkerManager {
    config: PythonWorkerConfig,
    slots: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    capacity_changed: Arc<tokio::sync::Notify>,
    reaper_runtime: Option<tokio::runtime::Handle>,
    tenants: Mutex<HashMap<String, Weak<Semaphore>>>,
    queries: Mutex<Vec<Weak<PythonQueryWorkers>>>,
}

static DEFAULT_MANAGER: LazyLock<Arc<PythonWorkerManager>> = LazyLock::new(|| {
    PythonWorkerManager::new(PythonWorkerConfig::default()).expect("valid defaults")
});

impl PythonWorkerManager {
    pub fn new(config: PythonWorkerConfig) -> Result<Arc<Self>> {
        let config =
            config.resolve_limits(std::thread::available_parallelism().map_or(1, usize::from))?;
        if !PYTHON_SANDBOX_ENFORCED {
            log::warn!(
                "Python UDF workers run WITHOUT the OS sandbox on this platform (local development only): user code can access files, network and processes of the query service user"
            );
        }
        Ok(Arc::new(Self {
            slots: Arc::new(Semaphore::new(config.max_workers)),
            queued: Arc::new(Semaphore::new(config.max_queued_queries)),
            capacity_changed: Arc::new(tokio::sync::Notify::new()),
            config,
            reaper_runtime: tokio::runtime::Handle::try_current().ok(),
            tenants: Mutex::new(HashMap::new()),
            queries: Mutex::new(Vec::new()),
        }))
    }

    pub fn init(config: PythonWorkerConfig) -> Result<()> {
        let manager = Self::new(config)?;
        GlobalInstance::set(manager);
        Ok(())
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::try_get().unwrap_or_else(|| DEFAULT_MANAGER.clone())
    }

    pub fn query(self: &Arc<Self>, tenant: &str, max_threads: usize) -> Arc<PythonQueryWorkers> {
        let mut tenants = self.tenants.lock();
        tenants.retain(|_, slots| slots.strong_count() > 0);
        let tenant_slots = tenants
            .entry(tenant.to_owned())
            .or_default()
            .upgrade()
            .unwrap_or_else(|| {
                let slots = Arc::new(Semaphore::new(self.config.max_workers_per_tenant));
                tenants.insert(tenant.to_owned(), Arc::downgrade(&slots));
                slots
            });
        let mut parallelism = max_threads
            .max(1)
            .min(self.config.max_workers)
            .min(self.config.max_workers_per_tenant);
        if self.config.max_workers_per_query != 0 {
            parallelism = parallelism.min(self.config.max_workers_per_query);
        }
        Arc::new(PythonQueryWorkers {
            tenant: tenant.to_owned(),
            manager: self.clone(),
            parallelism,
            execution_slots: Arc::new(Semaphore::new(parallelism)),
            extra_slots: OnceLock::new(),
            tenant_slots,
            registration: Mutex::new((0, false)),
            admission: OnceCell::new(),
            reservation: Mutex::new(None),
            runtimes: Mutex::new(Vec::new()),
            aggregate_registration: Mutex::new(()),
            finished: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            pipelines: AtomicUsize::new(0),
        })
    }

    fn has_waiting_admission(&self) -> bool {
        self.queued.available_permits() < self.config.max_queued_queries
    }

    fn reclaim_idle_extras(&self, tenant: &Arc<Semaphore>, count: usize) {
        let mut node_needed = count.saturating_sub(self.slots.available_permits());
        let mut tenant_needed = count.saturating_sub(tenant.available_permits());
        for query in self.queries.lock().iter().filter_map(Weak::upgrade) {
            let same_tenant = Arc::ptr_eq(tenant, &query.tenant_slots);
            for runtime in query.runtimes.lock().iter().filter_map(Weak::upgrade) {
                while node_needed > 0 || (same_tenant && tenant_needed > 0) {
                    if !runtime.retire_idle_extra() {
                        break;
                    }
                    node_needed = node_needed.saturating_sub(1);
                    if same_tenant {
                        tenant_needed = tenant_needed.saturating_sub(1);
                    }
                }
            }
        }
    }
}

/// Shared across local pipelines. Reserve one minimum worker per runtime as a
/// single admission, then grow pools opportunistically without waiting for extra
/// quota. max_threads bounds execution, not the number of UDF operators.
pub struct PythonQueryWorkers {
    tenant: String,
    manager: Arc<PythonWorkerManager>,
    parallelism: usize,
    execution_slots: Arc<Semaphore>,
    extra_slots: OnceLock<Arc<Semaphore>>,
    tenant_slots: Arc<Semaphore>,
    registration: Mutex<(u32, bool)>,
    admission: OnceCell<()>,
    reservation: Mutex<Option<Arc<Reservation>>>,
    runtimes: Mutex<Vec<Weak<PythonRuntime>>>,
    /// Serializes `aggregate_runtime` lookups with the registration they may
    /// perform, so one aggregate never registers twice.
    aggregate_registration: Mutex<()>,
    finished: AtomicBool,
    cancel: tokio::sync::Notify,
    pipelines: AtomicUsize,
}

/// A local fragment may finish while another fragment still uses Python. Only
/// the last pipeline lease releases the shared query budget and idle workers.
pub struct PythonPipelineLease {
    query: Arc<PythonQueryWorkers>,
}

impl Drop for PythonPipelineLease {
    fn drop(&mut self) {
        if self.query.pipelines.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.query.finish();
        }
    }
}

struct AdmissionWake(Arc<tokio::sync::Notify>);

impl Drop for AdmissionWake {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[derive(Debug)]
struct Reservation {
    // Drop permits before waking waiters, not after the Drop body has returned.
    permits: Option<ReservationPermits>,
    extra: bool,
    capacity_changed: Arc<tokio::sync::Notify>,
}

#[derive(Debug)]
struct ReservationPermits {
    _tenant: OwnedSemaphorePermit,
    _node: OwnedSemaphorePermit,
    _extra: Option<OwnedSemaphorePermit>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.permits.take();
        self.capacity_changed.notify_waiters();
    }
}

impl PythonQueryWorkers {
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    pub fn pipeline(self: &Arc<Self>) -> PythonPipelineLease {
        self.pipelines.fetch_add(1, Ordering::Relaxed);
        PythonPipelineLease {
            query: self.clone(),
        }
    }

    /// One runtime per aggregate function and query. Partial and final
    /// aggregation (and every plan fragment) build their own aggregate
    /// instance; registering each would spend the per-query worker minimum on
    /// duplicates of the same stateless worker pool.
    pub(crate) fn aggregate_runtime(
        self: &Arc<Self>,
        function: PythonFunction,
        integrations: Vec<String>,
    ) -> Result<Arc<PythonRuntime>> {
        let _guard = self.aggregate_registration.lock();
        let existing = self
            .runtimes
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .find(|runtime| {
                runtime.function == function && runtime.external_access_integrations == integrations
            });
        match existing {
            Some(runtime) => Ok(runtime),
            None => PythonRuntime::new(function, self.clone(), None, integrations),
        }
    }

    pub fn register(self: &Arc<Self>) -> Result<()> {
        let mut registration = self.registration.lock();
        if registration.1 || self.finished.load(Ordering::Acquire) {
            return Err(udf_error(
                "cannot add Python runtimes after query admission",
            ));
        }
        let config = &self.manager.config;
        let minimum_limit = if config.max_workers_per_query == 0 {
            config.max_workers_per_tenant
        } else {
            config.max_workers_per_query
        };
        if registration.0 as usize >= minimum_limit {
            return Err(udf_error(format!(
                "query minimum Python worker budget exceeds {minimum_limit}; increase Python worker capacity",
            )));
        }
        let first = registration.0 == 0;
        registration.0 += 1;
        drop(registration);
        if first {
            // Most query pipelines never execute Python. Do not grow a global
            // reclamation registry with ordinary SQL or completed HTTP contexts.
            let mut queries = self.manager.queries.lock();
            queries.retain(|query| query.upgrade().is_some_and(|query| !query.is_finished()));
            queries.push(Arc::downgrade(self));
        }
        Ok(())
    }

    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.cancel.notify_waiters();
        for runtime in self.runtimes.lock().iter().filter_map(Weak::upgrade) {
            runtime.fail();
        }
        // Reapers hold their own Arc<Reservation> until wait() completes.
        self.reservation.lock().take();
    }

    fn try_extra(&self) -> Option<Arc<Reservation>> {
        let query = self.extra_slots.get()?.clone().try_acquire_owned().ok()?;
        let tenant = self.tenant_slots.clone().try_acquire_owned().ok()?;
        let node = self.manager.slots.clone().try_acquire_owned().ok()?;
        Some(Arc::new(Reservation {
            permits: Some(ReservationPermits {
                _tenant: tenant,
                _node: node,
                _extra: Some(query),
            }),
            extra: true,
            capacity_changed: self.manager.capacity_changed.clone(),
        }))
    }

    fn reclaim_idle_extra(&self, requester: &PythonRuntime) {
        for runtime in self.runtimes.lock().iter().filter_map(Weak::upgrade) {
            if !std::ptr::eq(runtime.as_ref(), requester) && runtime.retire_idle_extra() {
                break;
            }
        }
    }

    async fn admit(&self) -> Result<Arc<Reservation>> {
        self.admission
            .get_or_try_init(|| async {
                // Declare before permits: cancellation must release semaphore
                // wait tickets and tenant quota BEFORE waking pool expanders.
                let _wake = AdmissionWake(self.manager.capacity_changed.clone());
                let _queued = self
                    .manager
                    .queued
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| udf_error("worker admission queue is full"))?;
                let count = {
                    let mut registration = self.registration.lock();
                    registration.1 = true;
                    registration.0
                };
                // Minima take priority over idle speculative capacity. Busy
                // extras yield on completion, without interrupting user code.
                self.manager
                    .reclaim_idle_extras(&self.tenant_slots, count as usize);
                tokio::time::timeout(self.manager.config.queue_timeout, async {
                    // All queries acquire in the same order. A tenant waiting for
                    // its own quota must not occupy node quota.
                    let tenant = self
                        .tenant_slots
                        .clone()
                        .acquire_many_owned(count)
                        .await
                        .map_err(udf_error)?;
                    let node = self
                        .manager
                        .slots
                        .clone()
                        .acquire_many_owned(count)
                        .await
                        .map_err(udf_error)?;
                    let mut reservation = self.reservation.lock();
                    if self.finished.load(Ordering::Acquire) {
                        return Err(udf_error("query has finished"));
                    }
                    let extra = if self.manager.config.max_workers_per_query == 0 {
                        self.parallelism.saturating_sub(1)
                    } else {
                        self.manager.config.max_workers_per_query - count as usize
                    };
                    self.extra_slots
                        .get_or_init(|| Arc::new(Semaphore::new(extra)));
                    *reservation = Some(Arc::new(Reservation {
                        permits: Some(ReservationPermits {
                            _tenant: tenant,
                            _node: node,
                            _extra: None,
                        }),
                        extra: false,
                        capacity_changed: self.manager.capacity_changed.clone(),
                    }));
                    Ok(())
                })
                .await
                .map_err(|_| udf_error("timed out waiting for worker quota"))?
            })
            .await?;
        self.reservation
            .lock()
            .clone()
            .ok_or_else(|| udf_error("query has finished"))
    }
}

/// The function a worker serves; see `arrow_udf_python_worker::WorkerFunction`.
pub(crate) type PythonFunction = arrow_udf_python_worker::WorkerFunction;
pub(crate) use arrow_udf_python_worker::AggregateOp;
pub(crate) use arrow_udf_python_worker::SANDBOX_ENFORCED as PYTHON_SANDBOX_ENFORCED;

/// A scalar or aggregate Python function of a query, executed by workers.
pub(crate) fn python_function(
    name: String,
    handler: String,
    code: String,
    return_field: Field,
    aggregate_state: Option<Field>,
) -> PythonFunction {
    PythonFunction {
        name,
        handler,
        code,
        return_field,
        aggregate_state,
        prelude: Some(RESTRICTED_PYTHON_CODE.to_owned()),
    }
}

pub(crate) fn array_batch(array: arrow_array::ArrayRef) -> Result<RecordBatch> {
    arrow_udf_python_worker::array_batch(array).map_err(udf_error)
}

pub(crate) fn encode_aggregate_request(op: AggregateOp, parts: &[RecordBatch]) -> Result<Vec<u8>> {
    arrow_udf_python_worker::encode_aggregate_request(op, parts).map_err(udf_error)
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    arrow_udf_python_worker::encode_batch(batch).map_err(udf_error)
}

pub(crate) fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    arrow_udf_python_worker::decode_batch(bytes).map_err(udf_error)
}

include!("python_worker_pool.rs");

/// A started worker together with the quota it occupies. The process (see
/// `arrow_udf_python_worker::WorkerProcess`) keeps a clone of the reservation
/// and its private directory until it has exited.
struct Worker {
    /// `None` only for scheduling tests, which never send requests.
    process: Option<arrow_udf_python_worker::WorkerProcess>,
    reservation: Arc<Reservation>,
}

impl Worker {
    async fn start(
        reservation: Arc<Reservation>,
        directory: Option<TempDir>,
        function: &PythonFunction,
        network_proxy: Option<Arc<dyn arrow_udf_python_worker::NetworkProxy>>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self> {
        let directory = match directory {
            Some(template) => {
                let quota = reservation.clone();
                databend_common_base::runtime::spawn_blocking(move || {
                    // A cancelled startup must not admit unlimited background
                    // dependency copies after dropping its process reservation.
                    let _quota = quota;
                    template.private_copy()
                })
                .await
                .map_err(udf_error)?
                .map_err(udf_error)?
            }
            None => TempDir::new().map_err(udf_error)?,
        };
        let mut command = arrow_udf_python_worker::WorkerCommand::current_exe([PYTHON_WORKER_ARG])
            .map_err(udf_error)?;
        // Native Internet sockets stay denied, with or without a proxy binding.
        command.allow_network = false;
        command.network_proxy = network_proxy;
        let working_dir = directory.path().to_path_buf();
        let process = arrow_udf_python_worker::WorkerProcess::spawn(
            &command,
            function,
            &working_dir,
            // Quota and files stay alive until the child has actually exited.
            Box::new((reservation.clone(), directory)),
            runtime,
        )
        .await
        .map_err(udf_error)?;
        log::debug!("Started Python UDF worker pid={:?}", process.id());
        Ok(Self {
            process: Some(process),
            reservation,
        })
    }

    async fn request(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let process = self
            .process
            .as_mut()
            .ok_or_else(|| udf_error("worker is not running"))?;
        process.request(request).await.map_err(udf_error)
    }
}

/// Internal binary entrypoint. Must run before initializing the query service,
/// crash tracker, Tokio thread pools, or command-line configuration.
pub fn run_python_udf_worker(socket_path: PathBuf) -> Result<()> {
    arrow_udf_python_worker::run_worker(&socket_path, arrow_udf_python_worker::WorkerHooks {
        // jemalloc (`background_thread:true` in the query binary) runs purge
        // threads that would stay outside the sandbox.
        before_sandbox: Some(Box::new(|| {
            databend_common_base::mem_allocator::stop_allocator_background_threads()
                .map_err(anyhow::Error::msg)
        })),
    })
    .map_err(udf_error)
}

include!("python_worker_tests.rs");
