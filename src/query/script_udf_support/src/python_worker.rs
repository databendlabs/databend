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
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Field;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::OnceCell;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::transform_udf_script::python_pool::RESTRICTED_PYTHON_CODE;
use super::transform_udf_script::venv::TempDir;

pub const PYTHON_WORKER_ARG: &str = "--internal-python-udf-worker";
// Bound allocations before reading a frame, including malformed worker output.
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

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
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        Self {
            max_workers: 16,
            max_workers_per_tenant: 8,
            max_workers_per_query: 4,
            max_queued_queries: 128,
            queue_timeout: Duration::from_secs(60),
            execution_timeout: Duration::from_secs(300),
        }
    }
}

pub struct PythonWorkerManager {
    config: PythonWorkerConfig,
    slots: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    reaper_runtime: Option<tokio::runtime::Handle>,
    tenants: Mutex<HashMap<String, Weak<Semaphore>>>,
}

static DEFAULT_MANAGER: LazyLock<Arc<PythonWorkerManager>> = LazyLock::new(|| {
    PythonWorkerManager::new(PythonWorkerConfig::default()).expect("valid defaults")
});

impl PythonWorkerManager {
    pub fn new(config: PythonWorkerConfig) -> Result<Arc<Self>> {
        if config.max_workers == 0
            || config.max_workers > u32::MAX as usize
            || config.max_workers_per_tenant == 0
            || config.max_workers_per_query == 0
            || config.max_workers_per_query > config.max_workers_per_tenant
            || config.max_workers_per_tenant > config.max_workers
            || config.max_queued_queries == 0
            || config.max_queued_queries > Semaphore::MAX_PERMITS
            || config.queue_timeout.is_zero()
            || config.execution_timeout.is_zero()
        {
            return Err(ErrorCode::InvalidConfig(
                "Python worker limits must satisfy 0 < query <= tenant <= node <= u32::MAX; queue size and timeouts must be positive",
            ));
        }
        Ok(Arc::new(Self {
            slots: Arc::new(Semaphore::new(config.max_workers)),
            queued: Arc::new(Semaphore::new(config.max_queued_queries)),
            config,
            reaper_runtime: tokio::runtime::Handle::try_current().ok(),
            tenants: Mutex::new(HashMap::new()),
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

    pub fn query(self: &Arc<Self>, tenant: &str) -> Arc<PythonQueryWorkers> {
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
        Arc::new(PythonQueryWorkers {
            manager: self.clone(),
            tenant_slots,
            registration: Mutex::new((0, false)),
            admission: OnceCell::new(),
            reservation: Mutex::new(None),
            runtimes: Mutex::new(Vec::new()),
            finished: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            pipelines: AtomicUsize::new(0),
        })
    }
}

/// Shared across every local pipeline of one query. Register all runtimes during
/// pipeline construction, then reserve the COMPLETE budget before starting any
/// worker. Never incrementally acquire slots while holding live workers.
pub struct PythonQueryWorkers {
    manager: Arc<PythonWorkerManager>,
    tenant_slots: Arc<Semaphore>,
    registration: Mutex<(u32, bool)>,
    admission: OnceCell<()>,
    reservation: Mutex<Option<Arc<Reservation>>>,
    runtimes: Mutex<Vec<Weak<PythonRuntime>>>,
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

#[derive(Debug)]
struct Reservation {
    _tenant: OwnedSemaphorePermit,
    _node: OwnedSemaphorePermit,
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

    pub fn register(&self) -> Result<()> {
        let mut registration = self.registration.lock();
        if registration.1 || self.finished.load(Ordering::Acquire) {
            return Err(udf_error(
                "cannot add Python runtimes after query admission",
            ));
        }
        if registration.0 as usize >= self.manager.config.max_workers_per_query {
            return Err(udf_error(format!(
                "query requires more than {} Python workers; increase python_udf_max_workers_per_query",
                self.manager.config.max_workers_per_query
            )));
        }
        registration.0 += 1;
        Ok(())
    }

    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.cancel.notify_waiters();
        for runtime in self.runtimes.lock().iter().filter_map(Weak::upgrade) {
            if let Ok(mut state) = runtime.state.try_lock() {
                *state = RuntimeState::Failed;
            }
        }
        // Reapers hold their own Arc<Reservation> until wait() completes.
        self.reservation.lock().take();
    }

    async fn admit(&self) -> Result<Arc<Reservation>> {
        self.admission
            .get_or_try_init(|| async {
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
                    *reservation = Some(Arc::new(Reservation {
                        _tenant: tenant,
                        _node: node,
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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PythonFunction {
    pub name: String,
    pub handler: String,
    pub code: String,
    pub return_field: Field,
}

/// One worker per runtime, shared by pipeline transforms only within this query.
/// A cancelled or failed request poisons the runtime: never retry user code.
pub struct PythonRuntime {
    function: PythonFunction,
    query: Arc<PythonQueryWorkers>,
    directory: Option<TempDir>,
    state: tokio::sync::Mutex<RuntimeState>,
}

enum RuntimeState {
    New,
    Running(Box<Worker>),
    Failed,
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
            state: tokio::sync::Mutex::new(RuntimeState::New),
        });
        query.runtimes.lock().push(Arc::downgrade(&runtime));
        Ok(runtime)
    }

    pub async fn call(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let cancelled = self.query.cancel.notified();
        tokio::pin!(cancelled);
        cancelled.as_mut().enable();
        let reservation = tokio::select! {
            _ = &mut cancelled => return Err(udf_error("query has finished")),
            result = self.query.admit() => result?,
        };
        if self.query.finished.load(Ordering::Acquire) {
            return Err(udf_error("query has finished"));
        }
        let mut state = self.state.lock().await;
        let previous = std::mem::replace(&mut *state, RuntimeState::Failed);
        // Own the worker across every await. Cancellation drops/kills it rather
        // than leaving a partially consumed response in a reusable connection.
        let operation = async {
            let mut worker = match previous {
                RuntimeState::New => Worker::start(
                    reservation,
                    self.directory.clone(),
                    &self.function,
                    self.query
                        .manager
                        .reaper_runtime
                        .clone()
                        .unwrap_or_else(tokio::runtime::Handle::current),
                )
                .await
                .map(Box::new)?,
                RuntimeState::Running(worker) => worker,
                RuntimeState::Failed => return Err(udf_error("runtime failed or was cancelled")),
            };
            let input = encode_batch(batch)?;
            worker.write_frame(&input).await?;
            let output = worker.read_response().await?;
            let batch = decode_batch(&output)?;
            Ok((worker, batch))
        };
        let (worker, result) = tokio::select! {
            _ = &mut cancelled => return Err(udf_error("query has finished")),
            result = tokio::time::timeout(self.query.manager.config.execution_timeout, operation) => {
                result.map_err(|_| udf_error("execution timed out"))??
            }
        };
        if self.query.finished.load(Ordering::Acquire) {
            return Err(udf_error("query has finished"));
        }
        *state = RuntimeState::Running(worker);
        Ok(result)
    }
}

struct Worker {
    child: Option<Child>,
    socket: UnixStream,
    reservation: Arc<Reservation>,
    directory: Option<TempDir>,
    runtime: tokio::runtime::Handle,
}

// A private listener path is used instead of stdout (UDF print() is arbitrary).
// No fork-without-exec, Python objects, or inherited interpreter state.
impl Worker {
    async fn start(
        reservation: Arc<Reservation>,
        directory: Option<TempDir>,
        function: &PythonFunction,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self> {
        let directory = Some(match directory {
            Some(dir) => dir,
            None => TempDir::new().map_err(udf_error)?,
        });
        let ipc_dir = TempDir::new().map_err(udf_error)?;
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(ipc_dir.path(), std::fs::Permissions::from_mode(0o700))
                .map_err(udf_error)?;
        }
        let socket_path = ipc_dir.path().join("ipc");
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(udf_error)?;
        let executable = std::env::current_exe().map_err(udf_error)?;
        let mut command = Command::new(executable);
        command
            .arg(PYTHON_WORKER_ARG)
            .arg(&socket_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(dir) = &directory {
            command.current_dir(dir.path());
        }
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn().map_err(udf_error)?;
        let process_group = child.id().ok_or_else(|| udf_error("missing worker pid"))?;
        log::debug!("Started Python UDF worker pid={process_group}");
        // The startup guard also reaps a child if accept is cancelled or fails.
        let mut guard = ChildGuard {
            child: Some(child),
            process_group,
            reservation: reservation.clone(),
            directory: directory.clone(),
            runtime: runtime.clone(),
        };
        let socket = tokio::select! {
            connection = listener.accept() => connection.map_err(udf_error)?.0,
            status = guard.child.as_mut().unwrap().wait() => {
                return Err(udf_error(format!("exited during startup: {:?}", status.map_err(udf_error)?)));
            }
        };
        let mut worker = Self {
            child: guard.child.take(),
            socket,
            reservation,
            directory,
            runtime,
        };
        worker
            .write_frame(&serde_json::to_vec(function).map_err(udf_error)?)
            .await?;
        worker.read_response().await?;
        Ok(worker)
    }

    async fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        check_length(bytes.len())?;
        self.socket
            .write_u32(bytes.len() as u32)
            .await
            .map_err(udf_error)?;
        self.socket.write_all(bytes).await.map_err(udf_error)
    }

    async fn read_response(&mut self) -> Result<Vec<u8>> {
        let status = self.socket.read_u8().await.map_err(udf_error)?;
        let len = self.socket.read_u32().await.map_err(udf_error)? as usize;
        check_length(len)?;
        let mut bytes = vec![0; len];
        self.socket
            .read_exact(&mut bytes)
            .await
            .map_err(udf_error)?;
        match status {
            0 => Ok(bytes),
            1 => Err(udf_error(String::from_utf8_lossy(&bytes))),
            _ => Err(udf_error("invalid response status")),
        }
    }
}

struct ChildGuard {
    child: Option<Child>,
    process_group: u32,
    reservation: Arc<Reservation>,
    directory: Option<TempDir>,
    runtime: tokio::runtime::Handle,
}

fn reap(
    child: &mut Option<Child>,
    process_group: u32,
    reservation: Arc<Reservation>,
    directory: Option<TempDir>,
    runtime: &tokio::runtime::Handle,
) {
    if let Some(mut child) = child.take() {
        // Keep quota AND files until the process has actually exited, including
        // cancellation during startup. Count starting/exiting workers too.
        if let Some(pid) = child.id() {
            // SAFETY: negative pid targets the worker's dedicated process group.
            // An unreaped child PID cannot have been reused by another process.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = child.start_kill();
        let _entered = runtime.enter();
        databend_common_base::runtime::spawn(async move {
            if let Err(error) = child.wait().await {
                log::warn!("Failed to reap Python UDF worker pid={process_group}: {error}");
            }
            log::debug!("Reaped Python UDF worker pid={process_group}");
            drop(directory);
            drop(reservation);
        });
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        reap(
            &mut self.child,
            self.process_group,
            self.reservation.clone(),
            self.directory.clone(),
            &self.runtime,
        );
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let process_group = self.child.as_ref().and_then(Child::id).unwrap_or_default();
        reap(
            &mut self.child,
            process_group,
            self.reservation.clone(),
            self.directory.clone(),
            &self.runtime,
        );
    }
}

fn check_length(len: usize) -> Result<()> {
    if len > MAX_FRAME_BYTES {
        return Err(udf_error(format!(
            "IPC frame exceeds {MAX_FRAME_BYTES} bytes; reduce batch size"
        )));
    }
    Ok(())
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).map_err(udf_error)?;
        writer.write(batch).map_err(udf_error)?;
        writer.finish().map_err(udf_error)?;
    }
    check_length(bytes.len())?;
    Ok(bytes)
}

fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(bytes, None).map_err(udf_error)?;
    let batch = reader
        .next()
        .ok_or_else(|| udf_error("missing Arrow batch"))?
        .map_err(udf_error)?;
    if reader.next().is_some() {
        return Err(udf_error("unexpected extra Arrow batch"));
    }
    Ok(batch)
}

fn read_frame(socket: &mut std::os::unix::net::UnixStream) -> Result<Vec<u8>> {
    let mut len = [0; 4];
    socket.read_exact(&mut len).map_err(udf_error)?;
    let len = u32::from_be_bytes(len) as usize;
    check_length(len)?;
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes).map_err(udf_error)?;
    Ok(bytes)
}

fn respond(socket: &mut std::os::unix::net::UnixStream, result: Result<Vec<u8>>) -> Result<()> {
    let (status, bytes) = match result {
        Ok(bytes) => (0, bytes),
        Err(error) => (1, error.message().into_bytes()),
    };
    check_length(bytes.len())?;
    socket.write_all(&[status]).map_err(udf_error)?;
    socket
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(udf_error)?;
    socket.write_all(&bytes).map_err(udf_error)
}

// Monitor peer closure independently of Python. A SIGKILLed query node cannot
// run Rust drops, and a UDF may never return to the protocol loop. Unlike Linux
// PR_SET_PDEATHSIG this does not kill a worker when just its spawning Tokio
// thread retires. This is lifecycle management, not a sandbox against hostile code.
fn watch_parent(socket: &std::os::unix::net::UnixStream) -> Result<()> {
    use std::os::fd::AsRawFd;
    let socket = socket.try_clone().map_err(udf_error)?;
    databend_common_base::runtime::Thread::named_spawn(
        Some("python-parent-watch".to_owned()),
        move || {
            let mut descriptor = libc::pollfd {
                fd: socket.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            loop {
                // SAFETY: descriptor is initialized and its owning socket stays
                // alive for this loop. We observe hangup only, never consume IPC.
                let result = unsafe { libc::poll(&mut descriptor, 1, -1) };
                if result < 0
                    && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                {
                    continue;
                }
                if result < 0
                    || descriptor.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0
                {
                    // SAFETY: kill a group only when this worker is its leader.
                    // _exit terminates the entire worker even if Python holds GIL.
                    unsafe {
                        let pid = libc::getpid();
                        if libc::getpgrp() == pid {
                            libc::kill(-pid, libc::SIGKILL);
                        }
                        libc::_exit(1);
                    }
                }
            }
        },
    );
    Ok(())
}

/// Internal binary entrypoint. Must run before initializing the query service,
/// crash tracker, Tokio thread pools, or command-line configuration.
pub fn run_python_udf_worker(socket_path: PathBuf) -> Result<()> {
    let mut socket = std::os::unix::net::UnixStream::connect(socket_path).map_err(udf_error)?;
    watch_parent(&socket)?;
    let init = (|| {
        let function: PythonFunction =
            serde_json::from_slice(&read_frame(&mut socket)?).map_err(udf_error)?;
        let mut runtime = arrow_udf_runtime::python::Builder::default()
            .safe_codes(RESTRICTED_PYTHON_CODE.to_owned())
            .build()
            .map_err(udf_error)?;
        runtime
            .add_function_with_handler(
                &function.name,
                function.return_field,
                arrow_udf_runtime::CallMode::CalledOnNullInput,
                &function.code,
                &function.handler,
            )
            .map_err(udf_error)?;
        Ok((runtime, function.name))
    })();
    let (runtime, name) = match init {
        Ok(value) => {
            respond(&mut socket, Ok(Vec::new()))?;
            value
        }
        Err(error) => {
            respond(&mut socket, Err(error))?;
            return Ok(());
        }
    };
    loop {
        let bytes = read_frame(&mut socket)?;
        let result = (|| {
            let input = decode_batch(&bytes)?;
            let output = runtime.call(&name, &input).map_err(udf_error)?;
            encode_batch(&output)
        })();
        let failed = result.is_err();
        respond(&mut socket, result)?;
        if failed {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserves_complete_query_budget_and_releases_on_drop() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 2,
            max_workers_per_tenant: 2,
            max_workers_per_query: 2,
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a");
        first.register().unwrap();
        first.register().unwrap();
        assert!(first.register().is_err());
        first.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 0);
        assert!(first.register().is_err());
        let second = manager.query("b");
        second.register().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.admit())
                .await
                .is_err()
        );
        // A cancelled admission cannot leak a partially acquired permit.
        assert_eq!(second.tenant_slots.available_permits(), 2);
        drop(first);
        second.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 1);
        drop(second);
        assert_eq!(manager.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn tenant_quota_is_shared_without_blocking_other_tenants() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 2,
            max_workers_per_tenant: 1,
            max_workers_per_query: 1,
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a");
        let second = manager.query("a");
        let other = manager.query("b");
        for query in [&first, &second, &other] {
            query.register().unwrap();
        }
        first.admit().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.admit())
                .await
                .is_err()
        );
        other.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 0);
    }

    #[tokio::test]
    async fn fragment_completion_does_not_finish_other_pipelines() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig::default()).unwrap();
        let query = manager.query("a");
        let first = query.pipeline();
        let second = query.pipeline();
        query.register().unwrap();
        query.admit().await.unwrap();
        drop(first);
        assert!(!query.finished.load(Ordering::Acquire));
        assert_eq!(manager.slots.available_permits(), 15);
        drop(second);
        assert!(query.finished.load(Ordering::Acquire));
        assert_eq!(manager.slots.available_permits(), 16);
    }

    #[tokio::test]
    async fn finish_keeps_quota_until_worker_reservations_are_dropped() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig::default()).unwrap();
        let query = manager.query("a");
        query.register().unwrap();
        let worker_reservation = query.admit().await.unwrap();
        query.finish();
        assert!(query.admit().await.is_err());
        assert!(query.register().is_err());
        assert_eq!(manager.slots.available_permits(), 15);
        drop(worker_reservation);
        assert_eq!(manager.slots.available_permits(), 16);
    }

    #[tokio::test]
    async fn admission_queue_is_bounded_and_times_out() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 1,
            max_workers_per_tenant: 1,
            max_workers_per_query: 1,
            max_queued_queries: 1,
            queue_timeout: Duration::from_millis(20),
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a");
        first.register().unwrap();
        first.admit().await.unwrap();
        let second = manager.query("a");
        second.register().unwrap();
        let third = manager.query("b");
        third.register().unwrap();
        let waiting = second.admit();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(
            third
                .admit()
                .await
                .unwrap_err()
                .message()
                .contains("queue is full")
        );
        assert!(waiting.await.unwrap_err().message().contains("timed out"));
        first.finish();
        third.admit().await.unwrap();
    }

    #[test]
    fn arrow_ipc_preserves_zero_argument_row_count_and_nulls() {
        use arrow_array::Int64Array;
        use arrow_array::RecordBatchOptions;
        use arrow_schema::DataType;
        use arrow_schema::Schema;
        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(
            decode_batch(&encode_batch(&empty).unwrap())
                .unwrap()
                .num_rows(),
            3
        );
        let field = Field::new("x", DataType::Int64, true).with_metadata(HashMap::from([(
            "extension-key".to_owned(),
            "extension-value".to_owned(),
        )]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(
            Int64Array::from(vec![Some(42), None, Some(-1)]),
        )])
        .unwrap();
        assert_eq!(decode_batch(&encode_batch(&batch).unwrap()).unwrap(), batch);
    }

    #[test]
    fn validates_config_and_frame_size() {
        assert!(
            PythonWorkerManager::new(PythonWorkerConfig {
                max_workers: 0,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            PythonWorkerManager::new(PythonWorkerConfig {
                max_workers_per_query: 17,
                ..Default::default()
            })
            .is_err()
        );
        assert!(check_length(MAX_FRAME_BYTES + 1).is_err());
    }
}
