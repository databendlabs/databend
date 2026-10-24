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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::Symbol;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;
use databend_common_sql::plans::UDFType;
use databend_query_script_udf_support::PYTHON_WORKER_ARG;
use databend_query_script_udf_support::PythonQueryWorkers;
use databend_query_script_udf_support::PythonWorkerConfig;
use databend_query_script_udf_support::PythonWorkerManager;
use databend_query_script_udf_support::ScriptUdfFunctionDesc;
use databend_query_script_udf_support::TransformUdfScript;
use databend_query_script_udf_support::run_python_udf_worker;

fn function(name: &str, code: &str) -> ScriptUdfFunctionDesc {
    ScriptUdfFunctionDesc {
        name: name.into(),
        func_name: "handler".into(),
        output_column: Symbol::default(),
        arg_indices: vec![0],
        arg_exprs: vec!["x".into()],
        data_type: Box::new(DataType::Number(NumberDataType::Int64)),
        headers: BTreeMap::new(),
        udf_type: UDFType::Script(Box::new(UDFScriptCode {
            language: UDFLanguage::Python,
            runtime_version: String::new(),
            imports_stage_info: vec![],
            imports: vec![],
            packages: vec![],
            code: Arc::new(code.as_bytes().to_vec().into_boxed_slice()),
        })),
    }
}

fn transform(query: Arc<PythonQueryWorkers>, name: &str, code: &str) -> TransformUdfScript {
    let functions = vec![function(name, code)];
    let runtimes = TransformUdfScript::init_runtime_for_query(&functions, query).unwrap();
    TransformUdfScript::new(FunctionContext::default(), functions, runtimes)
}

fn input(value: i64) -> DataBlock {
    DataBlock::new_from_columns(vec![Int64Type::from_data(vec![value])])
}

async fn call(transform: &mut TransformUdfScript, value: i64) -> Result<()> {
    let result = transform.transform(input(value)).await?;
    let expected = DataBlock::new_from_columns(vec![
        Int64Type::from_data(vec![value]),
        Int64Type::from_data(vec![value]),
    ]);
    assert_eq!(result.num_rows(), expected.num_rows());
    assert_eq!(result.convert_to_full().columns(), expected.columns());
    Ok(())
}

async fn parent_loss_probe() {
    use std::io::Write;
    let query = PythonWorkerManager::new(PythonWorkerConfig::default())
        .unwrap()
        .query("probe", 1);
    let mut udf = transform(
        query,
        "parent_loss",
        r#"
import os, time

def handler(x):
    if x == 0:
        return os.getpid()
    time.sleep(60)
    return x
"#,
    );
    let result = udf.transform(input(0)).await.unwrap();
    println!("{}", result.get_by_offset(1).index(0).unwrap());
    std::io::stdout().flush().unwrap();
    udf.transform(input(1)).await.unwrap();
}

#[cfg(target_os = "linux")]
async fn parent_loss_test() {
    use tokio::io::AsyncBufReadExt;
    use tokio::io::BufReader;
    let mut parent = tokio::process::Command::new(std::env::current_exe().unwrap())
        .arg("--parent-loss-probe")
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(parent.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let worker_pid: u32 = line.trim().parse().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    parent.kill().await.unwrap();
    // The worker watchdog must terminate a busy UDF even though the parent
    // cannot run cleanup. A zombie is dead; reaping then belongs to init.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match std::fs::read_to_string(format!("/proc/{worker_pid}/status")) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Ok(status)
                    if status
                        .lines()
                        .any(|line| line.starts_with("State:") && line.contains('Z')) =>
                {
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("worker survived loss of its parent");
}

// The worker must not inherit query node secrets, must not regain privileges
// through setuid binaries, and must not dump UDF memory to disk on a crash.
async fn hardening_tests() {
    let query = PythonWorkerManager::new(PythonWorkerConfig::default())
        .unwrap()
        .query("hardening", 1);
    let mut udf = transform(
        query.clone(),
        "hardening",
        r#"
import os, resource

def handler(x):
    assert 'DATABEND_TEST_SECRET' not in os.environ, 'query node environment leaked'
    assert 'PATH' in os.environ
    assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
    return os.getpid()
"#,
    );
    let result = udf.transform(input(0)).await.unwrap();
    let pid = result.get_by_offset(1).index(0).unwrap().to_string();
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        assert!(
            status
                .lines()
                .any(|line| line.split_whitespace().eq(["NoNewPrivs:", "1"])),
            "worker {pid} runs without no_new_privs"
        );
    }
    #[cfg(not(target_os = "linux"))]
    let _ = pid;
    query.finish();

    // A dependency must never reach pip as an option, URL or path. Rejected
    // before pip runs, so this needs no pip or network access.
    let query = PythonWorkerManager::new(PythonWorkerConfig::default())
        .unwrap()
        .query("hardening", 1);
    let injected = function(
        "pip_injection",
        "# /// script\n# dependencies = [\"--index-url=http://127.0.0.1:1/simple\"]\n# ///\ndef handler(x):\n    return x\n",
    );
    let error = TransformUdfScript::init_runtime_for_query(&[injected], query.clone())
        .err()
        .expect("option-like dependency must be rejected");
    assert!(
        error.message().contains("Invalid Python UDF dependency"),
        "{}",
        error.message()
    );
    query.finish();
}

// User code runs inside Landlock + seccomp: query node files, network, child
// programs and other processes are out of reach, while the private working
// directory, temporary files, threads and the standard library keep working.
// Linux only: macOS workers run unconfined for local development.
#[cfg(target_os = "linux")]
async fn sandbox_tests() {
    let secret =
        std::env::temp_dir().join(format!("databend-sandbox-secret-{}", std::process::id()));
    std::fs::write(&secret, "query node secret").unwrap();
    // A reachable TCP service proves the denial comes from the sandbox.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let code = format!(
        r#"
import datetime, decimal, json, os, socket, subprocess, tempfile, threading

SECRET = {secret:?}

def read_secret(): open(SECRET).read()
def read_passwd(): open('/etc/passwd').read()
def read_parent_environ(): open('/proc/%d/environ' % os.getppid()).read()
def list_root(): os.listdir('/')
def write_outside(): open(SECRET + '.new', 'w').write('x')
def remove_outside(): os.unlink(SECRET)
def chmod_outside(): os.chmod(SECRET, 0o777)
def tcp(): socket.create_connection(('127.0.0.1', {port}), timeout=1)
def unix_socket(): socket.socket(socket.AF_UNIX)
def spawn(): subprocess.run(['/bin/true'])
def signal_parent(): os.kill(os.getppid(), 0)
def signal_everyone(): os.kill(-1, 0)

def handler(x):
    for action in (read_secret, read_passwd, read_parent_environ, list_root,
                   write_outside, remove_outside, chmod_outside, tcp,
                   unix_socket, spawn, signal_parent, signal_everyone):
        try:
            action()
        except PermissionError:
            continue
        raise AssertionError('sandbox allowed ' + action.__name__)
    assert tempfile.gettempdir() == os.getcwd()
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'ok')
    with open('local.txt', 'w') as f:
        f.write('ok')
    with open('local.txt') as f:
        assert f.read() == 'ok'
    os.kill(os.getpid(), 0)
    worker = threading.Thread(target=lambda: json.dumps([str(decimal.Decimal(1))]))
    worker.start()
    worker.join()
    datetime.datetime.now().astimezone()
    return os.getpid()
"#,
        secret = secret.to_str().unwrap(),
    );
    let query = PythonWorkerManager::new(PythonWorkerConfig::default())
        .unwrap()
        .query("sandbox", 1);
    let mut udf = transform(query.clone(), "sandbox", &code);
    let result = udf.transform(input(0)).await.unwrap();
    let pid = result.get_by_offset(1).index(0).unwrap().to_string();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    assert!(
        status
            .lines()
            .any(|line| line.split_whitespace().eq(["Seccomp:", "2"])),
        "worker {pid} runs without a seccomp filter"
    );
    query.finish();
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "query node secret"
    );
    std::fs::remove_file(&secret).unwrap();
    drop(listener);
}

fn transforms(
    query: Arc<PythonQueryWorkers>,
    name: &str,
    code: &str,
    count: usize,
) -> Vec<TransformUdfScript> {
    let functions = vec![function(name, code)];
    let runtimes = TransformUdfScript::init_runtime_for_query(&functions, query).unwrap();
    (0..count)
        .map(|_| {
            TransformUdfScript::new(
                FunctionContext::default(),
                functions.clone(),
                runtimes.clone(),
            )
        })
        .collect()
}

async fn pids(transforms: &mut [TransformUdfScript]) -> BTreeSet<String> {
    futures::future::join_all(transforms.iter_mut().map(|udf| udf.transform(input(0))))
        .await
        .into_iter()
        .map(|result| {
            result
                .unwrap()
                .get_by_offset(1)
                .index(0)
                .unwrap()
                .to_string()
        })
        .collect()
}

async fn parallel_pool_tests() {
    let manager = PythonWorkerManager::new(PythonWorkerConfig {
        max_workers: 8,
        ..Default::default()
    })
    .unwrap();
    let code = "import os, time\ndef handler(x):\n    time.sleep(0.05)\n    return os.getpid()\n";
    for threads in [1, 4, 8] {
        let query = manager.query("parallel", threads);
        let mut batch = transforms(query.clone(), "pid", code, 8);
        assert_eq!(pids(&mut batch).await.len(), threads);
        // Same workers are reused, rather than fresh processes per batch.
        assert_eq!(pids(&mut batch).await.len(), threads);
        query.finish();
    }
    // Growth is demand-driven: max_threads=8 does not preallocate 8 workers.
    let query = manager.query("small", 8);
    let mut small = transforms(query.clone(), "small", code, 1);
    let first = pids(&mut small).await;
    assert_eq!(pids(&mut small).await, first);
    query.finish();

    // Two operators reserve their minima together. Extra capacity must be able
    // to move from an idle upstream pool to a busy downstream pool and back.
    let manager = PythonWorkerManager::new(PythonWorkerConfig {
        max_workers: 4,
        ..Default::default()
    })
    .unwrap();
    let query = manager.query("operators", 4);
    let mut upstream = transforms(query.clone(), "upstream", code, 3);
    let mut downstream = transforms(query.clone(), "downstream", code, 3);
    assert_eq!(pids(&mut upstream).await.len(), 3);
    assert_eq!(pids(&mut downstream).await.len(), 3);
    assert_eq!(pids(&mut upstream).await.len(), 3);
    query.finish();

    // Idle extras in an unfinished query must yield to another query's minimum
    // budget, including when its operators need multiple guaranteed workers.
    let first = manager.query("fair", 4);
    let mut first_batch = transforms(first.clone(), "first", code, 4);
    assert_eq!(pids(&mut first_batch).await.len(), 4);
    let second = manager.query("fair", 4);
    let mut left = transforms(second.clone(), "left", code, 1);
    let mut right = transforms(second.clone(), "right", code, 1);
    let (left_ids, right_ids) = tokio::join!(pids(&mut left), pids(&mut right));
    assert_eq!(left_ids.len(), 1);
    assert_eq!(right_ids.len(), 1);
    second.finish();
    assert!(!pids(&mut first_batch).await.is_empty());
    first.finish();

    // Cancelling a multi-worker query must kill all in-flight workers and wake
    // both pool and CPU-slot waiters. The next query must regain all capacity.
    let query = manager.query("cancel-pool", 4);
    let mut batch = transforms(
        query.clone(),
        "slow",
        "import time\ndef handler(x):\n    time.sleep(60)\n    return x\n",
        8,
    );
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        query.finish();
    };
    let calls = futures::future::join_all(batch.iter_mut().map(|udf| udf.transform(input(0))));
    let (results, ()) = tokio::join!(calls, cancel);
    assert!(results.iter().all(Result::is_err));
    let next = manager.query("after-cancel", 4);
    let mut batch = transforms(next.clone(), "healthy", code, 4);
    assert_eq!(pids(&mut batch).await.len(), 4);
    next.finish();

    // A failing worker poisons the whole pool, not just its own connection.
    let query = manager.query("failure-pool", 4);
    let mut batch = transforms(
        query.clone(),
        "crash-pool",
        "import os, time\ndef handler(x):\n    if x == 0:\n        os._exit(17)\n    time.sleep(60)\n    return x\n",
        4,
    );
    let results = futures::future::join_all(
        batch
            .iter_mut()
            .enumerate()
            .map(|(index, udf)| udf.transform(input(index as i64))),
    )
    .await;
    assert!(results.iter().all(Result::is_err));
    query.finish();
    let next = manager.query("after-failure", 4);
    let mut batch = transforms(next.clone(), "healthy", code, 4);
    assert_eq!(pids(&mut batch).await.len(), 4);
    next.finish();
}

async fn idle_pool_tests() {
    let manager = PythonWorkerManager::new(PythonWorkerConfig {
        max_workers: 4,
        // Real child processes use OS time; unit tests cover the default 300s
        // with paused Tokio time, without making this harness wait five minutes.
        idle_timeout: Duration::from_secs(3),
        ..Default::default()
    })
    .unwrap();
    let query = manager.query("idle", 4);
    let code = "import os, time\ndef handler(x):\n    time.sleep(4 if x else 0.05)\n    return os.getpid()\n";
    let mut batch = transforms(query.clone(), "idle", code, 4);
    let base = pids(&mut batch[..1]).await;
    let original = pids(&mut batch).await;
    assert_eq!(original.len(), 4);
    assert!(base.is_subset(&original));
    // All four workers are busy longer than the idle timeout. None may be killed.
    let busy = futures::future::join_all(batch.iter_mut().map(|udf| udf.transform(input(1))))
        .await
        .into_iter()
        .map(|result| {
            result
                .unwrap()
                .get_by_offset(1)
                .index(0)
                .unwrap()
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(busy, original);
    let extras: Vec<i32> = original
        .difference(&base)
        .map(|pid| pid.parse().unwrap())
        .collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // SAFETY: signal 0 only checks existence; it sends no signal.
            if extras.iter().all(|pid| unsafe { libc::kill(*pid, 0) != 0 }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("idle workers were not terminated and reaped without new requests");
    assert_eq!(pids(&mut batch[..1]).await, base);
    // Released quota permits demand-driven expansion again in the same SQL.
    let regrown = pids(&mut batch).await;
    assert_eq!(regrown.len(), 4);
    assert!(base.is_subset(&regrown));
    query.finish();
}

async fn process_tests() {
    let manager = PythonWorkerManager::new(PythonWorkerConfig {
        max_workers: 2,
        max_workers_per_tenant: 2,
        max_workers_per_query: 2,
        ..Default::default()
    })
    .unwrap();
    // Simultaneous queries execute exactly the same UDF definition. Each writes
    // its own marker to process-global state, sleeps, then checks for pollution.
    let code = r#"
import builtins, os, sys, time

def handler(x):
    marker = 'databend-isolation-' + str(x)
    assert not any(p.startswith('databend-isolation-') and p != marker for p in sys.path)
    assert not hasattr(builtins, '_databend_marker') or builtins._databend_marker == x
    sys.path.append(marker)
    builtins._databend_marker = x
    os.environ['DATABEND_UDF_MARKER'] = marker
    sys.modules['_databend_test_module'] = marker
    cwd = os.getcwd()
    time.sleep(0.05)
    assert sys.path[-1] == marker
    assert builtins._databend_marker == x
    assert os.environ['DATABEND_UDF_MARKER'] == marker
    assert sys.modules['_databend_test_module'] == marker
    assert os.getcwd() == cwd
    print('this must not corrupt IPC')
    return x
"#;
    let a = manager.query("tenant", 1);
    let b = manager.query("tenant", 1);
    let mut ta = transform(a.clone(), "same_udf", code);
    let mut tb = transform(b.clone(), "same_udf", code);
    let (ra, rb) = tokio::join!(call(&mut ta, 11), call(&mut tb, 22));
    ra.unwrap();
    rb.unwrap();
    // Reuse within a query, but release workers and permits on finish even if
    // transforms and QueryContext-like owners are retained.
    call(&mut ta, 11).await.unwrap();
    a.finish();
    b.finish();
    let fresh = manager.query("tenant", 1);
    let mut tf = transform(fresh.clone(), "same_udf", code);
    call(&mut tf, 33).await.unwrap();
    assert!(call(&mut ta, 11).await.is_err());
    fresh.finish();

    // Reserve both UDF slots atomically; one query must never deadlock while
    // holding its first worker and waiting for its second.
    let query = manager.query("tenant", 1);
    let mut first = transform(query.clone(), "first", "def handler(x):\n    return x\n");
    let mut second = transform(query.clone(), "second", "def handler(x):\n    return x\n");
    call(&mut first, 1).await.unwrap();
    call(&mut second, 2).await.unwrap();
    query.finish();

    // Native/process exit must fail only this UDF, without replaying the batch.
    let crashing = manager.query("tenant", 1);
    let mut tc = transform(
        crashing.clone(),
        "crash",
        "import os\ndef handler(x):\n    os._exit(17)\n",
    );
    assert!(call(&mut tc, 1).await.is_err());
    assert!(call(&mut tc, 1).await.is_err());
    crashing.finish();

    // A hanging worker is killed and reaped before the single slot is reusable.
    let manager = PythonWorkerManager::new(PythonWorkerConfig {
        max_workers: 1,
        max_workers_per_tenant: 1,
        max_workers_per_query: 1,
        execution_timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .unwrap();
    let hanging = manager.query("tenant", 1);
    let mut th = transform(
        hanging.clone(),
        "hang",
        "import time\ndef handler(x):\n    time.sleep(60)\n    return x\n",
    );
    assert!(
        call(&mut th, 1)
            .await
            .unwrap_err()
            .message()
            .contains("timed out")
    );
    hanging.finish();
    let healthy = manager.query("tenant", 1);
    let mut healthy_transform = transform(
        healthy.clone(),
        "healthy",
        "def handler(x):\n    return x\n",
    );
    call(&mut healthy_transform, 7).await.unwrap();
    healthy.finish();

    // Cancellation during an in-flight request must also unblock a queued query.
    let cancelled = manager.query("tenant", 1);
    let mut slow = transform(
        cancelled.clone(),
        "cancel",
        "import time\ndef handler(x):\n    time.sleep(60)\n    return x\n",
    );
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancelled.finish();
    };
    let (result, ()) = tokio::join!(call(&mut slow, 1), cancel);
    assert!(result.is_err());
    let last = manager.query("tenant", 1);
    let mut tl = transform(last.clone(), "last", "def handler(x):\n    return x\n");
    call(&mut tl, 9).await.unwrap();
    last.finish();
    hardening_tests().await;
    #[cfg(target_os = "linux")]
    sandbox_tests().await;
    parallel_pool_tests().await;
    idle_pool_tests().await;
    #[cfg(target_os = "linux")]
    parent_loss_test().await;
}

fn main() {
    databend_common_base::runtime::ThreadTracker::init();
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    if mode.as_deref() == Some(std::ffi::OsStr::new(PYTHON_WORKER_ARG)) {
        let result = run_python_udf_worker(args.next().unwrap().into());
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    // SAFETY: no other thread exists yet; the Tokio runtime is built below.
    unsafe { std::env::set_var("DATABEND_TEST_SECRET", "must-not-reach-workers") };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if mode.as_deref() == Some(std::ffi::OsStr::new("--parent-loss-probe")) {
                parent_loss_probe().await;
                return;
            }
            tokio::time::timeout(Duration::from_secs(90), process_tests())
                .await
                .unwrap();
        });
    println!(
        "Python worker process isolation, hardening, reuse, quota, failure, timeout and cancellation: passed"
    );
}
