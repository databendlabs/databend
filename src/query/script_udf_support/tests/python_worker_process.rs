// Copyright 2026 Datafuse Labs
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
        .query("probe");
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
    let a = manager.query("tenant");
    let b = manager.query("tenant");
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
    let fresh = manager.query("tenant");
    let mut tf = transform(fresh.clone(), "same_udf", code);
    call(&mut tf, 33).await.unwrap();
    assert!(call(&mut ta, 11).await.is_err());
    fresh.finish();

    // Reserve both UDF slots atomically; one query must never deadlock while
    // holding its first worker and waiting for its second.
    let query = manager.query("tenant");
    let mut first = transform(query.clone(), "first", "def handler(x):\n    return x\n");
    let mut second = transform(query.clone(), "second", "def handler(x):\n    return x\n");
    call(&mut first, 1).await.unwrap();
    call(&mut second, 2).await.unwrap();
    query.finish();

    // Native/process exit must fail only this UDF, without replaying the batch.
    let crashing = manager.query("tenant");
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
    let hanging = manager.query("tenant");
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
    let healthy = manager.query("tenant");
    let mut healthy_transform = transform(
        healthy.clone(),
        "healthy",
        "def handler(x):\n    return x\n",
    );
    call(&mut healthy_transform, 7).await.unwrap();
    healthy.finish();

    // Cancellation during an in-flight request must also unblock a queued query.
    let cancelled = manager.query("tenant");
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
    let last = manager.query("tenant");
    let mut tl = transform(last.clone(), "last", "def handler(x):\n    return x\n");
    call(&mut tl, 9).await.unwrap();
    last.finish();
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
            tokio::time::timeout(Duration::from_secs(45), process_tests())
                .await
                .unwrap();
        });
    println!(
        "Python worker process isolation, reuse, quota, failure, timeout and cancellation: passed"
    );
}
