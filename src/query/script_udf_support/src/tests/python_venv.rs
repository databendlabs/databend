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

use std::sync::mpsc;
use std::time::Duration;

use databend_common_base::runtime::Thread;
use databend_common_base::runtime::spawn;
use databend_common_cache::Cache;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::Symbol;

use super::*;

// No pip or Python interpreter is needed: seed an empty environment, then hold
// the real cache lock to simulate a slow install while calling init_runtime.
#[test]
fn test_python_venv_wait_does_not_starve_query_workers() {
    let dependency = format!("test-venv-{}", uuid::Uuid::now_v7());
    let temp_dir = venv::TempDir::new().unwrap();
    let archive = venv::archive_env(temp_dir.path()).unwrap();
    let key = venv::PyVenvKeyEntry::new(std::slice::from_ref(&dependency), vec![]);
    venv::PY_VENV_CACHE
        .write()
        .insert(key, venv::PyVenvCacheEntry::new(temp_dir, archive));

    let func = ScriptUdfFunctionDesc {
        name: "test_python_venv".to_string(),
        func_name: "handler".to_string(),
        output_column: Symbol::default(),
        arg_indices: vec![],
        arg_exprs: vec![],
        data_type: Box::new(DataType::Number(NumberDataType::Int64)),
        headers: BTreeMap::new(),
        udf_type: UDFType::Script(Box::new(UDFScriptCode {
            language: UDFLanguage::Python,
            runtime_version: String::new(),
            imports_stage_info: vec![],
            imports: vec![],
            packages: vec![dependency],
            code: Arc::new(
                b"def handler(x):\n    return x\n"
                    .to_vec()
                    .into_boxed_slice(),
            ),
        })),
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (heartbeat_tx, heartbeat_rx) = mpsc::channel();
    let guard = venv::PY_VENV_CACHE.write();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let func = func.clone();
        let started_tx = started_tx.clone();
        let _entered = rt.enter();
        tasks.push(spawn(async move {
            started_tx.send(()).unwrap();
            TransformUdfScript::init_runtime(&[func])
        }));
    }
    for _ in 0..2 {
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    // The last task may have sent its notification just before acquiring the
    // lock. Give it time to enter the blocking section before the heartbeat.
    std::thread::sleep(Duration::from_millis(100));
    {
        let _entered = rt.enter();
        spawn(async move { heartbeat_tx.send(()).unwrap() });
    }
    let heartbeat = heartbeat_rx.recv_timeout(Duration::from_secs(2));
    // Always release the lock before asserting, so a regression cannot hang
    // runtime shutdown while its workers are still parked.
    drop(guard);
    for task in tasks {
        let result = rt.block_on(task).unwrap();
        #[cfg(feature = "python-udf")]
        assert!(result.is_ok());
        #[cfg(not(feature = "python-udf"))]
        assert_eq!(result.err().unwrap().code(), ErrorCode::U_D_F_DATA_ERROR);
    }
    assert!(heartbeat.is_ok(), "Python UDF init starved query workers");
}

#[test]
fn test_python_venv_initialization_is_per_key() {
    let key = || venv::PyVenvKeyEntry::new(&[uuid::Uuid::now_v7().to_string()], vec![]);
    let slow_key = key();
    let slow_entry = venv::cache_entry(slow_key);
    let fast_key = key();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let installer = Thread::spawn(move || {
        slow_entry.get_or_init(|| {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let dir = venv::TempDir::new()?;
            let archive = venv::archive_env(dir.path()).map_err(ErrorCode::from_string)?;
            Ok((dir, archive))
        })
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (completed_tx, completed_rx) = mpsc::channel();
    let independent = Thread::spawn(move || {
        let fast_entry = venv::cache_entry(fast_key);
        let result = fast_entry.get_or_init(|| {
            let dir = venv::TempDir::new()?;
            let archive = venv::archive_env(dir.path()).map_err(ErrorCode::from_string)?;
            Ok((dir, archive))
        });
        completed_tx.send(result.is_ok()).unwrap();
    });
    let completed = completed_rx.recv_timeout(Duration::from_secs(2));
    release_tx.send(()).unwrap();
    installer.join().unwrap().unwrap();
    independent.join().unwrap();
    assert!(
        completed.unwrap(),
        "unrelated environments must initialize independently"
    );
}

#[test]
fn test_python_venv_concurrent_misses_initialize_once() {
    let entry = venv::PyVenvCacheEntry::default();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let installs = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| {
            let entry = entry.clone();
            let barrier = barrier.clone();
            let installs = installs.clone();
            Thread::spawn(move || {
                barrier.wait();
                let dir = entry
                    .get_or_init(|| {
                        installs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let dir = venv::TempDir::new()?;
                        let archive =
                            venv::archive_env(dir.path()).map_err(ErrorCode::from_string)?;
                        Ok((dir, archive))
                    })
                    .unwrap();
                barrier.wait();
                dir
            })
        })
        .collect::<Vec<_>>();
    let dirs = tasks
        .into_iter()
        .map(|task| task.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(installs.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(dirs.iter().all(|dir| dir.path() == dirs[0].path()));
}

#[test]
fn test_python_venv_retries_failure_and_restores_once() {
    let entry = venv::PyVenvCacheEntry::default();
    let err = entry
        .get_or_init(|| Err(ErrorCode::UDFRuntimeError("test install failure")))
        .err()
        .unwrap();
    assert!(err.message().contains("test install failure"));

    let temp_dir = entry
        .get_or_init(|| {
            let dir = venv::TempDir::new()?;
            std::fs::write(dir.path().join("helper.py"), b"VALUE = 42\n")?;
            let archive = venv::archive_env(dir.path()).map_err(ErrorCode::from_string)?;
            Ok((dir, archive))
        })
        .unwrap();
    let old_path = temp_dir.path().to_path_buf();
    drop(temp_dir);
    assert!(!old_path.exists());

    // All callers keep their directories alive until everyone has restored,
    // so every hit must return the same fully populated environment.
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let entry = entry.clone();
        let barrier = barrier.clone();
        tasks.push(Thread::spawn(move || {
            barrier.wait();
            let dir = entry
                .get_or_init(|| panic!("cache hit must not reinstall"))
                .unwrap();
            barrier.wait();
            assert_eq!(
                std::fs::read(dir.path().join("helper.py")).unwrap(),
                b"VALUE = 42\n"
            );
            dir
        }));
    }
    let dirs = tasks
        .into_iter()
        .map(|task| task.join().unwrap())
        .collect::<Vec<_>>();
    assert!(dirs.iter().all(|dir| dir.path() == dirs[0].path()));
    assert_ne!(dirs[0].path(), old_path);

    // Evicting/dropping the cache handle must not remove a live environment.
    drop(entry);
    assert_eq!(
        std::fs::read(dirs[0].path().join("helper.py")).unwrap(),
        b"VALUE = 42\n"
    );
}
