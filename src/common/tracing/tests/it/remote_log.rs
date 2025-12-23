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
use std::sync::Arc;
use std::time::Duration;

use async_channel::bounded;
use databend_common_base::base::GlobalInstance;
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_tracing::Config;
use databend_common_tracing::GlobalLogger;
use databend_common_tracing::LogMessage;
use databend_common_tracing::RemoteLog;
use databend_common_tracing::RemoteLogBuffer;
use databend_common_tracing::RemoteLogElement;
use databend_common_tracing::RemoteLogGuard;
use databend_common_tracing::convert_to_batch;
use log::Level;
use log::Record;
use opendal::Operator;
use opendal::services;

fn setup() -> Result<(RemoteLog, Box<RemoteLogGuard>)> {
    let mut labels = BTreeMap::new();
    labels.insert("cluster_id".into(), "cluster_id1".into());
    labels.insert("node_id".into(), "node_id1".into());
    let (remote_log, guard) = RemoteLog::new(&labels, &Config::default())?;

    Ok((remote_log, guard))
}

fn get_remote_log_elements() -> RemoteLogElement {
    RemoteLogElement {
        timestamp: jiff::Timestamp::now().as_microsecond(),
        path: "databend_query::interpreters::common::query_log: query_log.rs:71".to_string(),
        target: "databend::log::query".to_string(),
        cluster_id: "test_cluster".to_string(),
        node_id: "izs9przqhAN4n5hbJanJm2".to_string(),
        query_id: Some("89ad07ad-83fe-4424-8005-4c5b318a7212".to_string()),
        warehouse_id: None,
        log_level: "INFO".to_string(),
        message: "test".to_string(),
        fields: r#"{"key":"test"}"#.to_string(),
    }
}

#[test]
fn test_basic_parse() -> Result<()> {
    let (remote_log, _guard) = setup()?;
    let record = Record::builder()
        .args(format_args!("begin to list files"))
        .level(Level::Info)
        .target("databend_query::sessions::query_ctx")
        .module_path(Some("databend_query::sessions::query_ctx"))
        .file(Some("query_ctx.rs"))
        .line(Some(656))
        .build();

    let remote_log_element = remote_log.prepare_log_element(&record);

    assert_eq!(remote_log_element.cluster_id, "cluster_id1");
    assert_eq!(remote_log_element.node_id, "node_id1");
    assert_eq!(
        remote_log_element.path,
        "databend_query::sessions::query_ctx: query_ctx.rs:656"
    );

    assert_eq!(&remote_log_element.message, "begin to list files");

    Ok(())
}

#[test]
fn test_convert_to_batch() -> Result<()> {
    let elements = vec![get_remote_log_elements()];
    let _ = convert_to_batch(elements).unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_buffer_flush_with_buffer_limit() -> Result<()> {
    let (tx, rx) = bounded(10);
    let interval = Duration::from_secs(100).as_micros() as u64;
    let buffer = Arc::new(RemoteLogBuffer::new(tx.clone(), interval));
    for _i in 0..5005 {
        buffer.log(get_remote_log_elements())?
    }
    let res = rx.recv().await.unwrap();
    if let LogMessage::Flush(elements) = res {
        assert_eq!(elements.len(), 5000);
    }
    Ok(())
}

#[test]
fn test_buffer_flush_with_buffer_interval() -> Result<()> {
    init_global_logger()?;
    let (tx, rx) = bounded(10);
    let interval = Duration::from_secs(1).as_micros() as u64;
    let buffer = Arc::new(RemoteLogBuffer::new(tx.clone(), interval));
    for _i in 0..5 {
        buffer.log(get_remote_log_elements())?
    }
    std::thread::sleep(Duration::from_secs(1));
    buffer.log(get_remote_log_elements())?;
    let res = rx.recv_blocking().unwrap();
    if let LogMessage::Flush(elements) = res {
        assert_eq!(elements.len(), 6);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_buffer_flush_with_force_collect() -> Result<()> {
    // This simulates guard is dropped and collect is called
    let (tx, rx) = bounded(10);
    let interval = Duration::from_secs(100).as_micros() as u64;
    let buffer = Arc::new(RemoteLogBuffer::new(tx.clone(), interval));
    for _i in 0..500 {
        buffer.log(get_remote_log_elements())?
    }
    buffer.collect()?;
    let res = rx.recv().await.unwrap();
    if let LogMessage::Flush(elements) = res {
        assert_eq!(elements.len(), 500);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flush() -> Result<()> {
    let builder = services::Memory::default();
    let op = Operator::new(builder)?.finish();

    let path = "test_logs.parquet";
    let elements = vec![get_remote_log_elements()];
    let result = RemoteLog::do_flush(op.clone(), elements, path).await;
    assert!(result.is_ok());

    let exists = op.exists(path).await?;
    assert!(exists);

    Ok(())
}

fn init_global_logger() -> Result<()> {
    let thread = std::thread::current();
    GlobalInstance::init_testing(thread.name().unwrap());
    let instance = GlobalLogger::dummy();
    GlobalInstance::set(instance);
    Ok(())
}
