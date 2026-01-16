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
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::builder::StringBuilder;
use arrow_array::builder::TimestampMicrosecondBuilder;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::bounded;
use concurrent_queue::ConcurrentQueue;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::spawn;
use databend_common_exception::Result;
use jiff::Timestamp;
use log::Record;
use logforth::Append;
use logforth::Diagnostic;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use serde_json::Map;

use crate::Config;
use crate::GlobalLogger;
use crate::loggers::collect_kvs;

/// An appender that sends log records to persistent storage
#[derive(Debug)]
pub struct RemoteLog {
    cluster_id: String,
    node_id: String,
    warehouse_id: Option<String>,
    buffer: Arc<LogBuffer>,
}

#[derive(Debug)]
pub struct RemoteLogElement {
    pub timestamp: i64,
    pub path: String,
    pub target: String,
    pub cluster_id: String,
    pub node_id: String,
    pub query_id: Option<String>,
    pub warehouse_id: Option<String>,
    pub log_level: String,
    pub message: String,
    pub fields: String,
}

#[derive(Debug)]
pub struct LogBuffer {
    queue: ConcurrentQueue<RemoteLogElement>,
    last_collect: AtomicU64,
    sender: Sender<LogMessage>,
    /// interval is in microseconds
    interval: u64,
}

/// Guard for `RemoteLog` to ensure the log is flushed before exiting
pub struct RemoteLogGuard {
    _rt: Runtime,
    buffer: Arc<LogBuffer>,
}

pub enum LogMessage {
    Flush(Vec<RemoteLogElement>),
    ForceFlush(Vec<RemoteLogElement>, mpsc::Sender<Result<()>>),
}

impl Drop for RemoteLogGuard {
    fn drop(&mut self) {
        self.buffer.force_collect();
    }
}

impl RemoteLog {
    pub fn new(
        labels: &BTreeMap<String, String>,
        cfg: &Config,
    ) -> Result<(RemoteLog, Box<RemoteLogGuard>)> {
        // all interval in RemoteLog is microseconds
        let interval = Duration::from_secs(cfg.history.interval as u64).as_micros();
        let stage_name = cfg.history.stage_name.clone();
        let node_id = labels.get("node_id").cloned().unwrap_or_default();
        let rt = Runtime::with_worker_threads(2, Some("remote-log-writer".to_string()))?;
        let (tx, rx) = bounded(1);
        // warehouse_id need to be specified after `create warehouse`
        // TODO: inject warehouse_id like query_id
        let warehouse_id = None;
        let remote_log = RemoteLog {
            cluster_id: labels.get("cluster_id").cloned().unwrap_or_default(),
            node_id: node_id.clone(),
            warehouse_id,
            buffer: Arc::new(LogBuffer::new(tx.clone(), interval as u64)),
        };
        rt.spawn(async move { RemoteLog::work(rx, &stage_name).await }, None);
        let guard = RemoteLogGuard {
            _rt: rt,
            buffer: remote_log.buffer.clone(),
        };
        Ok((remote_log, Box::new(guard)))
    }

    pub async fn work(receiver: Receiver<LogMessage>, stage_name: &str) {
        while let Ok(log_element) = receiver.recv().await {
            match log_element {
                LogMessage::Flush(flush_buffer) => {
                    Self::flush_without_wait(flush_buffer, stage_name).await;
                }
                LogMessage::ForceFlush(flush_buffer, sender) => {
                    let result = Self::flush_and_wait(flush_buffer, stage_name).await;
                    let _ = sender.send(result);
                }
            }
        }
        let _ = receiver.close();
    }

    pub async fn flush_without_wait(flush_buffer: Vec<RemoteLogElement>, stage_name: &str) {
        if let Some((op, path)) = Self::flush_operation(&flush_buffer, stage_name).await {
            let path_clone = path.clone();
            spawn(async move {
                if let Err(e) = Self::do_flush(op, flush_buffer, &path_clone).await {
                    eprintln!("Failed to flush logs: {:?}", e);
                }
            });
        }
    }

    pub async fn flush_and_wait(
        flush_buffer: Vec<RemoteLogElement>,
        stage_name: &str,
    ) -> Result<()> {
        match Self::flush_operation(&flush_buffer, stage_name).await {
            Some((op, path)) => Self::do_flush(op, flush_buffer, &path).await,
            None => Ok(()),
        }
    }

    async fn flush_operation(
        flush_buffer: &[RemoteLogElement],
        stage_name: &str,
    ) -> Option<(Operator, String)> {
        if flush_buffer.is_empty() {
            return None;
        }

        let op = GlobalLogger::instance().get_operator().await;
        op.as_ref()?;

        let path = format!(
            "stage/internal/{}/{}.parquet",
            stage_name,
            uuid::Uuid::new_v4()
        );

        Some((op.unwrap(), path))
    }

    pub async fn do_flush(
        op: Operator,
        flush_buffer: Vec<RemoteLogElement>,
        path: &str,
    ) -> Result<()> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_bloom_filter_enabled(false)
            .build();
        let batch = convert_to_batch(flush_buffer)?;
        let mut buf = vec![];
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        op.write(path, buf).await?;
        Ok(())
    }

    pub fn prepare_log_element(&self, record: &Record) -> RemoteLogElement {
        let timestamp = Timestamp::now().as_microsecond();
        let query_id = ThreadTracker::query_id().cloned();
        let target = record.target().to_string();
        let message = record.args().to_string();
        let fields = Map::from_iter(collect_kvs(record.key_values()));
        let fields = serde_json::to_string(&fields).unwrap_or_default();

        let log_level = record.level().to_string();

        let path = format!(
            "{}: {}:{}",
            record.module_path().unwrap_or(""),
            Path::new(record.file().unwrap_or_default())
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
            record.line().unwrap_or(0)
        );

        RemoteLogElement {
            timestamp,
            path,
            target,
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            warehouse_id: self.warehouse_id.clone(),
            query_id,
            log_level,
            message,
            fields,
        }
    }
}

impl Append for RemoteLog {
    fn append(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<()> {
        // fsync wal
        let log_element = self.prepare_log_element(record);
        self.buffer
            .log(log_element)
            .map_err(|e| anyhow::anyhow!("Failed to push log element: {}", e))?;

        Ok(())
    }

    fn flush(&self) {
        self.buffer.force_collect();
    }
}

impl LogBuffer {
    const MAX_BUFFER_SIZE: usize = 5000;

    pub fn new(sender: Sender<LogMessage>, interval: u64) -> Self {
        Self {
            queue: ConcurrentQueue::unbounded(),
            last_collect: AtomicU64::new(Timestamp::now().as_microsecond() as u64),
            sender,
            interval,
        }
    }

    /// log will trigger a collect either when the buffer is full or the interval is reached
    pub fn log(&self, log_element: RemoteLogElement) -> anyhow::Result<()> {
        self.queue.push(log_element)?;
        if self.queue.len() >= Self::MAX_BUFFER_SIZE {
            self.last_collect
                .store(Timestamp::now().as_microsecond() as u64, Ordering::SeqCst);
            self.collect()?;
        }
        let now = Timestamp::now().as_microsecond() as u64;
        let mut current_last_collect = 0;
        loop {
            match self.last_collect.compare_exchange_weak(
                current_last_collect,
                now,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // Ensure operator is ready before collecting logs to prevent data loss during startup.
                    // If operator is not ready, skip collection this round and keep log entries in the queue
                    // MAX_BUFFER_SIZE limit can help to prevent unbounded queue growth.
                    if !GlobalLogger::instance().ready.load(Ordering::SeqCst) {
                        break;
                    }
                    self.collect()?;
                    break;
                }
                Err(last_collect) => {
                    if now - self.interval < last_collect {
                        break;
                    }
                    current_last_collect = last_collect;
                }
            }
        }
        Ok(())
    }

    pub fn collect(&self) -> anyhow::Result<()> {
        self.sender
            .send_blocking(LogMessage::Flush(Vec::from_iter(self.queue.try_iter())))
            .map_err(|e| anyhow::anyhow!("Failed to send log element: {}", e))?;
        Ok(())
    }

    pub fn force_collect(&self) {
        let (tx, rx) = channel();
        if self
            .sender
            .send_blocking(LogMessage::ForceFlush(
                Vec::from_iter(self.queue.try_iter()),
                tx,
            ))
            .is_ok()
        {
            let _ = rx.recv_timeout(Duration::from_secs(3));
        }
    }
}

pub fn convert_to_batch(records: Vec<RemoteLogElement>) -> Result<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("path", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("log_level", DataType::Utf8, true),
        Field::new("cluster_id", DataType::Utf8, true),
        Field::new("node_id", DataType::Utf8, true),
        Field::new("warehouse_id", DataType::Utf8, true),
        Field::new("query_id", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("fields", DataType::Utf8, true),
    ]);

    let mut timestamp = TimestampMicrosecondBuilder::with_capacity(records.len());
    let mut path = StringBuilder::new();
    let mut target = StringBuilder::new();
    let mut log_level = StringBuilder::new();
    let mut cluster_id = StringBuilder::new();
    let mut node_id = StringBuilder::new();
    let mut warehouse_id = StringBuilder::new();
    let mut query_id = StringBuilder::new();
    let mut message = StringBuilder::new();
    let mut fields = StringBuilder::new();

    for record in records {
        timestamp.append_value(record.timestamp);
        path.append_value(&record.path);
        target.append_value(&record.target);
        log_level.append_value(&record.log_level);
        cluster_id.append_value(&record.cluster_id);
        node_id.append_value(&record.node_id);
        warehouse_id.append_option(record.warehouse_id);
        query_id.append_option(record.query_id);
        message.append_value(&record.message);
        fields.append_value(&record.fields);
    }

    RecordBatch::try_new(Arc::new(schema), vec![
        Arc::new(timestamp.finish()),
        Arc::new(path.finish()),
        Arc::new(target.finish()),
        Arc::new(log_level.finish()),
        Arc::new(cluster_id.finish()),
        Arc::new(node_id.finish()),
        Arc::new(warehouse_id.finish()),
        Arc::new(query_id.finish()),
        Arc::new(message.finish()),
        Arc::new(fields.finish()),
    ])
    .map_err(|e| {
        databend_common_exception::ErrorCode::Internal(format!(
            "Failed to create record batch for remote log: {}",
            e
        ))
    })
}
