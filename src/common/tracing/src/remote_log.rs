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
use std::fmt::Formatter;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::StringBuilder;
use arrow_array::builder::TimestampMicrosecondBuilder;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::time::interval;
use databend_common_base::base::uuid;
use databend_common_base::runtime::spawn;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use log::Record;
use logforth::layout::collect_kvs;
use logforth::Append;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use serde_json::Map;

use crate::Config;
use crate::GlobalLogger;

/// An appender that sends log records to persistent storage
pub struct RemoteLog {
    sender: mpsc::Sender<Message>,
    cluster_id: String,
    node_id: String,
    tenant_id: String,
}

pub struct RemoteLogElement {
    pub timestamp: i64,
    pub path: String,
    pub cluster_id: String,
    pub node_id: String,
    pub query_id: Option<String>,
    pub log_level: String,
    pub messages: String,
}

pub enum Message {
    LogElement(RemoteLogElement),
    Shutdown,
    Flush,
}

/// Guard for `RemoteLog` to ensure the log is flushed before exiting
pub struct RemoteLogGuard {
    _rt: Runtime,
    sender: mpsc::Sender<Message>,
}

impl Drop for RemoteLogGuard {
    fn drop(&mut self) {
        if self.sender.try_send(Message::Shutdown).is_ok() {
            // wait for the log to be flushed
            // TODO: use blocking sleep here, may have better wayn
            std::thread::sleep(Duration::from_secs(3));
        }
    }
}

impl RemoteLog {
    pub fn new(
        labels: &BTreeMap<String, String>,
        cfg: &Config,
    ) -> Result<(RemoteLog, Box<RemoteLogGuard>)> {
        let interval = cfg.persistentlog.interval;
        let stage_name = cfg.persistentlog.stage_name.clone();
        let node_id = labels.get("node_id").cloned().unwrap_or_default();
        let rt = Runtime::with_worker_threads(2, Some("remote-log-writer".to_string()))?;
        let (tx, rx) = mpsc::channel(1);
        let tx_cloned = tx.clone();
        let remote_log = RemoteLog {
            sender: tx.clone(),
            cluster_id: labels.get("cluster_id").cloned().unwrap_or_default(),
            node_id: node_id.clone(),
            tenant_id: labels.get("tenant_id").cloned().unwrap_or_default(),
        };
        rt.spawn(async move { Self::interval_flush(interval, tx_cloned).await });
        rt.spawn(async move { RemoteLog::work(rx, &stage_name).await });
        let guard = RemoteLogGuard {
            _rt: rt,
            sender: tx,
        };
        Ok((remote_log, Box::new(guard)))
    }

    pub async fn interval_flush(interval_value: usize, sender: mpsc::Sender<Message>) {
        let mut interval = interval(Duration::from_secs(interval_value as u64));
        loop {
            interval.tick().await;
            if let Err(_e) = sender.send(Message::Flush).await {
                // Will exit if receiver is closed
                break;
            }
        }
    }

    pub async fn work(mut receiver: mpsc::Receiver<Message>, stage_name: &str) {
        let mut buffer = vec![];
        while let Some(msg) = receiver.recv().await {
            match msg {
                Message::LogElement(log_element) => {
                    buffer.push(log_element);
                }
                Message::Shutdown => {
                    if buffer.is_empty() {
                        break;
                    }
                    let flush_buffer = std::mem::take(&mut buffer);

                    RemoteLog::flush(flush_buffer, stage_name).await;
                    break;
                }
                Message::Flush => {
                    if buffer.is_empty() {
                        continue;
                    }
                    let flush_buffer = std::mem::take(&mut buffer);
                    RemoteLog::flush(flush_buffer, stage_name).await;
                }
            }
        }
        // receiver close will trigger interval_flush exit
        receiver.close()
    }

    pub async fn flush(flush_buffer: Vec<RemoteLogElement>, stage_name: &str) {
        if flush_buffer.is_empty() {
            return;
        }
        let op = GlobalLogger::instance().get_operator().await;
        if op.is_none() {
            return;
        }
        let path = format!(
            "stage/internal/{}/{}.parquet",
            stage_name,
            uuid::Uuid::new_v4()
        );
        spawn(async move {
            if let Err(e) = Self::do_flush(op.unwrap(), flush_buffer, &path).await {
                eprintln!("Failed to flush logs: {:?}", e);
            }
        });
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

    fn prepare_log_element(&self, record: &Record) -> RemoteLogElement {
        let query_id = ThreadTracker::query_id();
        let mut fields = Map::new();
        fields.insert("message".to_string(), format!("{}", record.args()).into());
        for (k, v) in collect_kvs(record.key_values()) {
            fields.insert(k, v.into());
        }
        let messages = serde_json::to_string(&fields).unwrap_or_default();
        let log_level = record.level().to_string();
        let timestamp = chrono::Local::now().timestamp_micros();
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
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            query_id: query_id.cloned(),
            log_level,
            messages,
        }
    }
}

impl Append for RemoteLog {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        let log_element = self.prepare_log_element(record);
        let _ = self.sender.try_send(Message::LogElement(log_element));
        Ok(())
    }
}

impl Debug for RemoteLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cluster_id: {}, node_id: {}, tenant_id: {})",
            self.cluster_id, self.node_id, self.tenant_id
        )
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
        Field::new("log_level", DataType::Utf8, true),
        Field::new("cluster_id", DataType::Utf8, true),
        Field::new("node_id", DataType::Utf8, true),
        Field::new("query_id", DataType::Utf8, true),
        Field::new("messages", DataType::Utf8, true),
    ]);

    let mut timestamp = TimestampMicrosecondBuilder::with_capacity(records.len());
    let mut path = StringBuilder::new();
    let mut log_level = StringBuilder::new();
    let mut cluster_id = StringBuilder::new();
    let mut node_id = StringBuilder::new();
    let mut query_id = StringBuilder::new();
    let mut messages = StringBuilder::new();

    for record in records {
        timestamp.append_value(record.timestamp);
        path.append_value(&record.path);
        log_level.append_value(&record.log_level);
        cluster_id.append_value(&record.cluster_id);
        node_id.append_value(&record.node_id);
        query_id.append_option(record.query_id);
        messages.append_value(&record.messages);
    }

    RecordBatch::try_new(Arc::new(schema), vec![
        Arc::new(timestamp.finish()),
        Arc::new(path.finish()),
        Arc::new(log_level.finish()),
        Arc::new(cluster_id.finish()),
        Arc::new(node_id.finish()),
        Arc::new(query_id.finish()),
        Arc::new(messages.finish()),
    ])
    .map_err(|e| {
        databend_common_exception::ErrorCode::Internal(format!(
            "Failed to create record batch for remote log: {}",
            e
        ))
    })
}
