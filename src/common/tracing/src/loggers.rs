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
use std::fmt;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime::ThreadTracker;
use fern::FormatCallback;
use opentelemetry::logs::AnyValue;
use opentelemetry::logs::Severity;
use opentelemetry::InstrumentationLibrary;
use opentelemetry_otlp::WithExportConfig;
use serde_json::Map;
use tracing_appender::non_blocking::NonBlocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;

use crate::config::OTLPEndpointConfig;
use crate::config::OTLPProtocol;

/// Create a `BufWriter<NonBlocking>` for a rolling file logger.
///
/// `BufWriter` collects log segments into a whole before sending to underlying writer.
/// `NonBlocking` sends log to another thread to execute the write IO to avoid blocking the thread
/// that calls `log`.
///
/// Note that `NonBlocking` will discard logs if there are too many `io::Write::write(NonBlocking)`,
/// especially when `fern` sends log segments one by one to the `Writer`.
/// Therefore a `BufWriter` is used to reduce the number of `io::Write::write(NonBlocking)`.
pub(crate) fn new_file_log_writer(
    dir: &str,
    name: impl ToString,
    max_files: usize,
) -> (BufWriter<NonBlocking>, WorkerGuard) {
    let rolling = RollingFileAppender::builder()
        .rotation(Rotation::HOURLY)
        .filename_prefix(name.to_string())
        .max_log_files(max_files)
        .build(dir)
        .expect("failed to initialize rolling file appender");
    let (non_blocking, flush_guard) = tracing_appender::non_blocking(rolling);
    let buffered_non_blocking = BufWriter::with_capacity(64 * 1024 * 1024, non_blocking);

    (buffered_non_blocking, flush_guard)
}

pub(crate) struct MinitraceLogger;

impl log::Log for MinitraceLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let mut message = format!(
            "{} {:>5} {}{}",
            chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            record.level(),
            record.args(),
            KvDisplay::new(record.key_values()),
        );
        if message.contains('\n') {
            // Align multi-line log messages with the first line after `level``.
            message = message.replace('\n', "\n                                  ");
        }
        minitrace::Event::add_to_local_parent(message, || []);
    }

    fn flush(&self) {}
}

#[derive(Debug)]
pub(crate) struct OpenTelemetryLogger {
    name: String,
    category: String,
    library: Arc<InstrumentationLibrary>,
    provider: opentelemetry_sdk::logs::LoggerProvider,
}

impl OpenTelemetryLogger {
    pub(crate) fn new(
        name: impl ToString,
        category: impl ToString,
        config: &OTLPEndpointConfig,
        labels: &BTreeMap<String, String>,
    ) -> Self {
        let endpoint = if !config.endpoint.trim_end_matches('/').ends_with("/v1/logs") {
            format!("{}/v1/logs", config.endpoint)
        } else {
            config.endpoint.clone()
        };

        let exporter = match config.protocol {
            OTLPProtocol::Grpc => {
                let export_config = opentelemetry_otlp::ExportConfig {
                    endpoint,
                    protocol: opentelemetry_otlp::Protocol::Grpc,
                    timeout: Duration::from_secs(
                        opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                    ),
                };
                let exporter_builder: opentelemetry_otlp::LogExporterBuilder =
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_export_config(export_config)
                        .into();
                exporter_builder
                    .build_log_exporter()
                    .expect("build grpc log exporter")
            }
            OTLPProtocol::Http => {
                let export_config = opentelemetry_otlp::ExportConfig {
                    endpoint,
                    protocol: opentelemetry_otlp::Protocol::HttpBinary,
                    timeout: Duration::from_secs(
                        opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                    ),
                };
                let exporter_builder: opentelemetry_otlp::LogExporterBuilder =
                    opentelemetry_otlp::new_exporter()
                        .http()
                        .with_export_config(export_config)
                        .into();
                exporter_builder
                    .build_log_exporter()
                    .expect("build http log exporter")
            }
        };
        let mut kvs = config
            .labels
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect::<Vec<_>>();
        kvs.push(opentelemetry::KeyValue::new(
            "category",
            category.to_string(),
        ));
        for (k, v) in labels {
            kvs.push(opentelemetry::KeyValue::new(k.to_string(), v.to_string()));
        }
        let provider = opentelemetry_sdk::logs::LoggerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_config(
                opentelemetry_sdk::logs::Config::default()
                    .with_resource(opentelemetry_sdk::Resource::new(kvs)),
            )
            .build();
        let library = Arc::new(InstrumentationLibrary::builder(name.to_string()).build());
        Self {
            name: name.to_string(),
            category: category.to_string(),
            library,
            provider,
        }
    }

    pub fn instrumentation_library(&self) -> &InstrumentationLibrary {
        &self.library
    }
}

impl log::Log for OpenTelemetryLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        // we handle level and target filter with fern
        true
    }

    fn log(&self, log_record: &log::Record<'_>) {
        let provider = self.provider.clone();
        let mut record = opentelemetry_sdk::logs::LogRecord::default();
        record.observed_timestamp = Some(chrono::Utc::now().into());
        record.severity_number = Some(map_severity_to_otel_severity(log_record.level()));
        record.severity_text = Some(log_record.level().as_str().into());
        record.body = Some(AnyValue::from(log_record.args().to_string()));

        for processor in provider.log_processors() {
            let record = record.clone();
            let data = opentelemetry_sdk::export::logs::LogData {
                record,
                instrumentation: self.instrumentation_library().clone(),
            };
            processor.emit(data);
        }
    }

    fn flush(&self) {
        let result = self.provider.force_flush();
        for r in result {
            if let Err(e) = r {
                eprintln!("flush logger {}:{} failed: {}", self.name, self.category, e);
            }
        }
    }
}

pub fn formatter(
    format: &str,
) -> fn(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    match format {
        "text" => format_text_log,
        "json" => format_json_log,
        _ => unreachable!("file logging format {format} is not supported"),
    }
}

fn format_json_log(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    let mut fields = Map::new();
    fields.insert("message".to_string(), format!("{}", message).into());
    let mut visitor = KvCollector {
        fields: &mut fields,
    };
    record.key_values().visit(&mut visitor).ok();

    match ThreadTracker::query_id() {
        None => {
            out.finish(format_args!(
                r#"{{"timestamp":"{}","level":"{}","fields":{}}}"#,
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                serde_json::to_string(&fields).unwrap_or_default(),
            ));
        }
        Some(query_id) => {
            out.finish(format_args!(
                r#"{{"timestamp":"{}","level":"{}","query_id":"{}","fields":{}}}"#,
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                query_id,
                serde_json::to_string(&fields).unwrap_or_default(),
            ));
        }
    }

    struct KvCollector<'a> {
        fields: &'a mut Map<String, serde_json::Value>,
    }

    impl<'a, 'kvs> log::kv::Visitor<'kvs> for KvCollector<'a> {
        fn visit_pair(
            &mut self,
            key: log::kv::Key<'kvs>,
            value: log::kv::Value<'kvs>,
        ) -> Result<(), log::kv::Error> {
            self.fields
                .insert(key.as_str().to_string(), value.to_string().into());
            Ok(())
        }
    }
}

fn format_text_log(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    match ThreadTracker::query_id() {
        None => {
            out.finish(format_args!(
                "{} {:>5} {}: {}:{} {}{}",
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                record.module_path().unwrap_or(""),
                Path::new(record.file().unwrap_or_default())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                record.line().unwrap_or(0),
                message,
                KvDisplay::new(record.key_values()),
            ));
        }
        Some(query_id) => {
            out.finish(format_args!(
                "{} {} {:>5} {}: {}:{} {}{}",
                query_id,
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                record.module_path().unwrap_or(""),
                Path::new(record.file().unwrap_or_default())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                record.line().unwrap_or(0),
                message,
                KvDisplay::new(record.key_values()),
            ));
        }
    }
}

pub struct KvDisplay<'kvs> {
    kv: &'kvs dyn log::kv::Source,
}

impl<'kvs> KvDisplay<'kvs> {
    pub fn new(kv: &'kvs dyn log::kv::Source) -> Self {
        Self { kv }
    }
}

impl fmt::Display for KvDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut visitor = KvWriter { writer: f };
        self.kv.visit(&mut visitor).ok();
        Ok(())
    }
}

struct KvWriter<'a, 'kvs> {
    writer: &'kvs mut fmt::Formatter<'a>,
}

impl<'a, 'kvs> log::kv::Visitor<'kvs> for KvWriter<'a, 'kvs> {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        write!(self.writer, " {key}={value}")?;
        Ok(())
    }
}

fn map_severity_to_otel_severity(level: log::Level) -> Severity {
    match level {
        log::Level::Error => Severity::Error,
        log::Level::Warn => Severity::Warn,
        log::Level::Info => Severity::Info,
        log::Level::Debug => Severity::Debug,
        log::Level::Trace => Severity::Trace,
    }
}
