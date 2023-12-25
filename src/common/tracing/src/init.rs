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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_base::base::tokio;
use databend_common_base::base::GlobalInstance;
use fern::FormatCallback;
use log::LevelFilter;
use log::Log;
use minitrace::prelude::*;
use serde_json::Map;

use crate::loggers::new_file_log_writer;
use crate::loggers::MinitraceLogger;
use crate::loggers::OpenTelemetryLogger;
use crate::Config;

const HEADER_TRACE_PARENT: &str = "traceparent";

#[allow(dyn_drop)]
pub struct GlobalLogger {
    _guards: Vec<Box<dyn Drop + Send + Sync + 'static>>,
}

impl GlobalLogger {
    pub fn init(name: &str, cfg: &Config, labels: BTreeMap<String, String>) {
        let _guards = init_logging(name, cfg, labels);
        GlobalInstance::set(Self { _guards });
    }
}

pub fn start_trace_for_remote_request<T>(name: &'static str, request: &tonic::Request<T>) -> Span {
    let span_context = try {
        let traceparent = request.metadata().get(HEADER_TRACE_PARENT)?.to_str().ok()?;
        SpanContext::decode_w3c_traceparent(traceparent)?
    };
    if let Some(span_context) = span_context {
        Span::root(name, span_context)
    } else {
        Span::noop()
    }
}

pub fn inject_span_to_tonic_request<T>(msg: impl tonic::IntoRequest<T>) -> tonic::Request<T> {
    let mut request = msg.into_request();
    if let Some(current) = SpanContext::current_local_parent() {
        let key = tonic::metadata::MetadataKey::from_bytes(HEADER_TRACE_PARENT.as_bytes()).unwrap();
        let val = tonic::metadata::AsciiMetadataValue::try_from(&current.encode_w3c_traceparent())
            .unwrap();
        request.metadata_mut().insert(key, val);
    }
    request
}

#[allow(dyn_drop)]
pub fn init_logging(
    name: &str,
    cfg: &Config,
    mut labels: BTreeMap<String, String>,
) -> Vec<Box<dyn Drop + Send + Sync + 'static>> {
    let mut guards: Vec<Box<dyn Drop + Send + Sync + 'static>> = Vec::new();
    let log_name = name;
    let trace_name = match labels.get("node_id") {
        None => name.to_string(),
        Some(node_id) => format!(
            "{}@{}",
            name,
            if node_id.len() >= 7 {
                &node_id[0..7]
            } else {
                &node_id
            }
        ),
    };
    // use name as service name if not specified
    if !labels.contains_key("service") {
        labels.insert("service".to_string(), trace_name.to_string());
    }

    // Initialize tracing reporter
    if cfg.tracing.on {
        let otlp_endpoint = cfg.tracing.otlp_endpoint.clone();

        let (reporter_rt, otlp_reporter) = std::thread::spawn(|| {
            // Init runtime with 2 threads.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let reporter = rt.block_on(async {
                minitrace_opentelemetry::OpenTelemetryReporter::new(
                    opentelemetry_otlp::SpanExporter::new_tonic(
                        opentelemetry_otlp::ExportConfig {
                            endpoint: otlp_endpoint,
                            protocol: opentelemetry_otlp::Protocol::Grpc,
                            timeout: Duration::from_secs(
                                opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                            ),
                        },
                        opentelemetry_otlp::TonicConfig::default(),
                    )
                    .expect("initialize otlp exporter"),
                    opentelemetry::trace::SpanKind::Server,
                    Cow::Owned(opentelemetry::sdk::Resource::new([
                        opentelemetry::KeyValue::new("service.name", trace_name.clone()),
                    ])),
                    opentelemetry::InstrumentationLibrary::new(
                        trace_name,
                        None::<&'static str>,
                        None::<&'static str>,
                        None,
                    ),
                )
            });
            (rt, reporter)
        })
        .join()
        .unwrap();

        minitrace::set_reporter(otlp_reporter, minitrace::collector::Config::default());

        guards.push(Box::new(defer::defer(minitrace::flush)));
        guards.push(Box::new(defer::defer(|| {
            std::thread::spawn(move || std::mem::drop(reporter_rt))
                .join()
                .unwrap()
        })));
    }

    // Initialize logging
    let mut normal_logger = fern::Dispatch::new();
    let mut query_logger = fern::Dispatch::new();
    let mut profile_logger = fern::Dispatch::new();

    // File logger
    if cfg.file.on {
        let (normal_log_file, flush_guard) =
            new_file_log_writer(&cfg.file.dir, log_name, cfg.file.limit);
        guards.push(Box::new(flush_guard));
        let dispatch = fern::Dispatch::new()
            .level(cfg.file.level.parse().unwrap_or(LevelFilter::Info))
            .format(formatter(&cfg.file.format))
            .chain(Box::new(normal_log_file) as Box<dyn Write + Send>);
        normal_logger = normal_logger.chain(dispatch);
    }

    // Console logger
    if cfg.stderr.on {
        let dispatch = fern::Dispatch::new()
            .level(cfg.stderr.level.parse().unwrap_or(LevelFilter::Info))
            .format(formatter(&cfg.stderr.format))
            .chain(std::io::stderr());
        normal_logger = normal_logger.chain(dispatch)
    }

    // OpenTelemetry logger
    if cfg.otlp.on {
        let mut labels = labels.clone();
        labels.insert("category".to_string(), "system".to_string());
        labels.extend(cfg.otlp.labels.clone());
        let logger = OpenTelemetryLogger::new(log_name, &cfg.otlp.endpoint, labels);
        let dispatch = fern::Dispatch::new()
            .level(cfg.otlp.level.parse().unwrap_or(LevelFilter::Info))
            .format(formatter("json"))
            .chain(Box::new(logger) as Box<dyn Log>);
        normal_logger = normal_logger.chain(dispatch);
    }

    // Log to minitrace
    if cfg.tracing.on {
        let level = cfg
            .tracing
            .capture_log_level
            .parse()
            .ok()
            .unwrap_or(LevelFilter::Info);
        normal_logger = normal_logger.chain(
            fern::Dispatch::new()
                .level(level)
                .chain(Box::new(MinitraceLogger) as Box<dyn Log>),
        );
    }

    // Query logger
    if cfg.query.on {
        if !cfg.query.dir.is_empty() {
            let (query_log_file, flush_guard) =
                new_file_log_writer(&cfg.query.dir, log_name, cfg.file.limit);
            guards.push(Box::new(flush_guard));
            query_logger = query_logger.chain(Box::new(query_log_file) as Box<dyn Write + Send>);
        }
        if !cfg.query.otlp_endpoint.is_empty() {
            let mut labels = labels.clone();
            labels.insert("category".to_string(), "query".to_string());
            labels.extend(cfg.query.labels.clone());
            let logger = OpenTelemetryLogger::new(log_name, &cfg.query.otlp_endpoint, labels);
            query_logger = query_logger.chain(Box::new(logger) as Box<dyn Log>);
        }
    }

    // Profile logger
    if cfg.profile.on {
        if !cfg.profile.dir.is_empty() {
            let (profile_log_file, flush_guard) =
                new_file_log_writer(&cfg.profile.dir, log_name, cfg.file.limit);
            guards.push(Box::new(flush_guard));
            profile_logger =
                profile_logger.chain(Box::new(profile_log_file) as Box<dyn Write + Send>);
        }
        if !cfg.profile.otlp_endpoint.is_empty() {
            let mut labels = labels.clone();
            labels.insert("category".to_string(), "profile".to_string());
            labels.extend(cfg.profile.labels.clone());
            let logger = OpenTelemetryLogger::new(log_name, &cfg.profile.otlp_endpoint, labels);
            profile_logger = profile_logger.chain(Box::new(logger) as Box<dyn Log>);
        }
    }

    let logger = fern::Dispatch::new()
        .chain(
            fern::Dispatch::new()
                .level_for("databend::log::query", LevelFilter::Off)
                .level_for("databend::log::profile", LevelFilter::Off)
                .filter(|meta| {
                    if meta.target().starts_with("databend_") {
                        true
                    } else {
                        meta.level() <= LevelFilter::Error
                    }
                })
                .chain(normal_logger),
        )
        .chain(
            fern::Dispatch::new()
                .level(LevelFilter::Off)
                .level_for("databend::log::query", LevelFilter::Info)
                .chain(query_logger),
        )
        .chain(
            fern::Dispatch::new()
                .level(LevelFilter::Off)
                .level_for("databend::log::profile", LevelFilter::Info)
                .chain(profile_logger),
        );

    // Set global logger
    if logger.apply().is_err() {
        eprintln!("logger has already been set");
        return Vec::new();
    }

    #[cfg(feature = "console")]
    init_tokio_console();

    guards
}

#[cfg(feature = "console")]
fn init_tokio_console() {
    use tracing_subscriber::prelude::*;

    let subscriber = tracing_subscriber::registry::Registry::default();
    let subscriber = subscriber.with(console_subscriber::spawn());
    tracing::subscriber::set_global_default(subscriber).ok();
}

fn formatter(
    format: &str,
) -> fn(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    match format {
        "text" => format_text_log,
        "json" => format_json_log,
        _ => unreachable!("file logging format {format} is not supported"),
    }
}

fn format_text_log(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    out.finish(format_args!(
        "{} {:>5} {}: {}:{} {}{}",
        humantime::format_rfc3339_micros(SystemTime::now()),
        record.level(),
        record.module_path().unwrap_or(""),
        record.file().unwrap_or(""),
        record.line().unwrap_or(0),
        message,
        KvDisplay {
            kv: record.key_values()
        }
    ));

    struct KvDisplay<'kvs> {
        kv: &'kvs dyn log::kv::Source,
    }

    impl fmt::Display for KvDisplay<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
}

fn format_json_log(out: FormatCallback, message: &fmt::Arguments, record: &log::Record) {
    let mut fields = Map::new();
    fields.insert("message".to_string(), format!("{}", message).into());
    let mut visitor = KvCollector {
        fields: &mut fields,
    };
    record.key_values().visit(&mut visitor).ok();

    out.finish(format_args!(
        r#"{{"timestamp":"{}","level":"{}","fields":{}}}"#,
        humantime::format_rfc3339_micros(SystemTime::now()),
        record.level(),
        serde_json::to_string(&fields).unwrap_or_default(),
    ));

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
