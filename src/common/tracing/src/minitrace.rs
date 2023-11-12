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
use std::fmt;
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::time::Duration;
use std::time::SystemTime;

use common_base::base::tokio;
use common_base::base::GlobalInstance;
use fern::FormatCallback;
use log::LevelFilter;
use log::Log;
use minitrace::prelude::*;
use serde_json::Map;
use tracing_appender::non_blocking::NonBlocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;

use crate::Config;

const HEADER_TRACE_PARENT: &str = "traceparent";

#[allow(dyn_drop)]
pub struct GlobalLogger {
    _guards: Vec<Box<dyn Drop + Send + Sync + 'static>>,
}

impl GlobalLogger {
    pub fn init(name: &str, cfg: &Config) {
        let _guards = init_logging(name, cfg);
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
pub fn init_logging(name: &str, cfg: &Config) -> Vec<Box<dyn Drop + Send + Sync + 'static>> {
    let mut guards: Vec<Box<dyn Drop + Send + Sync + 'static>> = Vec::new();

    // Initialize tracing reporter
    if cfg.tracing.on {
        let name = name.to_string();
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
                        opentelemetry::KeyValue::new("service.name", name.clone()),
                    ])),
                    opentelemetry::InstrumentationLibrary::new(
                        name,
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

    // Console logger
    if cfg.stderr.on {
        normal_logger = normal_logger.chain(
            fern::Dispatch::new()
                .level(cfg.stderr.level.parse().unwrap_or(LevelFilter::Info))
                .format(formatter(&cfg.stderr.format))
                .chain(std::io::stderr()),
        )
    }

    // File logger
    if cfg.file.on {
        let (normal_log_file, flush_guard) = new_file_log_writer(&cfg.file.dir, name);

        guards.push(Box::new(flush_guard));

        normal_logger = normal_logger.chain(
            fern::Dispatch::new()
                .level(cfg.file.level.parse().unwrap_or(LevelFilter::Info))
                .format(formatter(&cfg.file.format))
                .chain(Box::new(normal_log_file) as Box<dyn Write + Send>),
        );
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
        let (query_log_file, flush_guard) = new_file_log_writer(&cfg.query.dir, name);

        guards.push(Box::new(flush_guard));

        query_logger = query_logger.chain(Box::new(query_log_file) as Box<dyn Write + Send>);
    }

    let logger = fern::Dispatch::new()
        .chain(
            fern::Dispatch::new()
                .level_for("query", LevelFilter::Off)
                .chain(normal_logger),
        )
        .chain(
            fern::Dispatch::new()
                .level(LevelFilter::Off)
                .level_for("query", LevelFilter::Info)
                .chain(query_logger),
        );

    // Set global logger
    if logger.apply().is_err() {
        eprint!("logger has already been set");
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

struct MinitraceLogger;

impl Log for MinitraceLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if record.key_values().count() == 0 {
            minitrace::Event::add_to_local_parent(record.level().as_str(), || {
                [("message".into(), format!("{}", record.args()).into())]
            });
        } else {
            minitrace::Event::add_to_local_parent(record.level().as_str(), || {
                let mut pairs = Vec::with_capacity(record.key_values().count() + 1);
                pairs.push(("message".into(), format!("{}", record.args()).into()));
                let mut visitor = KvCollector { fields: &mut pairs };
                record.key_values().visit(&mut visitor).ok();
                pairs
            });
        }

        struct KvCollector<'a> {
            fields: &'a mut Vec<(Cow<'static, str>, Cow<'static, str>)>,
        }

        impl<'a, 'kvs> log::kv::Visitor<'kvs> for KvCollector<'a> {
            fn visit_pair(
                &mut self,
                key: log::kv::Key<'kvs>,
                value: log::kv::Value<'kvs>,
            ) -> Result<(), log::kv::Error> {
                self.fields
                    .push((key.as_str().to_string().into(), value.to_string().into()));
                Ok(())
            }
        }
    }

    fn flush(&self) {}
}

/// Create a `BufWriter<NonBlocking>` for a rolling file logger.
///
/// `BufWriter` collects log segments into a whole before sending to underlying writer.
/// `NonBlocking` sends log to another thread to execute the write IO to avoid blocking the thread
/// that calls `log`.
///
/// Note that `NonBlocking` will discard logs if there are too many `io::Write::write(NonBlocking)`,
/// especially when `fern` sends log segments one by one to the `Writer`.
/// Therefore a `BufWriter` is used to reduce the number of `io::Write::write(NonBlocking)`.
fn new_file_log_writer(dir: &str, name: impl ToString) -> (BufWriter<NonBlocking>, WorkerGuard) {
    let rolling = RollingFileAppender::new(Rotation::HOURLY, dir, name.to_string());
    let (non_blocking, flush_guard) = tracing_appender::non_blocking(rolling);
    let buffered_non_blocking = io::BufWriter::with_capacity(64 * 1024 * 1024, non_blocking);

    (buffered_non_blocking, flush_guard)
}
