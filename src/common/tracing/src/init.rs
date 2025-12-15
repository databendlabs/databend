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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_base::base::GlobalInstance;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::runtime::Thread;
use fastrace::prelude::*;
use log::LevelFilter;
use logforth::filter::EnvFilter;
use logforth::filter::env_filter::EnvFilterBuilder;
use opendal::Operator;
use opentelemetry_otlp::Compression;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_otlp::WithTonicConfig;

use crate::Config;
use crate::config::LogFormat;
use crate::config::OTLPProtocol;
use crate::filter::ThreadTrackerFilter;
use crate::loggers::IdenticalLayout;
use crate::loggers::JsonLayout;
use crate::loggers::TextLayout;
use crate::loggers::new_rolling_file_appender;
use crate::predefined_tables::table_to_target;
use crate::query_log_collector::QueryLogCollector;
use crate::remote_log::RemoteLog;
use crate::structlog::StructLogReporter;

const HEADER_TRACE_PARENT: &str = "traceparent";

pub struct GlobalLogger {
    _drop_guards: Vec<Box<dyn Send + Sync + 'static>>,
    pub remote_log_operator: RwLock<Option<Operator>>,
    pub ready: AtomicBool,
}

impl GlobalLogger {
    pub fn init(name: &str, cfg: &Config, labels: BTreeMap<String, String>) {
        let _drop_guards = init_logging(name, cfg, labels);

        // GlobalLogger is initialized before DataOperator, so set the operator to None first
        let remote_log_operator = RwLock::new(None);

        let instance = Arc::new(Self {
            _drop_guards,
            remote_log_operator,
            ready: AtomicBool::new(false),
        });
        GlobalInstance::set(instance);
    }

    pub fn dummy() -> Arc<GlobalLogger> {
        Arc::new(Self {
            _drop_guards: Vec::new(),
            remote_log_operator: RwLock::new(None),
            ready: AtomicBool::new(true),
        })
    }

    pub fn instance() -> Arc<GlobalLogger> {
        GlobalInstance::get()
    }

    // Get the operator for remote log when it is ready.
    pub async fn get_operator(&self) -> Option<Operator> {
        let operator = self.remote_log_operator.read().await;
        if let Some(operator) = operator.as_ref() {
            return Some(operator.clone());
        }
        None
    }

    // Set the operator for remote log, this should be only called once
    pub async fn set_operator(&self, operator: Operator) {
        let mut remote_log_operator = self.remote_log_operator.write().await;
        *remote_log_operator = Some(operator);
        self.ready.store(true, Ordering::SeqCst);
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

fn env_filter(level: &str) -> EnvFilter {
    EnvFilter::new(
        EnvFilterBuilder::new()
            .filter(Some("databend::log::query"), LevelFilter::Off)
            .filter(Some("databend::log::query::file"), LevelFilter::Off)
            .filter(Some("databend::log::profile"), LevelFilter::Off)
            .filter(Some("databend::log::structlog"), LevelFilter::Off)
            .filter(Some("databend::log::time_series"), LevelFilter::Off)
            .filter(Some("databend::log::access"), LevelFilter::Off)
            .parse(level),
    )
}

pub fn init_logging(
    log_name: &str,
    cfg: &Config,
    mut labels: BTreeMap<String, String>,
) -> Vec<Box<dyn Send + Sync + 'static>> {
    let mut _drop_guards: Vec<Box<dyn Send + Sync + 'static>> = Vec::new();
    if !labels.contains_key("service") {
        labels.insert("service".to_string(), log_name.to_string());
    }
    let trace_name = match labels.get("node_id") {
        None => log_name.to_string(),
        Some(node_id) => format!(
            "{}@{}",
            log_name,
            if node_id.len() >= 7 {
                &node_id[0..7]
            } else {
                &node_id
            }
        ),
    };
    // initialize tracing a reporter
    if cfg.tracing.on {
        let endpoint = cfg.tracing.otlp.endpoint.clone();
        let exporter = match cfg.tracing.otlp.protocol {
            OTLPProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_compression(Compression::Gzip)
                .with_endpoint(endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_timeout(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
                .build()
                .expect("initialize oltp grpc exporter"),
            OTLPProtocol::Http => opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_timeout(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
                .build()
                .expect("initialize oltp http exporter"),
        };

        let resource = opentelemetry_sdk::Resource::builder()
            .with_service_name(trace_name.clone())
            .with_attributes(
                cfg.tracing
                    .otlp
                    .labels
                    .iter()
                    .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string())),
            )
            .with_attributes(
                labels
                    .iter()
                    .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string())),
            )
            .build();
        let (reporter_rt, otlp_reporter) = Thread::spawn(move || {
            // init runtime with 2 threads
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let reporter = rt.block_on(async {
                fastrace_opentelemetry::OpenTelemetryReporter::new(
                    exporter,
                    opentelemetry::trace::SpanKind::Server,
                    Cow::Owned(resource),
                    opentelemetry::InstrumentationScope::builder(trace_name).build(),
                )
            });
            (rt, reporter)
        })
        .join()
        .unwrap();

        let trace_config = fastrace::collector::Config::default();
        if cfg.structlog.on {
            let reporter = StructLogReporter::wrap(otlp_reporter);
            fastrace::set_reporter(reporter, trace_config);
        } else {
            fastrace::set_reporter(otlp_reporter, trace_config);
        }

        _drop_guards.push(Box::new(defer::defer(fastrace::flush)));
        _drop_guards.push(Box::new(defer::defer(|| {
            Thread::spawn(move || std::mem::drop(reporter_rt))
                .join()
                .unwrap()
        })));
    } else if cfg.structlog.on {
        let reporter = StructLogReporter::new();
        fastrace::set_reporter(reporter, fastrace::collector::Config::default());
        _drop_guards.push(Box::new(defer::defer(fastrace::flush)));
    }

    // initialize logging
    let mut logger = logforth::builder();

    // file logger
    if cfg.file.on {
        let (normal_log_file, flush_guard) =
            new_rolling_file_appender(&cfg.file.dir, log_name, cfg.file.limit, cfg.file.max_size);
        _drop_guards.push(flush_guard);

        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(env_filter(&cfg.file.level))
                .filter(ThreadTrackerFilter)
                .append(match cfg.file.format {
                    LogFormat::Text => normal_log_file.with_layout(TextLayout::<false>),
                    LogFormat::Json => normal_log_file.with_layout(JsonLayout::<false>),
                    LogFormat::Identical => normal_log_file.with_layout(IdenticalLayout),
                })
        });
    }

    // console logger
    if cfg.stderr.on {
        logger = logger.dispatch(|dispatch| {
            let stderr = logforth::append::Stderr::default();
            dispatch
                .filter(env_filter(&cfg.stderr.level))
                .filter(ThreadTrackerFilter)
                .append(match cfg.stderr.format {
                    LogFormat::Text => stderr.with_layout(TextLayout::<false>),
                    LogFormat::Json => stderr.with_layout(JsonLayout::<false>),
                    LogFormat::Identical => stderr.with_layout(IdenticalLayout),
                })
        });
    }

    // opentelemetry logger
    if cfg.otlp.on {
        let labels = labels
            .iter()
            .chain(&cfg.otlp.endpoint.labels)
            .map(|(k, v)| (Cow::from(k.clone()), Cow::from(v.clone())))
            .chain([(Cow::from("category"), Cow::from("system"))]);
        let mut otel_builder = logforth::append::opentelemetry::OpentelemetryLogBuilder::new(
            log_name,
            format!("{}/v1/logs", &cfg.otlp.endpoint.endpoint),
        )
        .protocol(cfg.otlp.endpoint.protocol.into());
        for (k, v) in labels {
            otel_builder = otel_builder.label(k, v);
        }
        let otel = otel_builder
            .build()
            .expect("initialize opentelemetry logger");
        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(env_filter(&cfg.otlp.level))
                .filter(ThreadTrackerFilter)
                .append(otel)
        });
    }

    // log to fastrace
    if cfg.tracing.on || cfg.structlog.on {
        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(env_filter(&cfg.tracing.capture_log_level))
                .filter(ThreadTrackerFilter)
                .append(logforth::append::FastraceEvent::default())
        });
    }

    // query logger
    if cfg.query.on {
        if !cfg.query.dir.is_empty() {
            let (query_log_file, flush_guard) = new_rolling_file_appender(
                &cfg.query.dir,
                log_name,
                cfg.file.limit,
                cfg.file.max_size,
            );
            _drop_guards.push(flush_guard);

            logger = logger.dispatch(|dispatch| {
                dispatch
                    .filter(EnvFilter::new(
                        EnvFilterBuilder::new()
                            .filter(Some("databend::log::query::file"), LevelFilter::Trace),
                    ))
                    .filter(ThreadTrackerFilter)
                    .append(query_log_file.with_layout(IdenticalLayout))
            });
        }
        if let Some(endpoint) = &cfg.query.otlp {
            let labels = labels
                .iter()
                .chain(&endpoint.labels)
                .map(|(k, v)| (Cow::from(k.clone()), Cow::from(v.clone())))
                .chain([(Cow::from("category"), Cow::from("query"))]);
            let mut otel_builder = logforth::append::opentelemetry::OpentelemetryLogBuilder::new(
                log_name,
                format!("{}/v1/logs", &endpoint.endpoint),
            )
            .protocol(endpoint.protocol.into());
            for (k, v) in labels {
                otel_builder = otel_builder.label(k, v);
            }
            let otel = otel_builder
                .build()
                .expect("initialize opentelemetry logger");
            logger = logger.dispatch(|dispatch| {
                dispatch
                    .filter(EnvFilter::new(
                        EnvFilterBuilder::new()
                            .filter(Some("databend::log::query::file"), LevelFilter::Trace),
                    ))
                    .filter(ThreadTrackerFilter)
                    .append(otel)
            });
        }
    }

    // profile logger
    if cfg.profile.on {
        if !cfg.profile.dir.is_empty() {
            let (profile_log_file, flush_guard) = new_rolling_file_appender(
                &cfg.profile.dir,
                log_name,
                cfg.file.limit,
                cfg.file.max_size,
            );
            _drop_guards.push(flush_guard);

            logger = logger.dispatch(|dispatch| {
                dispatch
                    .filter(EnvFilter::new(
                        EnvFilterBuilder::new()
                            .filter(Some("databend::log::profile"), LevelFilter::Trace),
                    ))
                    .filter(ThreadTrackerFilter)
                    .append(profile_log_file.with_layout(IdenticalLayout))
            });
        }
        if let Some(endpoint) = &cfg.profile.otlp {
            let labels = labels
                .iter()
                .chain(&endpoint.labels)
                .map(|(k, v)| (Cow::from(k.clone()), Cow::from(v.clone())))
                .chain([(Cow::from("category"), Cow::from("profile"))]);
            let otel = logforth::append::opentelemetry::OpentelemetryLogBuilder::new(
                log_name,
                format!("{}/v1/logs", &endpoint.endpoint),
            )
            .protocol(endpoint.protocol.into())
            .labels(labels)
            .build()
            .expect("initialize opentelemetry logger");
            logger = logger.dispatch(|dispatch| {
                dispatch
                    .filter(EnvFilter::new(
                        EnvFilterBuilder::new()
                            .filter(Some("databend::log::profile"), LevelFilter::Trace),
                    ))
                    .filter(ThreadTrackerFilter)
                    .append(otel)
            });
        }
    }

    // structured logger
    if cfg.structlog.on && !cfg.structlog.dir.is_empty() {
        let (structlog_log_file, flush_guard) = new_rolling_file_appender(
            &cfg.structlog.dir,
            log_name,
            cfg.file.limit,
            cfg.file.max_size,
        );
        _drop_guards.push(flush_guard);

        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(EnvFilter::new(
                    EnvFilterBuilder::new()
                        .filter(Some("databend::log::structlog"), LevelFilter::Trace),
                ))
                .filter(ThreadTrackerFilter)
                .append(structlog_log_file)
        });
    }
    if cfg.history.on {
        let (remote_log, flush_guard) =
            RemoteLog::new(&labels, cfg).expect("initialize remote logger");

        let mut filter_builder =
            EnvFilterBuilder::new().filter(Some("databend::log::structlog"), LevelFilter::Off);

        let mut table_to_target = table_to_target();

        for table_cfg in cfg.history.tables.iter() {
            if let Some(target) = table_to_target.remove(&table_cfg.table_name) {
                filter_builder = filter_builder.filter(Some(&target), LevelFilter::Trace);
            }
        }
        for (_, target) in table_to_target {
            filter_builder = filter_builder.filter(Some(&target), LevelFilter::Off);
        }

        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(EnvFilter::new(filter_builder.parse(&cfg.history.level)))
                .filter(ThreadTrackerFilter)
                .append(remote_log)
        });
        _drop_guards.push(flush_guard);
    }

    // Query log collector
    {
        logger = logger.dispatch(|dispatch| {
            dispatch
                .filter(ThreadTrackerFilter)
                .append(QueryLogCollector::new())
        });
    }

    // set global logger
    if logger.try_apply().is_err() {
        eprintln!("logger has already been set");
        return Vec::new();
    }

    _drop_guards
}
