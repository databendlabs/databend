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
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Thread;
use fastrace::prelude::*;
use log::LevelFilter;
use logforth::filter::env::EnvFilterBuilder;
use logforth::filter::EnvFilter;
use logforth::Dispatch;
use logforth::Logger;
use opentelemetry_otlp::WithExportConfig;

use crate::config::OTLPProtocol;
use crate::loggers::get_layout;
use crate::loggers::new_rolling_file_appender;
use crate::structlog::StructLogReporter;
use crate::Config;

const HEADER_TRACE_PARENT: &str = "traceparent";

pub struct GlobalLogger {
    _drop_guards: Vec<Box<dyn Send + Sync + 'static>>,
}

impl GlobalLogger {
    pub fn init(name: &str, cfg: &Config, labels: BTreeMap<String, String>) {
        let _drop_guards = init_logging(name, cfg, labels);
        GlobalInstance::set(Self { _drop_guards });
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
            .filter(Some("databend::log::profile"), LevelFilter::Off)
            .filter(Some("databend::log::structlog"), LevelFilter::Off)
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
        let mut kvs = cfg
            .tracing
            .otlp
            .labels
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect::<Vec<_>>();
        kvs.push(opentelemetry::KeyValue::new(
            "service.name",
            trace_name.clone(),
        ));
        for (k, v) in &labels {
            kvs.push(opentelemetry::KeyValue::new(k.to_string(), v.to_string()));
        }
        let exporter = match cfg.tracing.otlp.protocol {
            OTLPProtocol::Grpc => opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_timeout(Duration::from_secs(
                    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                ))
                .build_span_exporter()
                .expect("initialize oltp grpc exporter"),
            OTLPProtocol::Http => opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_timeout(Duration::from_secs(
                    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                ))
                .build_span_exporter()
                .expect("initialize oltp http exporter"),
        };
        let (reporter_rt, otlp_reporter) = Thread::spawn(|| {
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
                    Cow::Owned(opentelemetry_sdk::Resource::new(kvs)),
                    opentelemetry::InstrumentationLibrary::builder(trace_name).build(),
                )
            });
            (rt, reporter)
        })
        .join()
        .unwrap();

        if cfg.structlog.on {
            let reporter = StructLogReporter::wrap(otlp_reporter);
            fastrace::set_reporter(reporter, fastrace::collector::Config::default());
        } else {
            fastrace::set_reporter(otlp_reporter, fastrace::collector::Config::default());
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
    let mut logger = Logger::new();

    // file logger
    if cfg.file.on {
        let (normal_log_file, flush_guard) =
            new_rolling_file_appender(&cfg.file.dir, log_name, cfg.file.limit, cfg.file.max_size);
        _drop_guards.push(flush_guard);

        let dispatch = Dispatch::new()
            .filter(env_filter(&cfg.file.level))
            .append(normal_log_file.with_layout(get_layout(&cfg.file.format)));
        logger = logger.dispatch(dispatch);
    }

    // console logger
    if cfg.stderr.on {
        let dispatch = Dispatch::new()
            .filter(env_filter(&cfg.stderr.level))
            .append(
                logforth::append::Stderr::default().with_layout(get_layout(&cfg.stderr.format)),
            );
        logger = logger.dispatch(dispatch);
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
        let dispatch = Dispatch::new()
            .filter(env_filter(&cfg.otlp.level))
            .append(otel);
        logger = logger.dispatch(dispatch);
    }

    // log to fastrace
    if cfg.tracing.on || cfg.structlog.on {
        let dispatch = Dispatch::new()
            .filter(env_filter(&cfg.tracing.capture_log_level))
            .append(logforth::append::FastraceEvent::default());
        logger = logger.dispatch(dispatch);
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

            let dispatch = Dispatch::new()
                .filter(EnvFilter::new(
                    EnvFilterBuilder::new()
                        .filter(Some("databend::log::query"), LevelFilter::Trace),
                ))
                .append(query_log_file.with_layout(get_layout("identical")));
            logger = logger.dispatch(dispatch);
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
            let dispatch = Dispatch::new()
                .filter(EnvFilter::new(
                    EnvFilterBuilder::new()
                        .filter(Some("databend::log::query"), LevelFilter::Trace),
                ))
                .append(otel);
            logger = logger.dispatch(dispatch);
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

            let dispatch = Dispatch::new()
                .filter(EnvFilter::new(
                    EnvFilterBuilder::new()
                        .filter(Some("databend::log::profile"), LevelFilter::Trace),
                ))
                .append(profile_log_file.with_layout(get_layout("identical")));
            logger = logger.dispatch(dispatch);
        }
        if let Some(endpoint) = &cfg.profile.otlp {
            let labels = labels
                .iter()
                .chain(&endpoint.labels)
                .map(|(k, v)| (Cow::from(k.clone()), Cow::from(v.clone())))
                .chain([(Cow::from("category"), Cow::from("profile"))]);
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
            let dispatch = Dispatch::new()
                .filter(EnvFilter::new(
                    EnvFilterBuilder::new()
                        .filter(Some("databend::log::profile"), LevelFilter::Trace),
                ))
                .append(otel);
            logger = logger.dispatch(dispatch);
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

        let dispatch = Dispatch::new()
            .filter(EnvFilter::new(
                EnvFilterBuilder::new()
                    .filter(Some("databend::log::structlog"), LevelFilter::Trace),
            ))
            .append(structlog_log_file);
        logger = logger.dispatch(dispatch);
    }

    // set global logger
    if logger.apply().is_err() {
        eprintln!("logger has already been set");
        return Vec::new();
    }

    _drop_guards
}
