// Copyright 2021 Datafuse Labs.
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

use std::env;
use std::io;
use std::sync::Arc;

use common_base::base::Singleton;
use common_exception::Result;
use once_cell::sync::OnceCell;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use sentry_tracing::EventFilter;
use tracing::Level;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_log::LogTracer;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use crate::Config;

/// Init logging and tracing.
///
/// TODO: we need to unify logging, tracing and metrics.
///
/// A local tracing collection(maybe for testing) can be done with a local jaeger server.
/// To report tracing data and view it:
///   docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest
///   DATABEND_JAEGER_AGENT_ENDPOINT=localhost:6831 RUST_LOG=trace cargo test
///   open http://localhost:16686/
///
/// To adjust batch sending delay, use `OTEL_BSP_SCHEDULE_DELAY`:
/// DATABEND_JAEGER_AGENT_ENDPOINT=localhost:6831 RUST_LOG=trace OTEL_BSP_SCHEDULE_DELAY=1 cargo test
// TODO(xp): use DATABEND_JAEGER_AGENT_ENDPOINT to assign jaeger server address.
pub fn init_logging(name: &str, cfg: &Config) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let subscriber = Registry::default();

    // File Layer
    let file_layer = if cfg.file.on {
        let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, &cfg.file.dir, name);
        let (rolling_writer, rolling_writer_guard) =
            tracing_appender::non_blocking(rolling_appender);

        let file_logging_layer = BunyanFormattingLayer::new(name.to_string(), rolling_writer);

        let filter = EnvFilter::new(&cfg.file.level);
        let file = file_logging_layer.with_filter(filter);

        guards.push(rolling_writer_guard);

        Some(file)
    } else {
        None
    };
    let subscriber = subscriber.with(file_layer);

    // Stderr (Console) Layer
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let stderr_layer = if cfg.stderr.on || rust_log.is_ok() {
        // Use env RUST_LOG to initialize log if present.
        // Otherwise, use the specified level.
        let directives = rust_log.unwrap_or_else(|_| cfg.stderr.level.to_string());
        let env_filter = EnvFilter::new(directives);

        let stderr = fmt::layer().with_writer(io::stderr).with_filter(env_filter);

        Some(stderr)
    } else {
        None
    };
    let subscriber = subscriber.with(stderr_layer);

    // Jaeger layer.
    // TODO: we should support config this in the future.
    let mut jaeger_layer = None;
    let jaeger_agent_endpoint =
        env::var("DATABEND_JAEGER_AGENT_ENDPOINT").unwrap_or_else(|_| "".to_string());
    if !jaeger_agent_endpoint.is_empty() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name(name)
            .with_agent_endpoint(jaeger_agent_endpoint)
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)
            .expect("install");

        jaeger_layer = Some(tracing_opentelemetry::layer().with_tracer(tracer));
    }
    let subscriber = subscriber.with(jaeger_layer);

    // Sentry Layer.
    // TODO: we should support config this in the future.
    let mut sentry_layer = None;
    let bend_sentry_env = env::var("DATABEND_SENTRY_DSN").unwrap_or_else(|_| "".to_string());
    if !bend_sentry_env.is_empty() {
        sentry_layer = Some(
            sentry_tracing::layer()
                .event_filter(|metadata| match metadata.level() {
                    &Level::ERROR | &Level::WARN => EventFilter::Event,
                    &Level::INFO | &Level::DEBUG | &Level::TRACE => EventFilter::Breadcrumb,
                })
                .span_filter(|metadata| {
                    matches!(
                        metadata.level(),
                        &Level::ERROR | &Level::WARN | &Level::INFO | &Level::DEBUG
                    )
                }),
        );
    }
    let subscriber = subscriber.with(sentry_layer);

    // For tokio-console
    #[cfg(feature = "console")]
    let subscriber = subscriber.with(console_subscriber::spawn());

    // Enable log compatible layer to convert log record to tracing span.
    // We will ignore any errors that returned by this fucntions.
    let _ = LogTracer::init();

    // Ignore errors returned by set_global_default.
    let _ = tracing::subscriber::set_global_default(subscriber);

    guards
}

pub fn init_query_logger(
    log_name: &str,
    dir: &str,
) -> (Vec<WorkerGuard>, Arc<dyn Subscriber + Send + Sync>) {
    let mut guards = vec![];

    let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, dir, log_name);
    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let format = tracing_subscriber::fmt::format()
        .without_time()
        .with_target(false)
        .with_level(false)
        .compact();
    guards.push(rolling_writer_guard);

    let subscriber = tracing_subscriber::fmt()
        .with_writer(rolling_writer)
        .event_format(format)
        .finish();

    (guards, Arc::new(subscriber))
}

pub struct QueryLogger {
    subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,

    /// log_guard preserve the nonblocking logger's guards so that our logger
    /// can flushes spans/events on a drop
    ///
    /// This field should never be used except in `drop`.
    _log_guards: Vec<WorkerGuard>,
}

static QUERY_LOGGER: OnceCell<Singleton<Arc<QueryLogger>>> = OnceCell::new();

impl QueryLogger {
    pub fn init(
        app_name_shuffle: String,
        config: &Config,
        v: Singleton<Arc<QueryLogger>>,
    ) -> Result<()> {
        let app_name = format!("databend-query-{}", app_name_shuffle);
        let mut _log_guards = init_logging(app_name.as_str(), config);
        let query_detail_dir = format!("{}/query-detail", config.file.dir);

        v.init(match config.file.on {
            true => {
                let (_guards, subscriber) = init_query_logger(&app_name_shuffle, &query_detail_dir);
                _log_guards.extend(_guards);

                Arc::new(QueryLogger {
                    _log_guards,
                    subscriber: Some(subscriber),
                })
            }
            false => Arc::new(QueryLogger {
                subscriber: None,
                _log_guards: vec![],
            }),
        })?;

        QUERY_LOGGER.set(v).ok();
        Ok(())
    }

    pub fn instance() -> Arc<QueryLogger> {
        match QUERY_LOGGER.get() {
            None => panic!("QueryLogger is not init"),
            Some(query_logger) => query_logger.get(),
        }
    }

    pub fn get_subscriber(&self) -> Option<Arc<dyn Subscriber + Send + Sync>> {
        self.subscriber.clone()
    }
}
