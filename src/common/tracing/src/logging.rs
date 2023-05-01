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

use std::env;
use std::io;
use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_exception::Result;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use sentry_tracing::EventFilter;
use tracing::Event;
use tracing::Level;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_log::LogTracer;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
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
/// TODO(xp): use DATABEND_JAEGER_AGENT_ENDPOINT to assign jaeger server address.
///
/// # Notes to implementation
///
/// Registry is composed by generic type parameters.
///
/// To make rust happy, we need to push `Option<Layer>` into it.
pub fn init_logging(name: &str, cfg: &Config) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let subscriber = Registry::default();

    // File Layer
    let subscriber = if cfg.file.on {
        let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, &cfg.file.dir, name);
        let (rolling_writer, rolling_writer_guard) =
            tracing_appender::non_blocking(rolling_appender);
        guards.push(rolling_writer_guard);

        let filter = EnvFilter::new(&cfg.file.level);

        match cfg.file.format.to_lowercase().as_str() {
            "text" => {
                let layer = fmt::layer()
                    .with_writer(rolling_writer)
                    .event_format(
                        format()
                            .with_ansi(false)
                            .with_file(true)
                            .with_line_number(true),
                    )
                    .with_filter(filter);
                subscriber.with(Some(layer)).with(None)
            }
            "json" => {
                let layer = fmt::layer()
                    .with_writer(rolling_writer)
                    .fmt_fields(fmt::format::JsonFields::default())
                    .event_format(format().json())
                    .with_filter(filter);
                subscriber.with(None).with(Some(layer))
            }
            v => {
                unreachable!("file logging format {v} is not supported");
            }
        }
    } else {
        subscriber.with(None).with(None)
    };

    // Stderr (Console) Layer
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let subscriber = if cfg.stderr.on || rust_log.is_ok() {
        // Use env RUST_LOG to initialize log if present.
        // Otherwise, use the specified level.
        let directives = rust_log.unwrap_or_else(|_| cfg.stderr.level.to_string());
        let env_filter = EnvFilter::new(directives);

        match cfg.stderr.format.to_lowercase().as_str() {
            "text" => {
                let layer = fmt::layer()
                    .with_writer(io::stderr)
                    .event_format(format().pretty())
                    .with_filter(env_filter);
                subscriber.with(Some(layer)).with(None)
            }
            "json" => {
                let layer = fmt::layer()
                    .with_writer(io::stderr)
                    .fmt_fields(fmt::format::JsonFields::default())
                    .event_format(format().json())
                    .with_filter(env_filter);
                subscriber.with(None).with(Some(layer))
            }
            v => {
                unreachable!("file logging format {v} is not supported");
            }
        }
    } else {
        subscriber.with(None).with(None)
    };

    // Jaeger layer.
    // TODO: we should support config this in the future.
    let mut jaeger_layer = None;
    let jaeger_agent_endpoint =
        env::var("DATABEND_JAEGER_AGENT_ENDPOINT").unwrap_or_else(|_| "".to_string());
    if !jaeger_agent_endpoint.is_empty() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name(name)
            .with_endpoint(jaeger_agent_endpoint)
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)
            .expect("install");

        // Load filter from `RUST_LOG`. Default to `ERROR`.
        let env_filter = EnvFilter::from_default_env();
        jaeger_layer = Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(env_filter),
        );
    }
    let subscriber = subscriber.with(jaeger_layer);

    let tracing_layer = if env::var("DATABEND_TRACING_LOG_ENABLED").is_ok() {
        let span_rolling_appender =
            RollingFileAppender::new(Rotation::HOURLY, format!("{}/tracing", cfg.file.dir), name);
        let (writer, writer_guard) = tracing_appender::non_blocking(span_rolling_appender);
        guards.push(writer_guard);

        let directives =
            env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_x| cfg.file.level.to_string());
        let env_filter = EnvFilter::new(directives);

        let f_layer = fmt::Layer::new()
            .with_span_events(fmt::format::FmtSpan::FULL)
            .with_writer(writer)
            .with_ansi(false)
            .event_format(EventFormatter {})
            .with_filter(env_filter);

        Some(f_layer)
    } else {
        None
    };
    let subscriber = subscriber.with(tracing_layer);

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
    // We will ignore any errors that returned by this functions.
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

impl QueryLogger {
    pub fn init(app_name_shuffle: String, config: &Config) -> Result<()> {
        let app_name = format!("databend-query-{}", app_name_shuffle);
        let mut _log_guards = init_logging(app_name.as_str(), config);
        let query_detail_dir = format!("{}/query-detail", config.file.dir);

        GlobalInstance::set(match config.file.on {
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
        });

        Ok(())
    }

    pub fn instance() -> Arc<QueryLogger> {
        GlobalInstance::get()
    }

    pub fn get_subscriber(&self) -> Option<Arc<dyn Subscriber + Send + Sync>> {
        self.subscriber.clone()
    }
}

/// Format tracing events with span-id support.
pub struct EventFormatter {}

impl<S, N> FormatEvent<S, N> for EventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();

        SystemTime {}.format_time(&mut writer)?;
        writer.write_char(' ')?;

        let fmt_level = meta.level().as_str();
        write!(writer, "{:>5} ", fmt_level)?;

        write!(writer, "{:0>15?} ", std::thread::current().name())?;
        write!(writer, "{:0>2?} ", std::thread::current().id())?;

        if let Some(scope) = ctx.event_scope() {
            let mut seen = false;

            for span in scope.from_root() {
                write!(writer, "{}", span.metadata().name())?;
                write!(writer, "#{:x}", span.id().into_u64())?;

                seen = true;

                let ext = span.extensions();
                if let Some(fields) = &ext.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "{{{}}}", fields)?;
                    }
                }
                write!(writer, ":")?;
            }

            if seen {
                writer.write_char(' ')?;
            }
        };

        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}
