// Copyright 2020 Datafuse Labs.
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
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;

use lazy_static::lazy_static;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_bunyan_formatter::JsonStorageLayer;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;
use tracing_subscriber::EnvFilter;

use crate::tracing::subscriber::DefaultGuard;

/// Write logs to stdout.
pub fn init_default_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        init_tracing_stdout();
    });
}

/// Init tracing for unittest.
/// Write logs to file `unittest`.
pub fn init_default_ut_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock().unwrap();
        *g = Some(init_global_tracing("unittest", "_logs"));
    });
}

lazy_static! {
    static ref GLOBAL_UT_LOG_GUARD: Arc<Mutex<Option<WorkerGuard>>> = Arc::new(Mutex::new(None));
}

/// Init logging and tracing.
///
/// To enable reporting tracing data to jaeger, set env var `FUSE_JAEGER` to non-empty value.
/// A local tracing collection(maybe for testing) can be done with a local jaeger server.
/// To report tracing data and view it:
///   docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest
///   FUSE_JAEGER=on RUST_LOG=trace cargo test
///   open http://localhost:16686/
///
/// To adjust batch sending delay, use `OTEL_BSP_SCHEDULE_DELAY`:
///   FUSE_JAEGER=on RUST_LOG=trace OTEL_BSP_SCHEDULE_DELAY=1 cargo test
///
// TODO(xp): use FUSE_JAEGER to assign jaeger server address.
fn init_tracing_stdout() {
    let fmt_layer = Layer::default()
        .with_thread_ids(true)
        .with_thread_names(false)
        // .pretty()
        .with_ansi(false)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(jaeger_layer());

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
}

fn jaeger_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> Option<impl tracing_subscriber::layer::Layer<S>> {
    let fuse_jaeger = env::var("FUSE_JAEGER").unwrap_or_else(|_| "".to_string());

    if !fuse_jaeger.is_empty() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("databend-store")
            .install_batch(opentelemetry::runtime::Tokio)
            .expect("install");

        let ot_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        Some(ot_layer)
    } else {
        None
    }
}

/// Write logs to file and rotation by HOUR.
pub fn init_tracing_with_file(app_name: &str, dir: &str, level: &str) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_logging_layer = Layer::new().with_writer(stdout_writer);
    guards.push(stdout_guard);

    let file_appender = RollingFileAppender::new(Rotation::HOURLY, dir, app_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let file_logging_layer = BunyanFormattingLayer::new(app_name.to_string(), file_writer);
    guards.push(file_guard);

    let subscriber = Registry::default()
        .with(EnvFilter::new(level))
        .with(stdout_logging_layer)
        .with(JsonStorageLayer)
        .with(file_logging_layer)
        .with(jaeger_layer());

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    guards
}

/// Creates a tracing/logging subscriber that is valid until the guards are dropped.
/// The format layer logging span/event in plain text, without color, one event per line.
/// This is useful in a unit test.
pub fn init_tracing(app_name: &str, dir: &str) -> (WorkerGuard, DefaultGuard) {
    let (g, sub) = init_file_subscriber(app_name, dir);

    let subs_guard = tracing::subscriber::set_default(sub);

    tracing::info!("initialized tracing");
    (g, subs_guard)
}

/// Creates a global tracing/logging subscriber which saves events in one log file.
pub fn init_global_tracing(app_name: &str, dir: &str) -> WorkerGuard {
    let (g, sub) = init_file_subscriber(app_name, dir);
    tracing::subscriber::set_global_default(sub).expect("error setting global tracing subscriber");

    tracing::info!("initialized global tracing");
    g
}

/// Create a file based tracing/logging subscriber.
/// A guard must be held during using the logging.
/// The format layer logging span/event in plain text, without color, one event per line.
/// Optionally it adds a layer to send to opentelemetry if env var `FUSE_JAEGER` is present.
pub fn init_file_subscriber(app_name: &str, dir: &str) -> (WorkerGuard, impl Subscriber) {
    let path_str = dir.to_string() + "/" + app_name;
    let path: &Path = path_str.as_ref();

    // open log file

    let mut open_options = OpenOptions::new();
    open_options.append(true).create(true);

    let mut open_res = open_options.open(path);
    if open_res.is_err() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
            open_res = open_options.open(path);
        }
    }

    let f = open_res.unwrap();

    // build subscriber

    let (writer, writer_guard) = tracing_appender::non_blocking(f);

    let f_layer = Layer::new()
        .with_writer(writer)
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_ansi(false)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(f_layer)
        .with(jaeger_layer());

    (writer_guard, subscriber)
}
