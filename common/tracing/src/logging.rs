// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Once;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_bunyan_formatter::JsonStorageLayer;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

/// Write logs to stdout.
pub fn init_default_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        init_tracing_stdout("error");
    });
}

fn init_tracing_stdout(level: &str) {
    let fmt_layer = fmt::Layer::default()
        .with_thread_ids(true)
        .with_ansi(true)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default()
        .with(EnvFilter::new(level))
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
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
        .with(file_logging_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    guards
}
