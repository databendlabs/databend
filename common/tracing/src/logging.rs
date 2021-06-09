// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Once;

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

pub fn init_default_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        init_tracing(EnvFilter::from_default_env());
    });
}

pub fn init_tracing_with_level(level: &str) {
    static START: Once = Once::new();

    START.call_once(|| {
        let log_layer_filter = EnvFilter::try_new(level).unwrap();
        init_tracing(log_layer_filter);
    });
}

fn init_tracing(filter: EnvFilter) {
    let fmt_layer = fmt::Layer::default()
        .with_thread_ids(true)
        .with_ansi(true)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default().with(filter).with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
}
