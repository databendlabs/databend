use std::sync::Once;

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

pub fn init_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        // init without span:
        // fmt::init();

        let fmt_layer = fmt::Layer::default()
            .with_span_events(fmt::format::FmtSpan::FULL)
            .with_ansi(false);

        let subscriber = Registry::default()
            .with(EnvFilter::from_default_env())
            .with(fmt_layer);

        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
    });
}
