// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;

use common_tracing::Config;
use once_cell::sync::Lazy;
use tracing::Event;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::fmt;
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
use tracing_subscriber::Registry;

struct MetaLogGuard {
    #[allow(dead_code)]
    log_guards: Vec<WorkerGuard>,
}

static META_LOG_GUARD: Lazy<Arc<Mutex<Option<MetaLogGuard>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()` and keeps the guard held.
#[macro_export]
macro_rules! init_meta_ut {
    () => {{
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        common_meta_sled_store::init_temp_sled_db(t);

        let guards = $crate::logging::init_meta_ut_tracing();

        let name = common_tracing::func_name!();
        let span = tracing::debug_span!("ut", "{}", name.split("::").last().unwrap());
        (guards, span)
    }};
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

/// Initialize unit test tracing for metasrv
pub fn init_meta_ut_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        let guards = do_init_meta_ut_tracing("meta_unittests", &Config::new_testing());

        let mut g = META_LOG_GUARD.as_ref().lock().unwrap();
        *g = Some(MetaLogGuard { log_guards: guards });
    });
}

pub fn do_init_meta_ut_tracing(name: &str, config: &Config) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let span_rolling_appender = RollingFileAppender::new(Rotation::HOURLY, &config.file.dir, name);
    let (writer, writer_guard) = tracing_appender::non_blocking(span_rolling_appender);

    let directives =
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_x| config.file.level.to_string());

    let f_layer = fmt::Layer::new()
        .with_span_events(fmt::format::FmtSpan::FULL)
        .with_writer(writer)
        .with_ansi(false)
        .event_format(EventFormatter {});

    let env_filter = EnvFilter::new(directives);

    guards.push(writer_guard);

    let subscriber = Registry::default().with(env_filter).with(f_layer);

    let _ = tracing::subscriber::set_global_default(subscriber);
    tracing::error!("start");

    guards
}
