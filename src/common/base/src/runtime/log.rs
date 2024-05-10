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

use std::fmt;

use log::error;
use log::info;
use log::warn;
use log::LevelFilter;
use tokio::task_local;
use tracing::subscriber::set_global_default;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

task_local! {
    pub static QUERY_ID: String;
}

pub struct QueryIdLayer;

impl<S> Layer<S> for QueryIdLayer
where S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = QueryIdVisitor::new();
        event.record(&mut visitor);

        let metadata = event.metadata();
        let logger = log::logger();

        match *metadata.level() {
            tracing::Level::ERROR => {
                if logger.enabled(&log::Metadata::builder().level(log::Level::Error).build()) {
                    QUERY_ID.with(|id| {
                        if !id.is_empty() {
                            error!("[{}] {}", id, visitor.message);
                        } else {
                            error!("{}", visitor.message);
                        }
                    });
                }
            }
            tracing::Level::WARN => {
                if logger.enabled(&log::Metadata::builder().level(log::Level::Warn).build()) {
                    QUERY_ID.with(|id| {
                        if !id.is_empty() {
                            warn!("[{}] {}", id, visitor.message);
                        } else {
                            warn!("{}", visitor.message);
                        }
                    });
                }
            }
            tracing::Level::INFO => {
                if logger.enabled(&log::Metadata::builder().level(log::Level::Info).build()) {
                    QUERY_ID.with(|id| {
                        if !id.is_empty() {
                            info!("[{}] {}", id, visitor.message);
                        } else {
                            info!("-- {}", visitor.message);
                        }
                    });
                }
            }
            _ => {}
        }
    }
}

struct QueryIdVisitor {
    message: String,
}

impl QueryIdVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl<'a> tracing::field::Visit for QueryIdVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_runtime_log() {
    let subscriber = Registry::default().with(QueryIdLayer);

    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(LevelFilter::Info)
        .try_init();

    set_global_default(subscriber).expect("Failed to set subscriber");

    async fn some_async_work() {
        info!("This is an info log in some_async_work");
    }

    async fn spawn_in_spawn() {
        info!("This is an info log in spawn_in_spawn");
    }

    info!("This is an info log without query_id");

    tokio::spawn(async {
        QUERY_ID
            .scope(String::from("query-1"), async {
                some_async_work().await;
            })
            .await;
    });

    tokio::spawn(async {
        QUERY_ID
            .scope(String::from("query-2"), async {
                some_async_work().await;
            })
            .await;
    });

    tokio::spawn(async {
        QUERY_ID
            .scope(String::from("query-3"), async {
                spawn_in_spawn().await;
            })
            .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}
