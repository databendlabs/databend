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
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use fastrace::collector::Config as CollectorConfig;
use fastrace::collector::Reporter;
use fastrace::collector::SpanRecord;
use fastrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::InstrumentationScope;
use opentelemetry::KeyValue;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;
use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::trace::SpanExporter;

static TRACE_TEST_LOCK: Mutex<()> = Mutex::new(());
static TRACE_REPORTER: OnceLock<TraceCaptureHandle> = OnceLock::new();

pub(crate) fn trace_test_lock() -> &'static Mutex<()> {
    &TRACE_TEST_LOCK
}

pub(crate) fn persist_trace_debug_output<T: serde::Serialize>(
    output_name: &str,
    content: &T,
) -> anyhow::Result<PathBuf> {
    let output_dir = env_trace_debug_output_dir();
    fs::create_dir_all(&output_dir)?;
    let file_path = output_dir.join(format!(
        "{output_name}-{}.otlp.json",
        trace_debug_timestamp()
    ));

    fs::write(&file_path, serde_json::to_vec_pretty(content)?)?;
    Ok(file_path)
}

fn env_trace_debug_output_dir() -> PathBuf {
    std::env::var("DATABEND_TRACE_DEBUG_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("databend-trace-debug"))
}

fn trace_debug_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    format!("{secs}-{millis:03}")
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TraceDebugRuntimeFlavor {
    CurrentThread,
    MultiThread,
}

pub(crate) fn run_future_in_named_thread<F, Fut, T>(
    thread_name: &str,
    runtime_flavor: TraceDebugRuntimeFlavor,
    task: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<T>> + 'static,
    T: Send + 'static,
{
    let thread_name = thread_name.to_string();
    let handle = std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || -> anyhow::Result<T> {
            let runtime = match runtime_flavor {
                TraceDebugRuntimeFlavor::CurrentThread => {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?
                }
                TraceDebugRuntimeFlavor::MultiThread => {
                    let mut builder = tokio::runtime::Builder::new_multi_thread();
                    builder.enable_all();
                    builder.thread_name(thread_name.clone());
                    builder.build()?
                }
            };

            runtime.block_on(task())
        })?;

    match handle.join() {
        Ok(result) => result,
        Err(cause) => {
            let message = if let Some(message) = cause.downcast_ref::<&'static str>() {
                (*message).to_string()
            } else if let Some(message) = cause.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic message".to_string()
            };
            Err(anyhow::anyhow!("trace debug thread panicked: {message}"))
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct TraceCaptureHandle {
    inner: Arc<TraceCaptureState>,
}

#[derive(Default)]
struct TraceCaptureState {
    spans: Mutex<Vec<SpanRecord>>,
}

impl TraceCaptureHandle {
    pub(crate) fn reset(&self) {
        self.inner.spans.lock().unwrap().clear();
    }

    pub(crate) fn snapshot(&self) -> Vec<SpanRecord> {
        self.inner.spans.lock().unwrap().clone()
    }
}

struct TraceCaptureReporter {
    state: Arc<TraceCaptureState>,
}

impl TraceCaptureReporter {
    fn new(handle: &TraceCaptureHandle) -> Self {
        Self {
            state: handle.inner.clone(),
        }
    }
}

impl Reporter for TraceCaptureReporter {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        self.state.spans.lock().unwrap().extend(spans);
    }
}

#[derive(Debug, Clone, Default)]
struct CapturingSpanExporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl CapturingSpanExporter {
    fn new() -> (Self, Arc<Mutex<Vec<SpanData>>>) {
        let spans = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                spans: Arc::clone(&spans),
            },
            spans,
        )
    }
}

impl SpanExporter for CapturingSpanExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        self.spans.lock().unwrap().extend(batch);
        std::future::ready(Ok(()))
    }
}

pub(crate) fn trace_capture_handle() -> TraceCaptureHandle {
    TRACE_REPORTER
        .get_or_init(|| {
            let handle = TraceCaptureHandle::default();
            fastrace::set_reporter(
                TraceCaptureReporter::new(&handle),
                CollectorConfig::default().report_interval(Duration::from_secs(3600)),
            );
            handle
        })
        .clone()
}

pub(crate) async fn setup_trace_debug_fixture() -> anyhow::Result<TestFixture> {
    let mut config = ConfigBuilder::create().config();
    config.log.tracing.on = false;
    config.log.structlog.on = true;
    TestFixture::setup_with_config(&config)
        .await
        .map_err(Into::into)
}

pub(crate) fn build_trace_debug_otlp_export(spans: &[SpanRecord]) -> ExportTraceServiceRequest {
    if spans.is_empty() {
        return ExportTraceServiceRequest {
            resource_spans: Vec::new(),
        };
    }

    let mut spans = spans.to_vec();
    spans.sort_by_key(|span| {
        (
            span.trace_id.0,
            span.begin_time_unix_ns,
            span.duration_ns,
            span.span_id.0,
        )
    });

    let resource = Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", "databend-query"),
            KeyValue::new("service.namespace", "databend"),
        ])
        .build();
    let resource_attrs = ResourceAttributesWithSchema::from(&resource);

    let (exporter, exported_spans) = CapturingSpanExporter::new();
    let scope = InstrumentationScope::builder("databend-query.trace-debug").build();
    let mut reporter = OpenTelemetryReporter::new(exporter, Cow::Owned(resource), scope);
    reporter.report(spans);

    ExportTraceServiceRequest {
        resource_spans: group_spans_by_resource_and_scope(
            exported_spans.lock().unwrap().clone(),
            &resource_attrs,
        ),
    }
}

fn span_property<'a>(span: &'a SpanRecord, key: &str) -> Option<&'a str> {
    span.properties
        .iter()
        .find(|(k, _)| k.as_ref() == key)
        .map(|(_, v)| v.as_ref())
}

#[derive(Debug)]
pub(crate) struct QueryTraceTree {
    pub(crate) spans: Vec<SpanRecord>,
    pub(crate) roots: Vec<usize>,
    pub(crate) children: Vec<Vec<usize>>,
    #[allow(dead_code)]
    pub(crate) parents: Vec<Option<usize>>,
}

pub(crate) fn span_end_time(span: &SpanRecord) -> u64 {
    span.begin_time_unix_ns.saturating_add(span.duration_ns)
}

fn format_relative_time_ns(base_time: u64, timestamp: u64) -> String {
    let delta_us = timestamp.saturating_sub(base_time) / 1_000;
    format!("+{delta_us}us")
}

fn format_duration_ns(duration_ns: u64) -> String {
    format!("{}us", duration_ns / 1_000)
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct QueryTraceFormatOptions {
    pub(crate) show_duration: bool,
    pub(crate) show_links: bool,
    pub(crate) show_thread_name: bool,
    pub(crate) show_ids: bool,
    pub(crate) show_properties: bool,
    pub(crate) show_events: bool,
}

impl Default for QueryTraceFormatOptions {
    fn default() -> Self {
        Self {
            show_duration: true,
            show_links: true,
            show_thread_name: true,
            show_ids: false,
            show_properties: false,
            show_events: false,
        }
    }
}

#[allow(dead_code)]
impl QueryTraceFormatOptions {
    pub(crate) fn with_duration(mut self, enabled: bool) -> Self {
        self.show_duration = enabled;
        self
    }

    pub(crate) fn with_links(mut self, enabled: bool) -> Self {
        self.show_links = enabled;
        self
    }

    pub(crate) fn with_thread_name(mut self, enabled: bool) -> Self {
        self.show_thread_name = enabled;
        self
    }

    pub(crate) fn with_ids(mut self, enabled: bool) -> Self {
        self.show_ids = enabled;
        self
    }

    pub(crate) fn with_properties(mut self, enabled: bool) -> Self {
        self.show_properties = enabled;
        self
    }

    pub(crate) fn with_events(mut self, enabled: bool) -> Self {
        self.show_events = enabled;
        self
    }
}

pub(crate) struct QueryTraceTreeFormatter<'a> {
    tree: &'a QueryTraceTree,
    options: QueryTraceFormatOptions,
}

#[allow(dead_code)]
impl<'a> QueryTraceTreeFormatter<'a> {
    pub(crate) fn new(tree: &'a QueryTraceTree) -> Self {
        Self {
            tree,
            options: QueryTraceFormatOptions::default(),
        }
    }

    pub(crate) fn with_options(mut self, options: QueryTraceFormatOptions) -> Self {
        self.options = options;
        self
    }

    pub(crate) fn format(&self) -> String {
        let Some(root_index) = self.tree.roots.first().copied() else {
            return "<empty trace>".to_string();
        };

        let base_time = self.tree.spans[root_index].begin_time_unix_ns;
        let mut lines = Vec::new();

        for &tree_root_index in &self.tree.roots {
            self.walk(tree_root_index, 0, base_time, &mut lines);
        }

        lines.join("\n")
    }

    fn walk(&self, index: usize, depth: usize, base_time: u64, lines: &mut Vec<String>) {
        let span = &self.tree.spans[index];
        let indent = "  ".repeat(depth);
        let mut suffixes = vec![format!(
            "{} -> {}",
            format_relative_time_ns(base_time, span.begin_time_unix_ns),
            format_relative_time_ns(base_time, span_end_time(span)),
        )];

        if self.options.show_duration {
            suffixes.push(format!("dur={}", format_duration_ns(span.duration_ns)));
        }

        if self.options.show_links && !span.links.is_empty() {
            suffixes.push(format!("links={}", span.links.len()));
        }

        if self.options.show_thread_name
            && let Some(thread_name) = span_property(span, "thread_name")
        {
            suffixes.push(format!("thread={thread_name}"));
        }

        if self.options.show_ids {
            suffixes.push(format!("trace={:?}", span.trace_id));
            suffixes.push(format!("span={:?}", span.span_id));
            suffixes.push(format!("parent={:?}", span.parent_id));
        }

        if self.options.show_properties && !span.properties.is_empty() {
            let properties = span
                .properties
                .iter()
                .filter(|(key, _)| !self.options.show_thread_name || key.as_ref() != "thread_name")
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>();
            if !properties.is_empty() {
                suffixes.push(format!("properties=[{}]", properties.join(", ")));
            }
        }

        lines.push(format!("{indent}{} [{}]", span.name, suffixes.join(", ")));

        if self.options.show_events {
            for event in &span.events {
                let event_indent = "  ".repeat(depth + 1);
                let mut event_suffixes = vec![format!(
                    "at={}",
                    format_relative_time_ns(base_time, event.timestamp_unix_ns)
                )];
                if !event.properties.is_empty() {
                    let properties = event
                        .properties
                        .iter()
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    event_suffixes.push(format!("properties=[{properties}]"));
                }
                lines.push(format!(
                    "{event_indent}event {} [{}]",
                    event.name,
                    event_suffixes.join(", ")
                ));
            }
        }

        for &child_index in &self.tree.children[index] {
            self.walk(child_index, depth + 1, base_time, lines);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RawSpanFormatOptions {
    pub(crate) show_ids: bool,
    pub(crate) show_links: bool,
    pub(crate) show_thread_name: bool,
    pub(crate) show_properties: bool,
    pub(crate) show_events: bool,
}

impl Default for RawSpanFormatOptions {
    fn default() -> Self {
        Self {
            show_ids: true,
            show_links: true,
            show_thread_name: true,
            show_properties: false,
            show_events: false,
        }
    }
}

#[allow(dead_code)]
impl RawSpanFormatOptions {
    pub(crate) fn with_properties(mut self, enabled: bool) -> Self {
        self.show_properties = enabled;
        self
    }

    pub(crate) fn with_thread_name(mut self, enabled: bool) -> Self {
        self.show_thread_name = enabled;
        self
    }

    pub(crate) fn with_ids(mut self, enabled: bool) -> Self {
        self.show_ids = enabled;
        self
    }

    pub(crate) fn with_links(mut self, enabled: bool) -> Self {
        self.show_links = enabled;
        self
    }

    pub(crate) fn with_events(mut self, enabled: bool) -> Self {
        self.show_events = enabled;
        self
    }
}
