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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use fastrace::collector::Reporter;
use fastrace::collector::SpanRecord;
use serde_json::Value as JsonValue;
use serde_json::json;

/// Filter options for trace collection
#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct TraceFilterOptions {
    /// Minimum duration in microseconds (spans shorter than this are filtered out)
    pub min_duration_us: Option<u64>,
    /// Maximum duration in microseconds (spans longer than this are filtered out)
    pub max_duration_us: Option<u64>,
    /// Filter level: if true, exclude processor-level spans
    pub high_level_only: bool,
}

impl TraceFilterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_min_duration_us(mut self, min_us: u64) -> Self {
        self.min_duration_us = Some(min_us);
        self
    }

    pub fn with_max_duration_us(mut self, max_us: u64) -> Self {
        self.max_duration_us = Some(max_us);
        self
    }

    pub fn with_high_level_only(mut self, high_level: bool) -> Self {
        self.high_level_only = high_level;
        self
    }

    /// Check if a span passes the filter
    pub fn passes(&self, span: &SpanRecord) -> bool {
        let duration_us = span.duration_ns / 1_000;

        // Check min duration
        if let Some(min) = self.min_duration_us
            && duration_us < min
        {
            return false;
        }

        // Check max duration
        if let Some(max) = self.max_duration_us
            && duration_us > max
        {
            return false;
        }

        // Check high level filter
        if self.high_level_only {
            let name = &span.name;
            if name.ends_with("::process") || name.ends_with("::async_process") {
                return false;
            }
            if name.contains("ProcessorAsyncTask") {
                return false;
            }
        }

        true
    }
}

/// In-memory trace collector that captures fastrace spans with optional filtering
#[derive(Clone, Default)]
pub struct TraceCollector {
    spans: Arc<Mutex<Vec<SpanRecord>>>,
    filter: Arc<Mutex<TraceFilterOptions>>,
}

impl TraceCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_filter(filter: TraceFilterOptions) -> Self {
        Self {
            spans: Arc::new(Mutex::new(Vec::new())),
            filter: Arc::new(Mutex::new(filter)),
        }
    }

    pub fn set_filter(&self, filter: TraceFilterOptions) {
        *self.filter.lock().unwrap() = filter;
    }

    pub fn get_spans(&self) -> Vec<SpanRecord> {
        self.spans.lock().unwrap().clone()
    }
}

impl Reporter for TraceCollector {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        let filter = self.filter.lock().unwrap();
        let filtered: Vec<SpanRecord> = spans
            .into_iter()
            .filter(|span| filter.passes(span))
            .collect();
        drop(filter);
        self.spans.lock().unwrap().extend(filtered);
    }
}

/// Query trace utilities for EXPLAIN TRACE
pub struct QueryTrace;

impl QueryTrace {
    /// Convert trace_id to hex string (32 chars for 128-bit)
    fn trace_id_to_hex(trace_id: &fastrace::collector::TraceId) -> String {
        format!("{:032x}", trace_id.0)
    }

    /// Convert span_id to hex string (16 chars for 64-bit)
    fn span_id_to_hex(span_id: &fastrace::collector::SpanId) -> String {
        format!("{:016x}", span_id.0)
    }

    /// Convert a single span to Jaeger format
    /// process_id is the node_id to distinguish different nodes in Jaeger
    fn span_to_jaeger(span: &SpanRecord, node_id: &str) -> JsonValue {
        let trace_id = Self::trace_id_to_hex(&span.trace_id);
        let span_id = Self::span_id_to_hex(&span.span_id);

        // Convert nanoseconds to microseconds for Jaeger
        let start_time = span.begin_time_unix_ns / 1000;
        let duration = span.duration_ns / 1000;

        // Build tags from properties
        let tags: Vec<JsonValue> = span
            .properties
            .iter()
            .map(|(k, v)| {
                json!({
                    "key": k,
                    "type": "string",
                    "value": v
                })
            })
            .collect();

        // Build references (parent span)
        let mut references = Vec::new();
        if span.parent_id.0 != 0 {
            references.push(json!({
                "refType": "CHILD_OF",
                "traceID": trace_id,
                "spanID": Self::span_id_to_hex(&span.parent_id)
            }));
        }

        // Build logs from events
        let logs: Vec<JsonValue> = span
            .events
            .iter()
            .map(|event| {
                let mut fields = vec![json!({
                    "key": "event",
                    "type": "string",
                    "value": event.name
                })];
                for (k, v) in &event.properties {
                    fields.push(json!({
                        "key": k,
                        "type": "string",
                        "value": v
                    }));
                }
                json!({
                    "timestamp": event.timestamp_unix_ns / 1000,
                    "fields": fields
                })
            })
            .collect();

        // Use node_id as processID to distinguish nodes in Jaeger
        json!({
            "traceID": trace_id,
            "spanID": span_id,
            "operationName": span.name,
            "references": references,
            "startTime": start_time,
            "duration": duration,
            "tags": tags,
            "logs": logs,
            "processID": node_id
        })
    }

    /// Convert spans to Jaeger JSON format (single node)
    pub fn to_jaeger_json(spans: Vec<SpanRecord>) -> String {
        Self::to_jaeger_json_with_node(spans, "local")
    }

    /// Convert spans to Jaeger JSON format with node identifier
    pub fn to_jaeger_json_with_node(spans: Vec<SpanRecord>, node_id: &str) -> String {
        // Convert all spans
        let jaeger_spans: Vec<JsonValue> = spans
            .iter()
            .map(|span| Self::span_to_jaeger(span, node_id))
            .collect();

        // Group spans by traceID
        let mut traces_by_id: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for span in jaeger_spans {
            let trace_id = span["traceID"].as_str().unwrap_or("").to_string();
            traces_by_id.entry(trace_id).or_default().push(span);
        }

        // Build processes map - one process per node
        let mut processes = serde_json::Map::new();
        processes.insert(
            node_id.to_string(),
            json!({
                "serviceName": format!("databend-query-{}", node_id),
                "tags": []
            }),
        );

        // Build traces array
        let traces: Vec<JsonValue> = traces_by_id
            .into_iter()
            .map(|(trace_id, spans)| {
                json!({
                    "traceID": trace_id,
                    "spans": spans,
                    "processes": processes.clone()
                })
            })
            .collect();

        let output = json!({
            "data": traces
        });

        serde_json::to_string_pretty(&output)
            .unwrap_or_else(|e| format!("Error serializing trace: {}", e))
    }

    /// Merge multiple Jaeger JSON traces into a single Jaeger JSON
    /// Only includes spans that match the specified trace_id_filter
    pub fn merge_jaeger_traces(
        local_spans: Vec<SpanRecord>,
        local_node_id: &str,
        remote_traces: &[(String, String)], // (node_id, jaeger_json)
        trace_id_filter: &str,              // Only include spans with this trace ID
    ) -> String {
        // Collect all spans from local node with node_id as processID
        let mut all_spans: Vec<JsonValue> = local_spans
            .iter()
            .map(|span| Self::span_to_jaeger(span, local_node_id))
            .collect();

        // Collect all unique node_ids for processes
        let mut node_ids: HashSet<String> = HashSet::new();
        node_ids.insert(local_node_id.to_string());

        // Parse and merge spans from remote nodes, filtering by trace ID
        for (_node_id, trace_json) in remote_traces {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(trace_json)
                && let Some(data) = parsed.get("data").and_then(|v| v.as_array())
            {
                for trace in data {
                    // Only include traces that match the filter
                    let trace_id = trace.get("traceID").and_then(|v| v.as_str()).unwrap_or("");
                    if trace_id != trace_id_filter {
                        continue;
                    }

                    if let Some(spans) = trace.get("spans").and_then(|v| v.as_array()) {
                        for span in spans {
                            // Extract node_id from the span's processID
                            if let Some(pid) = span.get("processID").and_then(|v| v.as_str()) {
                                node_ids.insert(pid.to_string());
                            }
                            all_spans.push(span.clone());
                        }
                    }
                }
            }
        }

        // Build processes map - one process per node
        let mut processes = serde_json::Map::new();
        for node_id in &node_ids {
            processes.insert(
                node_id.clone(),
                json!({
                    "serviceName": format!("databend-query-{}", node_id),
                    "tags": [
                        {"key": "node_id", "type": "string", "value": node_id}
                    ]
                }),
            );
        }

        // Build single trace with all spans (they all have the same trace ID now)
        let output = json!({
            "data": [{
                "traceID": trace_id_filter,
                "spans": all_spans,
                "processes": processes
            }]
        });

        serde_json::to_string_pretty(&output)
            .unwrap_or_else(|e| format!("Error serializing trace: {}", e))
    }
}
