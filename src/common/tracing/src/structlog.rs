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
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write;

use fastrace::collector::EventRecord;
use fastrace::collector::Reporter;
use fastrace::collector::SpanId;
use fastrace::collector::SpanRecord;
use itertools::Itertools;
use log::info;
pub struct DummyReporter;

impl Reporter for DummyReporter {
    fn report(&mut self, _spans: Vec<SpanRecord>) {}
}

pub struct StructLogReporter<R: Reporter> {
    inner: R,
}

impl Default for StructLogReporter<DummyReporter> {
    fn default() -> Self {
        Self::new()
    }
}

impl StructLogReporter<DummyReporter> {
    pub fn new() -> Self {
        Self {
            inner: DummyReporter,
        }
    }
}

impl<R: Reporter> StructLogReporter<R> {
    pub fn wrap(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: Reporter> Reporter for StructLogReporter<R> {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        let mut traces_contain_event = HashSet::new();

        // Find all traces that contain event.
        for span in &spans {
            if !span.events.is_empty() {
                traces_contain_event.insert(span.trace_id);
            }
        }

        // Print all traces that contain event.
        for trace_id in traces_contain_event {
            let spans: Vec<&SpanRecord> = spans
                .iter()
                .filter(|span| span.trace_id == trace_id)
                .collect();
            pretty_print_trace(&spans);
        }

        self.inner.report(spans);
    }
}

pub fn pretty_print_trace(spans: &[&SpanRecord]) {
    debug_assert!(spans.iter().map(|span| span.trace_id).all_equal());

    for mut tree in build_trees(spans) {
        let has_event = remove_no_event(&mut tree);
        if !has_event {
            return;
        }

        sort_tree(&mut tree);
        let mut buf = String::new();
        write_tree(&mut buf, &tree, "".to_string(), true, true).unwrap();

        info!(target: "databend::log::structlog", "{buf}");
    }
}

#[derive(Debug, Clone)]
struct TreeNode {
    record: TreeRecord,
    children: Vec<TreeNode>,
}

#[derive(Debug, Clone)]
enum TreeRecord {
    Span(SpanRecord),
    Event(EventRecord),
}

impl TreeRecord {
    fn is_event(&self) -> bool {
        match self {
            TreeRecord::Span(_) => false,
            TreeRecord::Event(_) => true,
        }
    }

    fn name(&self) -> &str {
        match self {
            TreeRecord::Span(span) => &span.name,
            TreeRecord::Event(event) => &event.name,
        }
    }

    fn start_timestamp(&self) -> u64 {
        match self {
            TreeRecord::Span(span) => span.begin_time_unix_ns,
            TreeRecord::Event(event) => event.timestamp_unix_ns,
        }
    }

    fn properties(&self) -> &[(Cow<'static, str>, Cow<'static, str>)] {
        match self {
            TreeRecord::Span(span) => &span.properties,
            TreeRecord::Event(event) => &event.properties,
        }
    }
}

impl TreeNode {
    fn from_span(span: SpanRecord) -> Self {
        TreeNode {
            record: TreeRecord::Span(span),
            children: Vec::new(),
        }
    }

    fn from_event(event: EventRecord) -> Self {
        TreeNode {
            record: TreeRecord::Event(event),
            children: Vec::new(),
        }
    }
}

fn build_trees(spans: &[&SpanRecord]) -> Vec<TreeNode> {
    let mut span_ids = HashSet::new();
    let mut raw = HashMap::new();
    for span in spans {
        span_ids.insert(span.span_id);
        raw.entry(span.parent_id)
            .or_insert_with(Vec::new)
            .push(*span);
    }

    let roots = raw.keys().filter(|id| !span_ids.contains(id)).cloned();
    roots
        .filter_map(|root| build_sub_tree(root, &raw).pop())
        .collect()
}

fn build_sub_tree(parent_id: SpanId, raw: &HashMap<SpanId, Vec<&SpanRecord>>) -> Vec<TreeNode> {
    let mut trees = Vec::new();
    if let Some(records) = raw.get(&parent_id) {
        for record in records {
            let mut tree = TreeNode::from_span((*record).clone());
            tree.children = build_sub_tree(record.span_id, raw);
            tree.children
                .extend(record.events.iter().cloned().map(TreeNode::from_event));
            trees.push(tree);
        }
    }
    trees
}

fn remove_no_event(node: &mut TreeNode) -> bool {
    if node.record.is_event() {
        return true;
    }
    node.children.retain_mut(remove_no_event);
    !node.children.is_empty()
}

fn sort_tree(node: &mut TreeNode) {
    node.children
        .sort_by_key(|child| child.record.start_timestamp());
    for child in &mut node.children {
        sort_tree(child);
    }
}

fn write_tree(
    buf: &mut impl Write,
    node: &TreeNode,
    prefix: String,
    is_last: bool,
    is_root: bool,
) -> std::fmt::Result {
    let connector = if is_root {
        ""
    } else if is_last {
        "╰─ "
    } else {
        "├─ "
    };

    let lines = node.record.name().lines().collect::<Vec<_>>();
    let is_multiline = lines.len() > 1;

    // Update prefix for children
    let new_prefix = if is_root {
        ""
    } else if is_last {
        "   "
    } else {
        "│  "
    };
    let children_prefix = prefix.clone() + new_prefix;

    // Process span name
    for (i, line) in lines.iter().enumerate() {
        if i == 0 {
            write!(buf, "{}{}{}", prefix, connector, line)?;
        } else {
            write!(buf, "{}{}", children_prefix, line)?;
        }
        if i < lines.len() - 1 {
            writeln!(buf)?;
        }
    }

    // Process properties
    if is_multiline {
        write!(buf, "\n{}", children_prefix)?;
    } else {
        write!(buf, " ")?; // Single line, continue on the same line
    }
    write_properties(buf, node.record.properties())?;
    writeln!(buf)?;

    // Process children
    let count = node.children.len();
    for (i, child) in node.children.iter().enumerate() {
        write_tree(buf, child, children_prefix.clone(), i + 1 == count, false)?;
    }

    Ok(())
}

fn write_properties(
    buf: &mut impl Write,
    properties: &[(Cow<'static, str>, Cow<'static, str>)],
) -> std::fmt::Result {
    if properties.is_empty() {
        return Ok(());
    }

    write!(buf, "{{ ")?;
    for (i, (k, v)) in properties.iter().enumerate() {
        if i > 0 {
            write!(buf, ", ")?;
        }
        write!(buf, r#""{}": "{}""#, k, v)?;
    }
    write!(buf, " }}")?;

    Ok(())
}
