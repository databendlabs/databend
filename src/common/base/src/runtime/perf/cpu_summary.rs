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

//! Tabular and folded views of the CPU samples of `EXPLAIN PERF CPU`.

use std::collections::HashMap;
use std::collections::HashSet;

use crate::runtime::AllocStack;

/// One sampled call stack and the number of samples it received.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CpuStack {
    pub thread: String,
    /// Symbolized frames, outermost first.
    pub frames: Vec<String>,
    pub samples: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuSummaryLevel {
    Summary,
    /// A function, ranked by the samples in the function itself.
    Function,
    /// A Databend function, ranked by the samples of the stacks whose innermost Databend frame
    /// it is, i.e. the samples of the function and of the library code it calls.
    Site,
}

impl CpuSummaryLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            CpuSummaryLevel::Summary => "summary",
            CpuSummaryLevel::Function => "function",
            CpuSummaryLevel::Site => "site",
        }
    }
}

/// One row of the tabular summary of CPU samples.
#[derive(Debug, Clone, PartialEq)]
pub struct CpuSummaryRow {
    pub level: CpuSummaryLevel,
    /// `None` for the summary row.
    pub function: Option<String>,
    pub self_samples: u64,
    pub total_samples: u64,
    /// The share of all samples by the self samples.
    pub share: f64,
}

/// Whether the frame is a function of Databend, possibly a method `<Type>::method`.
pub fn is_databend_frame(frame: &str) -> bool {
    frame.trim_start_matches('<').starts_with("databend_")
}

/// The site of a stack in summaries: its innermost Databend frame, which tells which Databend
/// code runs the standard library and third party code above it. Falls back to the innermost
/// frame when the stack has no Databend frame. `frames` are outermost first.
pub fn stack_site(frames: &[String]) -> Option<&str> {
    frames
        .iter()
        .rev()
        .find(|frame| is_databend_frame(frame))
        .or(frames.last())
        .map(String::as_str)
}

/// Summarizes `stacks` into a summary row, the `limit` functions with the most self samples, and
/// the `limit` sites with the most self samples, see [`stack_site`].
///
/// The function rows show the leaf code that burns the CPU, often the standard library. The site
/// rows attribute the same samples to the Databend code running it. The total samples of a row
/// include the callees of its function. Call paths are shown by the folded stacks.
pub fn summarize_cpu_stacks(stacks: &[CpuStack], limit: usize) -> Vec<CpuSummaryRow> {
    let all = stacks.iter().map(|stack| stack.samples).sum::<u64>();

    let mut functions: HashMap<&str, (u64, u64)> = HashMap::new();
    let mut sites: HashMap<&str, u64> = HashMap::new();
    for stack in stacks {
        if let Some(leaf) = stack.frames.last() {
            functions.entry(leaf).or_default().0 += stack.samples;
        }
        if let Some(site) = stack_site(&stack.frames) {
            *sites.entry(site).or_default() += stack.samples;
        }

        // A recursive function is counted once per stack.
        let mut seen = HashSet::new();
        for frame in &stack.frames {
            if seen.insert(frame.as_str()) {
                functions.entry(frame).or_default().1 += stack.samples;
            }
        }
    }

    let share = |samples: u64| samples as f64 / all.max(1) as f64;
    let mut rows = vec![CpuSummaryRow {
        level: CpuSummaryLevel::Summary,
        function: None,
        self_samples: all,
        total_samples: all,
        share: 1.0,
    }];

    let mut leaves = functions
        .iter()
        .filter(|(_, (self_samples, _))| *self_samples > 0)
        .map(|(function, (self_samples, _))| (*function, *self_samples))
        .collect::<Vec<_>>();
    let mut sites = sites.into_iter().collect::<Vec<_>>();
    for (level, ranked) in [
        (CpuSummaryLevel::Function, &mut leaves),
        (CpuSummaryLevel::Site, &mut sites),
    ] {
        ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(right.0)));
        rows.extend(
            ranked
                .iter()
                .take(limit)
                .map(|(function, self_samples)| CpuSummaryRow {
                    level,
                    function: Some(function.to_string()),
                    self_samples: *self_samples,
                    total_samples: functions.get(function).map_or(0, |x| x.1),
                    share: share(*self_samples),
                }),
        );
    }
    rows
}

/// Folds `stacks` into `thread;frame;...;frame` lines with their samples, the largest `limit`
/// stacks first.
pub fn cpu_folded_stacks(stacks: &[CpuStack], limit: Option<usize>) -> Vec<(String, u64)> {
    // `;` separates frames in the folded format, it appears in Rust types such as `[u8; 8]`.
    let sanitize = |frame: &str| frame.replace(';', ",");

    let mut lines: HashMap<String, u64> = HashMap::new();
    for stack in stacks {
        let mut frames = Vec::with_capacity(stack.frames.len() + 1);
        frames.push(sanitize(&stack.thread));
        frames.extend(stack.frames.iter().map(|frame| sanitize(frame)));
        *lines.entry(frames.join(";")).or_default() += stack.samples;
    }

    let mut lines = lines.into_iter().collect::<Vec<_>>();
    lines.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    lines.truncate(limit.unwrap_or(usize::MAX));
    lines
}

/// Renders `stacks` as a flamegraph SVG, rooted at the threads.
pub fn cpu_flamegraph(stacks: &[CpuStack], title: &str) -> Result<String, String> {
    let lines = cpu_folded_stacks(stacks, None)
        .into_iter()
        .map(|(stack, samples)| format!("{stack} {samples}"))
        .collect::<Vec<_>>();

    let mut options = pprof::flamegraph::Options::default();
    options.title = title.to_string();
    options.count_name = "samples".to_string();

    let mut svg = Vec::new();
    pprof::flamegraph::from_lines(&mut options, lines.iter().map(String::as_str), &mut svg)
        .map_err(|e| format!("failed to render flamegraph: {e}"))?;
    String::from_utf8(svg).map_err(|e| format!("invalid flamegraph svg: {e}"))
}

/// The samples one node sends back to the coordinator of `EXPLAIN PERF`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PerfSamples {
    pub cpu: Vec<CpuStack>,
    pub memory: Vec<AllocStack>,
}

impl PerfSamples {
    pub fn is_empty(&self) -> bool {
        self.cpu.is_empty() && self.memory.is_empty()
    }
}

/// Prefixes the folded stacks of each node with the node id, the largest `limit` stacks first.
pub fn prefix_folded_by_node(
    nodes: Vec<(String, Vec<(String, u64)>)>,
    limit: Option<usize>,
) -> Vec<(String, u64)> {
    let mut lines = nodes
        .into_iter()
        .flat_map(|(node, lines)| {
            let node = node.replace(';', ",");
            lines
                .into_iter()
                .map(move |(stack, value)| (format!("{node};{stack}"), value))
        })
        .collect::<Vec<_>>();
    lines.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    lines.truncate(limit.unwrap_or(usize::MAX));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stack(frames: &[&str], samples: u64) -> CpuStack {
        CpuStack {
            thread: "worker".to_string(),
            frames: frames.iter().map(|x| x.to_string()).collect(),
            samples,
        }
    }

    #[test]
    fn test_summarize_cpu_stacks() {
        let stacks = vec![
            stack(&["main", "databend_join", "databend_hash"], 6),
            stack(&["main", "databend_join", "memcpy"], 3),
            stack(&["main", "<databend_scan>::read", "memcpy"], 1),
            // Recursion is counted once in the total.
            stack(&["main", "databend_sort", "databend_sort"], 2),
        ];

        let recursive = summarize_cpu_stacks(&stacks, 3);
        // Counted once per stack in the total.
        assert_eq!(recursive[3].function.as_deref(), Some("databend_sort"));
        assert_eq!(recursive[3].self_samples, 2);
        assert_eq!(recursive[3].total_samples, 2);
        assert_eq!(recursive.len(), 1 + 3 + 3);

        let rows = summarize_cpu_stacks(&stacks, 2)
            .iter()
            .map(|row| {
                format!(
                    "{} {} {} {} {:.2}",
                    row.level.as_str(),
                    row.function.as_deref().unwrap_or("-"),
                    row.self_samples,
                    row.total_samples,
                    row.share
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![
            "summary - 12 12 1.00",
            "function databend_hash 6 6 0.50",
            "function memcpy 4 4 0.33",
            "site databend_hash 6 6 0.50",
            // `memcpy` is attributed to the Databend function calling it.
            "site databend_join 3 9 0.25",
        ]);
    }

    #[test]
    fn test_stack_site() {
        let frames = |frames: &[&str]| frames.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            stack_site(&frames(&["main", "<databend_join>::probe", "memcpy"])),
            Some("<databend_join>::probe")
        );
        assert_eq!(stack_site(&frames(&["main", "memcpy"])), Some("memcpy"));
        assert_eq!(stack_site(&[]), None);
    }

    #[test]
    fn test_cpu_folded_stacks() {
        let stacks = vec![
            stack(&["main", "a"], 1),
            stack(&["main", "b<[u8; 8]>"], 3),
            stack(&["main", "a"], 1),
        ];

        assert_eq!(cpu_folded_stacks(&stacks, None), vec![
            ("worker;main;b<[u8, 8]>".to_string(), 3),
            ("worker;main;a".to_string(), 2),
        ]);
        assert_eq!(cpu_folded_stacks(&stacks, Some(1)).len(), 1);
    }

    #[test]
    fn test_prefix_folded_by_node() {
        let lines = prefix_folded_by_node(
            vec![
                ("n1".to_string(), vec![("a;b".to_string(), 3)]),
                ("n2".to_string(), vec![
                    ("a;c".to_string(), 5),
                    ("a".to_string(), 1),
                ]),
            ],
            Some(2),
        );
        assert_eq!(lines, vec![
            ("n2;a;c".to_string(), 5),
            ("n1;a;b".to_string(), 3)
        ]);
    }

    #[test]
    fn test_perf_samples_roundtrip() {
        let samples = PerfSamples {
            cpu: vec![stack(&["main", "a"], 2)],
            memory: vec![AllocStack {
                plan: Some((3, "HashJoin".to_string())),
                frames: vec!["main".to_string()],
                bytes: 1024,
            }],
        };
        let json = serde_json::to_vec(&samples).unwrap();
        let decoded: PerfSamples = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.cpu, samples.cpu);
        assert_eq!(decoded.memory, samples.memory);
        assert!(!decoded.is_empty());
    }
}
