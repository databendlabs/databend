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

//! Tabular views of the CPU samples of `EXPLAIN PERF CPU`.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// The heaviest call path to the function, see [`heaviest_path`]. `None` for the summary row.
    pub path: Option<String>,
    pub self_samples: u64,
    pub total_samples: u64,
    /// The share of all samples by the self samples.
    pub share: f64,
}

/// Whether the frame is a function of Databend, possibly a method `<Type>::method`.
pub fn is_databend_frame(frame: &str) -> bool {
    frame.trim_start_matches('<').starts_with("databend_")
}

/// The index of the site of a stack in summaries: its innermost Databend frame, which tells which
/// Databend code runs the standard library and third party code above it. Falls back to the
/// innermost frame when the stack has no Databend frame. `frames` are outermost first.
pub(crate) fn site_index(frames: &[String]) -> Option<usize> {
    frames
        .iter()
        .rposition(|frame| is_databend_frame(frame))
        .or(frames.len().checked_sub(1))
}

/// The number of Databend callers in the path of a summary row.
pub const PATH_DEPTH: usize = 3;

/// The Databend callers of `frames[index]`, innermost first and at most [`PATH_DEPTH`], joined by
/// ` <- `. Callers repeating the function or the previous caller, e.g. recursion, are skipped.
pub(crate) fn caller_path(frames: &[String], index: usize) -> String {
    let function = frames[index].as_str();
    let mut callers: Vec<&str> = Vec::with_capacity(PATH_DEPTH);
    for frame in frames[..index].iter().rev() {
        if callers.len() == PATH_DEPTH {
            break;
        }
        if is_databend_frame(frame) && frame != function && callers.last() != Some(&frame.as_str())
        {
            callers.push(frame);
        }
    }
    callers.join(" <- ")
}

/// The call path with the most weight among the `paths` of a row, followed by its share of the
/// row when the row is reached through other paths too. `None` without callers.
pub(crate) fn heaviest_path(paths: &HashMap<String, u64>) -> Option<String> {
    let total = paths.values().sum::<u64>();
    let (path, weight) = paths
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)))?;
    if path.is_empty() {
        return None;
    }
    match *weight < total {
        true => Some(format!(
            "{path} ({:.0}% of the row)",
            *weight as f64 * 100.0 / total as f64
        )),
        false => Some(path.clone()),
    }
}

/// Summarizes `stacks` into a summary row, the `limit` functions with the most self samples, and
/// the `limit` sites with the most self samples, see [`site_index`].
///
/// The function rows show the leaf code that burns the CPU, often the standard library. The site
/// rows attribute the same samples to the Databend code running it. The total samples of a row
/// include the callees of its function. The path of a row is its heaviest chain of Databend
/// callers, see [`caller_path`].
pub fn summarize_cpu_stacks(stacks: &[CpuStack], limit: usize) -> Vec<CpuSummaryRow> {
    let all = stacks.iter().map(|stack| stack.samples).sum::<u64>();

    let mut functions: HashMap<&str, (u64, u64)> = HashMap::new();
    let mut sites: HashMap<&str, u64> = HashMap::new();
    let mut paths: HashMap<(CpuSummaryLevel, &str), HashMap<String, u64>> = HashMap::new();
    for stack in stacks {
        if let Some(leaf) = stack.frames.last() {
            functions.entry(leaf).or_default().0 += stack.samples;
            let path = caller_path(&stack.frames, stack.frames.len() - 1);
            *paths
                .entry((CpuSummaryLevel::Function, leaf))
                .or_default()
                .entry(path)
                .or_default() += stack.samples;
        }
        if let Some(index) = site_index(&stack.frames) {
            let site = stack.frames[index].as_str();
            *sites.entry(site).or_default() += stack.samples;
            let path = caller_path(&stack.frames, index);
            *paths
                .entry((CpuSummaryLevel::Site, site))
                .or_default()
                .entry(path)
                .or_default() += stack.samples;
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
        path: None,
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
                    path: paths.get(&(level, *function)).and_then(heaviest_path),
                    self_samples: *self_samples,
                    total_samples: functions.get(function).map_or(0, |x| x.1),
                    share: share(*self_samples),
                }),
        );
    }
    rows
}

/// Folds `stacks` into `thread;frame;...;frame` lines with their samples, the input of the
/// flamegraph, the largest `limit` stacks first.
fn cpu_folded_stacks(stacks: &[CpuStack], limit: Option<usize>) -> Vec<(String, u64)> {
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
    // The flamegraph renderer fails without stacks, e.g. for a statement too short to be sampled.
    if lines.is_empty() {
        return Ok("<p>No samples were taken.</p>".to_string());
    }

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
    fn test_flamegraph_without_samples() {
        assert!(cpu_flamegraph(&[], "cpu").unwrap().contains("No samples"));
        assert!(
            crate::runtime::alloc_flamegraph(&[], "memory")
                .unwrap()
                .contains("No samples")
        );
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

        let paths = summarize_cpu_stacks(&stacks, 2)
            .into_iter()
            .map(|row| row.path)
            .collect::<Vec<_>>();
        assert_eq!(paths, vec![
            None,
            Some("databend_join".to_string()),
            // `memcpy` is reached through two paths.
            Some("databend_join (75% of the row)".to_string()),
            Some("databend_join".to_string()),
            // `main` is not a Databend frame.
            None,
        ]);
    }

    #[test]
    fn test_caller_path() {
        let frames = |frames: &[&str]| frames.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let stack = frames(&[
            "main",
            "databend_a",
            "databend_b",
            "databend_b",
            "std::iter",
            "databend_c",
            "databend_d",
            "databend_d",
            "memcpy",
        ]);
        // Repeated callers and the function itself are skipped, up to `PATH_DEPTH` callers.
        assert_eq!(
            caller_path(&stack, 7),
            "databend_c <- databend_b <- databend_a"
        );
        assert_eq!(caller_path(&stack, 1), "");
    }

    #[test]
    fn test_site_index() {
        let frames = |frames: &[&str]| frames.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            site_index(&frames(&["main", "<databend_join>::probe", "memcpy"])),
            Some(1)
        );
        assert_eq!(site_index(&frames(&["main", "memcpy"])), Some(1));
        assert_eq!(site_index(&[]), None);
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
    fn test_perf_samples_roundtrip() {
        let samples = PerfSamples {
            cpu: vec![stack(&["main", "a"], 2)],
            memory: vec![AllocStack {
                query_id: None,
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
