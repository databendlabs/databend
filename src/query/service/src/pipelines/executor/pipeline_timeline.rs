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

//! A timeline of where the processors of one running graph spend their time.
//!
//! Test instrumentation, enabled by `pipeline_timeline_interval_ms`. Time is cut
//! into buckets of that length. For every processor it records how long it was in
//! each [`TimelineState`] within each bucket, and a sampler thread records, at the
//! end of each bucket, how much every profile statistic of the processor grew in
//! it. Processors of the same plan node with the same name are summed into one
//! group. When the graph is dropped the timeline is logged at INFO, so it reaches
//! `system_history.log_history`: one `[PIPELINE-TIMELINE]` line per group and per
//! chunk of at most [`BUCKETS_PER_LINE`] buckets, the group as JSON at the end.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadJoinHandle;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::get_statistics_desc;
use log::info;
use parking_lot::Mutex;
use serde::Serialize;

/// What a processor is doing, as the executor sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimelineState {
    /// Its last event was `NeedData`: it waits for its inputs.
    NeedData = 0,
    /// Its last event was `NeedConsume`: it waits for its outputs to be taken.
    NeedConsume = 1,
    /// Its last event was `Sync`, and no worker runs it yet.
    SyncQueued = 2,
    /// A worker runs its `process`.
    SyncRunning = 3,
    /// Its last event was `Async`: its `async_process` has not completed.
    Async = 4,
    /// The executor runs its `event`, which some processors use for real work,
    /// such as the scatter and encoding of an exchange sender.
    InEvent = 5,
    /// The executor has not run its `event` yet, e.g. the probe side of a join
    /// while the build side is not finished.
    Unscheduled = 6,
}

const STATES: usize = 7;
const STATE_NAMES: [&str; STATES] = [
    "NeedData",
    "NeedConsume",
    "SyncQueued",
    "SyncRunning",
    "Async",
    "InEvent",
    "Unscheduled",
];

/// Buckets in one log line, so a long query does not make lines of megabytes.
const BUCKETS_PER_LINE: usize = 600;

static GRAPH_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
pub struct TimelineConfig {
    pub interval: Duration,
    pub query_id: String,
    pub node_id: String,
    pub max_threads: u64,
}

struct Clock {
    start: Instant,
    interval_ns: u64,
}

impl Clock {
    fn now(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

struct NodeTimelineInner {
    /// `None` after `Finished`.
    state: Option<TimelineState>,
    since: u64,
    /// Nanoseconds in each state, per bucket.
    buckets: Vec<[u64; STATES]>,
}

impl NodeTimelineInner {
    fn accrue(&mut self, interval_ns: u64, now: u64) {
        let Some(state) = self.state else {
            self.since = now;
            return;
        };
        let mut since = self.since;
        while since < now {
            let bucket = (since / interval_ns) as usize;
            let end = now.min((bucket as u64 + 1) * interval_ns);
            if self.buckets.len() <= bucket {
                self.buckets.resize(bucket + 1, [0; STATES]);
            }
            self.buckets[bucket][state as usize] += end - since;
            since = end;
        }
        self.since = now;
    }
}

pub struct NodeTimeline {
    clock: Arc<Clock>,
    inner: Mutex<NodeTimelineInner>,
}

impl NodeTimeline {
    /// The processor is now in `state`; `None` once it finished.
    pub fn enter(&self, state: Option<TimelineState>) {
        let now = self.clock.now();
        let mut inner = self.inner.lock();
        inner.accrue(self.clock.interval_ns, now);
        inner.state = state;
    }

    fn close(&self, now: u64) -> Vec<[u64; STATES]> {
        let mut inner = self.inner.lock();
        inner.accrue(self.clock.interval_ns, now);
        inner.state = None;
        std::mem::take(&mut inner.buckets)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    plan_id: Option<u32>,
    plan_name: String,
    processor: String,
}

struct Group {
    key: GroupKey,
    title: String,
    processors: usize,
    /// Growth of each statistic that ever grew, per bucket.
    stats: BTreeMap<usize, Vec<u64>>,
}

struct SampledNode {
    group: usize,
    profile: Arc<Profile>,
    last: Vec<usize>,
}

struct Shared {
    clock: Arc<Clock>,
    stop: AtomicBool,
    groups: Mutex<Vec<Group>>,
    sampled: Mutex<Vec<SampledNode>>,
}

impl Shared {
    /// Adds what every statistic grew since the last sample to `bucket`.
    fn sample(&self, bucket: usize) {
        let mut sampled = self.sampled.lock();
        let mut groups = self.groups.lock();
        for node in sampled.iter_mut() {
            let group = &mut groups[node.group];
            for (index, last) in node.last.iter_mut().enumerate() {
                let value = node.profile.statistics[index].load(Ordering::Relaxed);
                let grown = value.saturating_sub(*last);
                *last = value;
                if grown == 0 {
                    continue;
                }
                let series = group.stats.entry(index).or_default();
                if series.len() <= bucket {
                    series.resize(bucket + 1, 0);
                }
                series[bucket] += grown as u64;
            }
        }
    }

    fn run_sampler(&self) {
        let interval_ns = self.clock.interval_ns;
        loop {
            let now = self.clock.now();
            let bucket = now / interval_ns;
            let next = (bucket + 1) * interval_ns;
            std::thread::park_timeout(Duration::from_nanos(next.saturating_sub(now)));
            let now = self.clock.now();
            // What grew until now is put in the bucket the sample closes.
            self.sample((now.saturating_sub(1) / interval_ns) as usize);
            if self.stop.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

/// The timeline of one running graph.
pub struct GraphTimeline {
    config: TimelineConfig,
    seq: usize,
    start_unix_ms: u64,
    shared: Arc<Shared>,
    nodes: Vec<(usize, Arc<NodeTimeline>)>,
    sampler: Option<ThreadJoinHandle<()>>,
}

impl GraphTimeline {
    pub fn create(config: TimelineConfig) -> GraphTimeline {
        let interval_ns = (config.interval.as_nanos() as u64).max(1_000_000);
        let clock = Arc::new(Clock {
            start: Instant::now(),
            interval_ns,
        });
        let start_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        GraphTimeline {
            config,
            seq: GRAPH_SEQ.fetch_add(1, Ordering::Relaxed),
            start_unix_ms,
            shared: Arc::new(Shared {
                clock,
                stop: AtomicBool::new(false),
                groups: Mutex::new(vec![]),
                sampled: Mutex::new(vec![]),
            }),
            nodes: vec![],
            sampler: None,
        }
    }

    /// Registers a processor; the timeline it records its states into.
    pub fn register(&mut self, profile: &Arc<Profile>) -> Arc<NodeTimeline> {
        let key = GroupKey {
            plan_id: profile.plan_id,
            plan_name: profile.plan_name.clone().unwrap_or_default(),
            processor: profile.p_name.clone(),
        };
        let mut groups = self.shared.groups.lock();
        let group = match groups.iter().position(|group| group.key == key) {
            Some(group) => group,
            None => {
                groups.push(Group {
                    key,
                    title: profile.title.to_string(),
                    processors: 0,
                    stats: BTreeMap::new(),
                });
                groups.len() - 1
            }
        };
        groups[group].processors += 1;
        drop(groups);

        self.shared.sampled.lock().push(SampledNode {
            group,
            profile: profile.clone(),
            last: vec![0; profile.statistics.len()],
        });
        let node = Arc::new(NodeTimeline {
            clock: self.shared.clock.clone(),
            inner: Mutex::new(NodeTimelineInner {
                state: Some(TimelineState::Unscheduled),
                since: 0,
                buckets: vec![],
            }),
        });
        self.nodes.push((group, node.clone()));
        node
    }

    /// Starts sampling the statistics of the registered processors.
    pub fn start(&mut self) {
        let shared = self.shared.clone();
        self.sampler = Some(Thread::named_spawn(
            Some("pipeline-timeline".to_string()),
            move || shared.run_sampler(),
        ));
    }

    fn finish(&mut self) -> TimelineReport {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(sampler) = self.sampler.take() {
            sampler.thread().unpark();
            let _ = sampler.join();
        }
        let now = self.shared.clock.now();
        let interval_ns = self.shared.clock.interval_ns;
        let buckets = (now / interval_ns + 1) as usize;

        let groups = std::mem::take(&mut *self.shared.groups.lock());
        let mut states = vec![vec![[0u64; STATES]; buckets]; groups.len()];
        for (group, node) in &self.nodes {
            for (bucket, times) in node.close(now).iter().enumerate().take(buckets) {
                for state in 0..STATES {
                    states[*group][bucket][state] += times[state];
                }
            }
        }

        let desc = get_statistics_desc();
        let stat_names: HashMap<usize, String> = desc
            .iter()
            .map(|(name, desc)| (desc.index, format!("{name:?}")))
            .collect();

        let groups = groups
            .into_iter()
            .zip(states)
            .map(|(group, states)| {
                let state_us = STATE_NAMES
                    .iter()
                    .enumerate()
                    .filter_map(|(state, name)| {
                        let series: Vec<u64> = states.iter().map(|b| b[state] / 1_000).collect();
                        series
                            .iter()
                            .any(|v| *v > 0)
                            .then(|| (name.to_string(), series))
                    })
                    .collect();
                let stats = group
                    .stats
                    .into_iter()
                    .map(|(index, mut series)| {
                        series.resize(buckets, 0);
                        let name = stat_names
                            .get(&index)
                            .cloned()
                            .unwrap_or_else(|| index.to_string());
                        (name, series)
                    })
                    .collect();
                GroupReport {
                    plan_id: group.key.plan_id,
                    plan_name: group.key.plan_name,
                    processor: group.key.processor,
                    title: group.title,
                    processors: group.processors,
                    state_us,
                    stats,
                }
            })
            .collect();

        TimelineReport {
            query_id: self.config.query_id.clone(),
            node_id: self.config.node_id.clone(),
            graph_seq: self.seq,
            start_unix_ms: self.start_unix_ms,
            interval_us: interval_ns / 1_000,
            buckets,
            max_threads: self.config.max_threads,
            groups,
        }
    }
}

impl Drop for GraphTimeline {
    fn drop(&mut self) {
        let report = self.finish();
        for group in &report.groups {
            for offset in (0..report.buckets.max(1)).step_by(BUCKETS_PER_LINE) {
                let Ok(line) = serde_json::to_string(&group.slice(offset, BUCKETS_PER_LINE)) else {
                    continue;
                };
                info!(
                    "[PIPELINE-TIMELINE] query_id={} node_id={} graph={} start_unix_ms={} interval_us={} max_threads={} buckets={} offset={} group={}",
                    report.query_id,
                    report.node_id,
                    report.graph_seq,
                    report.start_unix_ms,
                    report.interval_us,
                    report.max_threads,
                    report.buckets,
                    offset,
                    line
                );
            }
        }
    }
}

#[derive(Serialize)]
struct GroupReport {
    plan_id: Option<u32>,
    plan_name: String,
    processor: String,
    title: String,
    processors: usize,
    /// Microseconds the processors of the group spent in each state, per bucket.
    state_us: BTreeMap<String, Vec<u64>>,
    /// Growth of each profile statistic, per bucket.
    stats: BTreeMap<String, Vec<u64>>,
}

struct TimelineReport {
    query_id: String,
    node_id: String,
    graph_seq: usize,
    start_unix_ms: u64,
    interval_us: u64,
    buckets: usize,
    max_threads: u64,
    groups: Vec<GroupReport>,
}

impl GroupReport {
    /// The group with only the buckets from `offset`, at most `len` of them.
    fn slice(&self, offset: usize, len: usize) -> GroupReport {
        let cut = |series: &BTreeMap<String, Vec<u64>>| {
            series
                .iter()
                .map(|(name, values)| {
                    let end = values.len().min(offset + len);
                    let part = values.get(offset..end).unwrap_or_default().to_vec();
                    (name.clone(), part)
                })
                .collect()
        };
        GroupReport {
            plan_id: self.plan_id,
            plan_name: self.plan_name.clone(),
            processor: self.processor.clone(),
            title: self.title.clone(),
            processors: self.processors,
            state_us: cut(&self.state_us),
            stats: cut(&self.stats),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_time_is_split_at_bucket_edges() {
        let mut inner = NodeTimelineInner {
            state: Some(TimelineState::SyncRunning),
            since: 50,
            buckets: vec![],
        };
        inner.accrue(100, 250);
        assert_eq!(inner.buckets.len(), 3);
        assert_eq!(inner.buckets[0][TimelineState::SyncRunning as usize], 50);
        assert_eq!(inner.buckets[1][TimelineState::SyncRunning as usize], 100);
        assert_eq!(inner.buckets[2][TimelineState::SyncRunning as usize], 50);

        inner.state = None;
        inner.accrue(100, 400);
        assert_eq!(inner.buckets.len(), 3);
        assert_eq!(inner.since, 400);
    }

    #[test]
    fn test_log_lines_carry_their_part_of_each_series() {
        let group = GroupReport {
            plan_id: Some(1),
            plan_name: "Scan".to_string(),
            processor: "Read".to_string(),
            title: String::new(),
            processors: 2,
            state_us: BTreeMap::from([("Async".to_string(), vec![1, 2, 3, 4, 5])]),
            stats: BTreeMap::from([("ScanBytes".to_string(), vec![7, 8])]),
        };
        let second = group.slice(2, 2);
        assert_eq!(second.state_us["Async"], vec![3, 4]);
        assert!(second.stats["ScanBytes"].is_empty());
        assert_eq!(group.slice(4, 2).state_us["Async"], vec![5]);
    }
}
