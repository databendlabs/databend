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

/// A hardware or software performance event that can be measured via perf_event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum PerfEvent {
    // Hardware events
    CpuCycles,
    Instructions,
    CacheMisses,
    BranchMisses,
    CacheReferences,
    L1dReadMisses,
    L1dReadAccesses,
    L1dWriteMisses,
    LlReadMisses,
    LlWriteMisses,
    DtlbReadMisses,
    ItlbReadMisses,
    BpuReadMisses,
    BranchInstructions,
    BusCycles,
    StalledCyclesFrontend,
    StalledCyclesBackend,
    RefCycles,
    // Software events
    PageFaults,
    ContextSwitches,
    CpuMigrations,
    MinorFaults,
    MajorFaults,
    TaskClock,
}

/// (name, variant, display_name)
const EVENT_TABLE: &[(&str, PerfEvent, &str)] = &[
    ("cycles", PerfEvent::CpuCycles, "CPU Cycles"),
    ("instructions", PerfEvent::Instructions, "Instructions"),
    ("cache-misses", PerfEvent::CacheMisses, "Cache Misses"),
    ("branch-misses", PerfEvent::BranchMisses, "Branch Misses"),
    ("cache-refs", PerfEvent::CacheReferences, "Cache Refs"),
    (
        "l1d-read-misses",
        PerfEvent::L1dReadMisses,
        "L1D Read Misses",
    ),
    (
        "l1d-read-accesses",
        PerfEvent::L1dReadAccesses,
        "L1D Read Accesses",
    ),
    (
        "l1d-write-misses",
        PerfEvent::L1dWriteMisses,
        "L1D Write Misses",
    ),
    ("ll-read-misses", PerfEvent::LlReadMisses, "LL Read Misses"),
    (
        "ll-write-misses",
        PerfEvent::LlWriteMisses,
        "LL Write Misses",
    ),
    (
        "dtlb-read-misses",
        PerfEvent::DtlbReadMisses,
        "DTLB Read Misses",
    ),
    (
        "itlb-read-misses",
        PerfEvent::ItlbReadMisses,
        "ITLB Read Misses",
    ),
    (
        "bpu-read-misses",
        PerfEvent::BpuReadMisses,
        "BPU Read Misses",
    ),
    (
        "branch-instructions",
        PerfEvent::BranchInstructions,
        "Branch Instructions",
    ),
    ("bus-cycles", PerfEvent::BusCycles, "Bus Cycles"),
    (
        "stalled-cycles-frontend",
        PerfEvent::StalledCyclesFrontend,
        "Stalled Cycles (Frontend)",
    ),
    (
        "stalled-cycles-backend",
        PerfEvent::StalledCyclesBackend,
        "Stalled Cycles (Backend)",
    ),
    ("ref-cycles", PerfEvent::RefCycles, "Ref Cycles"),
    ("page-faults", PerfEvent::PageFaults, "Page Faults"),
    (
        "context-switches",
        PerfEvent::ContextSwitches,
        "Context Switches",
    ),
    ("cpu-migrations", PerfEvent::CpuMigrations, "CPU Migrations"),
    ("minor-faults", PerfEvent::MinorFaults, "Minor Faults"),
    ("major-faults", PerfEvent::MajorFaults, "Major Faults"),
    ("task-clock", PerfEvent::TaskClock, "Task Clock"),
];

impl PerfEvent {
    /// Parse a user-facing event name (e.g. "cycles", "cache-misses") into a `PerfEvent`.
    pub fn from_name(name: &str) -> Option<Self> {
        EVENT_TABLE
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(_, e, _)| *e)
    }

    pub fn display_name(&self) -> &'static str {
        EVENT_TABLE
            .iter()
            .find(|(_, e, _)| e == self)
            .map(|(_, _, d)| *d)
            .unwrap_or("Unknown")
    }

    /// All valid user-facing event names.
    pub fn all_names() -> impl Iterator<Item = &'static str> {
        EVENT_TABLE.iter().map(|(n, _, _)| *n)
    }

    /// The default set of events used when none are specified.
    /// Each event is in its own group (standalone) by default.
    pub fn default_groups() -> Vec<Vec<Self>> {
        vec![
            vec![Self::CpuCycles, Self::Instructions],
            vec![Self::CacheMisses, Self::CacheReferences],
            vec![Self::BranchMisses],
        ]
    }
}

/// A single perf event measurement with per-event multiplexing info.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PerfValue {
    pub count: u64,
    pub multiplexed: bool,
}

#[cfg(target_os = "linux")]
impl PerfEvent {
    fn to_builder(self) -> perf_event::Builder<'static> {
        use perf_event::Builder;
        use perf_event::events::Cache;
        use perf_event::events::CacheId;
        use perf_event::events::CacheOp;
        use perf_event::events::CacheResult;
        use perf_event::events::Hardware;
        use perf_event::events::Software;

        match self {
            Self::CpuCycles => Builder::new(Hardware::CPU_CYCLES),
            Self::Instructions => Builder::new(Hardware::INSTRUCTIONS),
            Self::CacheMisses => Builder::new(Hardware::CACHE_MISSES),
            Self::BranchMisses => Builder::new(Hardware::BRANCH_MISSES),
            Self::CacheReferences => Builder::new(Hardware::CACHE_REFERENCES),
            Self::BranchInstructions => Builder::new(Hardware::BRANCH_INSTRUCTIONS),
            Self::BusCycles => Builder::new(Hardware::BUS_CYCLES),
            Self::StalledCyclesFrontend => Builder::new(Hardware::STALLED_CYCLES_FRONTEND),
            Self::StalledCyclesBackend => Builder::new(Hardware::STALLED_CYCLES_BACKEND),
            Self::RefCycles => Builder::new(Hardware::REF_CPU_CYCLES),
            Self::L1dReadMisses => Builder::new(Cache {
                which: CacheId::L1D,
                operation: CacheOp::READ,
                result: CacheResult::MISS,
            }),
            Self::L1dReadAccesses => Builder::new(Cache {
                which: CacheId::L1D,
                operation: CacheOp::READ,
                result: CacheResult::ACCESS,
            }),
            Self::L1dWriteMisses => Builder::new(Cache {
                which: CacheId::L1D,
                operation: CacheOp::WRITE,
                result: CacheResult::MISS,
            }),
            Self::LlReadMisses => Builder::new(Cache {
                which: CacheId::LL,
                operation: CacheOp::READ,
                result: CacheResult::MISS,
            }),
            Self::LlWriteMisses => Builder::new(Cache {
                which: CacheId::LL,
                operation: CacheOp::WRITE,
                result: CacheResult::MISS,
            }),
            Self::DtlbReadMisses => Builder::new(Cache {
                which: CacheId::DTLB,
                operation: CacheOp::READ,
                result: CacheResult::MISS,
            }),
            Self::ItlbReadMisses => Builder::new(Cache {
                which: CacheId::ITLB,
                operation: CacheOp::READ,
                result: CacheResult::MISS,
            }),
            Self::BpuReadMisses => Builder::new(Cache {
                which: CacheId::BPU,
                operation: CacheOp::READ,
                result: CacheResult::MISS,
            }),
            Self::PageFaults => Builder::new(Software::PAGE_FAULTS),
            Self::ContextSwitches => Builder::new(Software::CONTEXT_SWITCHES),
            Self::CpuMigrations => Builder::new(Software::CPU_MIGRATIONS),
            Self::MinorFaults => Builder::new(Software::PAGE_FAULTS_MIN),
            Self::MajorFaults => Builder::new(Software::PAGE_FAULTS_MAJ),
            Self::TaskClock => Builder::new(Software::TASK_CLOCK),
        }
    }
}

#[cfg(target_os = "linux")]
mod target_impl {
    use std::io;

    use perf_event::Counter;
    use perf_event::Group;

    use super::PerfEvent;

    /// Per-thread hardware performance counters supporting mixed grouped and
    /// standalone modes.
    ///
    /// - Multi-event groups: events are atomically scheduled together by the
    ///   kernel, making ratios between them meaningful (e.g. IPC).
    /// - Standalone counters: each event runs independently, avoiding the
    ///   kernel's group size limit.
    ///
    /// Created once per executor worker thread and reused across processor
    /// executions via reset→enable→process→disable→read.
    pub struct PerfCounters {
        /// Multi-event groups (len > 1 events each).
        groups: Vec<(Group, Vec<(PerfEvent, Counter)>)>,
        /// Standalone counters (single events, not in any group).
        standalone: Vec<(PerfEvent, Counter)>,
    }

    impl PerfCounters {
        /// Try to create perf counters for the given event groups.
        /// Each inner slice with len > 1 becomes a perf_event Group;
        /// single-element slices become standalone Counters.
        /// Returns `None` if the kernel denies access or any counter fails.
        pub fn try_new(event_groups: &[Vec<PerfEvent>]) -> Option<Self> {
            let mut groups = Vec::new();
            let mut standalone = Vec::new();

            for group_events in event_groups {
                if group_events.len() > 1 {
                    let mut group = Group::new().ok()?;
                    let mut counters = Vec::with_capacity(group_events.len());
                    for &event in group_events {
                        let counter = group.add(&event.to_builder()).ok()?;
                        counters.push((event, counter));
                    }
                    groups.push((group, counters));
                } else if let Some(&event) = group_events.first() {
                    let counter = event.to_builder().build().ok()?;
                    standalone.push((event, counter));
                }
            }

            Some(PerfCounters { groups, standalone })
        }

        /// Reset all counters to zero and enable them.
        pub fn reset_and_enable(&mut self) -> io::Result<()> {
            for (group, _) in &mut self.groups {
                group.reset()?;
                group.enable()?;
            }
            for (_, counter) in &mut self.standalone {
                counter.reset()?;
                counter.enable()?;
            }
            Ok(())
        }

        /// Disable all counters and read values.
        /// Returns per-event `(event, count, multiplexed)` triples.
        pub fn disable_and_read(&mut self) -> io::Result<Vec<(PerfEvent, u64, bool)>> {
            let mut results = Vec::new();

            for (group, counters) in &mut self.groups {
                group.disable()?;
                let counts = group.read()?;
                let multiplexed = match (counts.time_enabled(), counts.time_running()) {
                    (Some(enabled), Some(running)) => running < enabled,
                    _ => false,
                };
                for (event, counter) in counters.iter() {
                    results.push((*event, counts[counter], multiplexed));
                }
            }

            for (event, counter) in &mut self.standalone {
                counter.disable()?;
                let cat = counter.read_count_and_time()?;
                let multiplexed = cat.time_running < cat.time_enabled;
                results.push((*event, cat.count, multiplexed));
            }

            Ok(results)
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod target_impl {
    use super::PerfEvent;

    /// Stub implementation for non-Linux platforms. All operations are no-ops.
    pub struct PerfCounters;

    impl PerfCounters {
        pub fn try_new(_event_groups: &[Vec<PerfEvent>]) -> Option<Self> {
            None
        }

        pub fn reset_and_enable(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        pub fn disable_and_read(&mut self) -> std::io::Result<Vec<(PerfEvent, u64, bool)>> {
            Ok(vec![])
        }
    }
}

pub use target_impl::PerfCounters;

fn default_frequency() -> i32 {
    99
}

/// Unified configuration for EXPLAIN PERF: profiler (flamegraph) + hw counters.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PerfConfig {
    pub profiler_enabled: bool,
    pub event_groups: Vec<Vec<PerfEvent>>,
    #[serde(default = "default_frequency")]
    pub frequency: i32,
}

impl PerfConfig {
    /// True if either the CPU profiler or hw counters are active.
    pub fn is_perf_active(&self) -> bool {
        self.profiler_enabled || !self.event_groups.is_empty()
    }

    /// True if hardware performance counters are requested.
    pub fn has_hw_counters(&self) -> bool {
        !self.event_groups.is_empty()
    }

    /// Flatten all event groups into a single ordered list of events.
    pub fn all_events(&self) -> Vec<PerfEvent> {
        self.event_groups.iter().flatten().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_table_covers_all_variants() {
        assert_eq!(
            EVENT_TABLE.len(),
            std::mem::variant_count::<PerfEvent>(),
            "EVENT_TABLE has {} entries but PerfEvent has {} variants — add the missing entry",
            EVENT_TABLE.len(),
            std::mem::variant_count::<PerfEvent>(),
        );
    }
}
