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

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytesize::ByteSize;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;

const GLOBAL_PRESSURE_SLEEP_BACKOFF_INIT_MS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillDecision {
    NoSpill,
    SpillNow,
    Sleep(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillBackoffSettings {
    pub max_sleep_ms: u64,
    pub min_query_memory_usage: u64,
}

impl SpillBackoffSettings {
    fn should_backoff(&self, query_usage: usize) -> bool {
        query_usage <= self.min_query_memory_usage as usize
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpillBackoffState {
    pub consumed_sleep_ms: u64,
    pub attempts: u32,
}

impl SpillBackoffState {
    fn reset(&mut self) {
        self.consumed_sleep_ms = 0;
        self.attempts = 0;
    }

    fn next_sleep_ms(&mut self, max_sleep_ms: u64) -> Option<u64> {
        if max_sleep_ms == 0 {
            return None;
        }

        let remaining_budget = max_sleep_ms.saturating_sub(self.consumed_sleep_ms);
        if remaining_budget == 0 {
            return None;
        }

        let mut delay_ms = GLOBAL_PRESSURE_SLEEP_BACKOFF_INIT_MS;
        for _ in 0..self.attempts.min(16) {
            delay_ms = delay_ms.saturating_mul(2);
        }

        let actual_sleep_ms = delay_ms.min(remaining_budget);
        self.consumed_sleep_ms = self.consumed_sleep_ms.saturating_add(actual_sleep_ms);
        self.attempts = self.attempts.saturating_add(1);
        Some(actual_sleep_ms)
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct MemorySettings {
    pub spill_unit_size: usize,

    pub enable_global_level_spill: bool,
    pub max_memory_usage: usize,
    pub global_memory_tracking: &'static MemStat,

    pub enable_group_spill: bool,

    pub enable_query_level_spill: bool,
    pub max_query_memory_usage: usize,
    pub query_memory_tracking: Option<Arc<MemStat>>,

    pub spill_backoff: Option<SpillBackoffSettings>,
}

impl Debug for MemorySettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct Tracking<'a>(&'a MemStat);

        impl Debug for Tracking<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("MemStat")
                    .field("used", &ByteSize(self.0.get_memory_usage() as _))
                    .field("peak_used", &ByteSize(self.0.get_peak_memory_usage() as _))
                    .field("memory_limit", &self.0.get_limit())
                    .finish()
            }
        }

        let mut f = f.debug_struct("MemorySettings");
        let mut f = f
            .field("max_memory_usage", &ByteSize(self.max_memory_usage as _))
            .field("enable_global_level_spill", &self.enable_global_level_spill)
            .field(
                "global_memory_tracking",
                &Tracking(self.global_memory_tracking),
            )
            .field(
                "max_query_memory_usage",
                &ByteSize(self.max_query_memory_usage as _),
            );

        if let Some(tracking) = &self.query_memory_tracking {
            f = f.field("query_memory_tracking", &Tracking(tracking));
        }

        f.field("enable_query_level_spill", &self.enable_query_level_spill)
            .field("spill_backoff", &self.spill_backoff)
            .field("spill_unit_size", &self.spill_unit_size)
            .finish()
    }
}

pub struct MemorySettingsBuilder {
    max_memory_usage: Option<usize>,

    enable_group_spill: bool,

    max_query_memory_usage: Option<usize>,
    query_memory_tracking: Option<Arc<MemStat>>,

    spill_backoff: Option<SpillBackoffSettings>,

    spill_unit_size: Option<usize>,
}

impl MemorySettingsBuilder {
    pub fn with_max_memory_usage(mut self, max: usize) -> Self {
        self.max_memory_usage = Some(max);
        self
    }

    pub fn with_max_query_memory_usage(
        mut self,
        max: usize,
        tracking: Option<Arc<MemStat>>,
    ) -> Self {
        self.max_query_memory_usage = Some(max);
        self.query_memory_tracking = tracking;
        self
    }

    pub fn with_workload_group(mut self, enable: bool) -> Self {
        self.enable_group_spill = enable;
        self
    }

    pub fn with_spill_unit_size(mut self, spill_unit_size: usize) -> Self {
        self.spill_unit_size = Some(spill_unit_size);
        self
    }

    pub fn with_spill_backoff(mut self, spill_backoff: Option<SpillBackoffSettings>) -> Self {
        self.spill_backoff = spill_backoff;
        self
    }

    pub fn build(self) -> MemorySettings {
        MemorySettings {
            enable_group_spill: self.enable_group_spill,

            enable_global_level_spill: self.max_memory_usage.is_some(),
            max_memory_usage: self.max_memory_usage.unwrap_or(usize::MAX),
            global_memory_tracking: &GLOBAL_MEM_STAT,

            enable_query_level_spill: self.max_query_memory_usage.is_some(),
            max_query_memory_usage: self.max_query_memory_usage.unwrap_or(usize::MAX),
            query_memory_tracking: self.query_memory_tracking,

            spill_backoff: self.spill_backoff,

            spill_unit_size: self.spill_unit_size.unwrap_or(0),
        }
    }
}

impl MemorySettings {
    pub fn builder() -> MemorySettingsBuilder {
        MemorySettingsBuilder {
            max_memory_usage: None,

            enable_group_spill: true,

            max_query_memory_usage: None,
            query_memory_tracking: None,

            spill_backoff: None,

            spill_unit_size: None,
        }
    }

    pub fn check_spill(&self) -> bool {
        if let Some(remain) = self.check_global()
            && remain <= 0
        {
            return true;
        }

        if let Some(remain) = self.check_workload_group()
            && remain <= 0
        {
            return true;
        }

        if let Some(remain) = self.check_query()
            && remain <= 0
        {
            true
        } else {
            false
        }
    }

    pub fn check_spill_with_backoff(&self, backoff_state: &mut SpillBackoffState) -> SpillDecision {
        let decision = (|| {
            if !self.check_spill() {
                return SpillDecision::NoSpill;
            }

            let Some(spill_backoff) = self.spill_backoff else {
                return SpillDecision::SpillNow;
            };

            let Some(query_usage) = self.current_query_usage() else {
                return SpillDecision::SpillNow;
            };

            if !spill_backoff.should_backoff(query_usage) {
                return SpillDecision::SpillNow;
            }

            match backoff_state.next_sleep_ms(spill_backoff.max_sleep_ms) {
                Some(sleep_ms) => SpillDecision::Sleep(sleep_ms),
                None => SpillDecision::SpillNow,
            }
        })();

        if !matches!(decision, SpillDecision::Sleep(_)) {
            backoff_state.reset();
        }
        decision
    }

    fn check_global(&self) -> Option<isize> {
        self.enable_global_level_spill.then(|| {
            let usage = self.global_memory_tracking.get_memory_usage();
            if usage >= self.max_memory_usage {
                -((usage - self.max_memory_usage) as isize)
            } else {
                (self.max_memory_usage - usage) as isize
            }
        })
    }

    fn check_workload_group(&self) -> Option<isize> {
        if !self.enable_group_spill {
            return None;
        }

        let workload_group = ThreadTracker::workload_group()?;
        let usage = workload_group.mem_stat.get_memory_usage();
        let max_memory_usage = workload_group.max_memory_usage.load(Ordering::Relaxed);

        if max_memory_usage == 0 {
            return None;
        }

        Some(if usage >= max_memory_usage {
            -((usage - max_memory_usage) as isize)
        } else {
            (max_memory_usage - usage) as isize
        })
    }

    fn check_query(&self) -> Option<isize> {
        if !self.enable_query_level_spill {
            return None;
        }

        let usage = self.current_query_usage()?;

        Some(if usage >= self.max_query_memory_usage {
            -((usage - self.max_query_memory_usage) as isize)
        } else {
            (self.max_query_memory_usage - usage) as isize
        })
    }

    pub fn check_spill_remain(&self) -> Option<isize> {
        [
            self.check_global(),
            self.check_workload_group(),
            self.check_query(),
        ]
        .into_iter()
        .flatten()
        .reduce(|a, b| a.min(b))
    }

    fn current_query_usage(&self) -> Option<usize> {
        self.query_memory_tracking
            .as_ref()
            .map(|tracking| tracking.get_memory_usage())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_base::uniq_id::GlobalUniq;

    use super::*;

    impl Default for MemorySettings {
        fn default() -> Self {
            Self {
                enable_group_spill: true,
                max_memory_usage: usize::MAX,
                enable_global_level_spill: false,
                global_memory_tracking: create_static_mem_stat(0),
                max_query_memory_usage: 0,
                query_memory_tracking: None,
                enable_query_level_spill: false,
                spill_backoff: None,
                spill_unit_size: 4096,
            }
        }
    }

    fn create_static_mem_stat(usage: usize) -> &'static MemStat {
        let mem_stat = MemStat::create(GlobalUniq::unique());
        let _ = mem_stat.record_memory::<false>(usage as i64, 0);
        Box::leak(Box::new(Arc::into_inner(mem_stat).unwrap()))
    }

    fn create_mem_stat(usage: usize) -> Arc<MemStat> {
        let mem_stat = MemStat::create(GlobalUniq::unique());
        let _ = mem_stat.record_memory::<false>(usage as i64, 0);
        mem_stat
    }

    #[test]
    fn global_spill_triggered_when_global_memory_reaches_threshold() {
        let global_mem = create_static_mem_stat(100);
        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_memory_usage: 100,
            ..Default::default()
        };
        assert!(settings.check_spill());
    }

    #[test]
    fn query_spill_triggered_when_both_levels_enabled_and_query_exceeds() {
        let query_mem = create_mem_stat(100);
        let global_mem = create_static_mem_stat(50);

        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_query_memory_usage: 100,
            enable_query_level_spill: true,
            query_memory_tracking: Some(query_mem.clone()),
            max_memory_usage: 100,
            ..Default::default()
        };
        assert!(settings.check_spill());
    }

    #[test]
    fn query_spill_alone_triggered_when_enabled_and_exceeds() {
        let query_mem = create_mem_stat(100);

        let settings = MemorySettings {
            enable_query_level_spill: true,
            max_query_memory_usage: 100,
            query_memory_tracking: Some(query_mem.clone()),
            ..Default::default()
        };
        assert!(settings.check_spill());
    }

    #[test]
    fn no_spill_when_neither_condition_met() {
        let global_mem = create_static_mem_stat(50);
        let query_mem = create_mem_stat(50);

        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_query_memory_usage: 100,
            max_memory_usage: 100,
            enable_query_level_spill: true,
            query_memory_tracking: Some(query_mem.clone()),
            ..Default::default()
        };
        assert!(!settings.check_spill());
    }

    #[test]
    fn no_query_spill_when_no_tracking() {
        let global_mem = create_static_mem_stat(50);

        let settings = MemorySettings {
            enable_global_level_spill: false,
            global_memory_tracking: global_mem,
            max_query_memory_usage: 100,
            max_memory_usage: 100,
            enable_query_level_spill: true,
            query_memory_tracking: None,
            ..Default::default()
        };
        assert!(!settings.check_spill());
    }

    #[test]
    fn boundary_case_exact_threshold() {
        let query_mem = create_mem_stat(100);

        let settings = MemorySettings {
            enable_query_level_spill: true,
            max_query_memory_usage: 100,
            query_memory_tracking: Some(query_mem.clone()),
            ..Default::default()
        };
        assert!(settings.check_spill());
    }

    #[test]
    fn global_priority_over_query_when_both_exceed() {
        let query_mem = create_mem_stat(150);
        let global_mem = create_static_mem_stat(150);

        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_query_memory_usage: 100,
            max_memory_usage: 100,
            enable_query_level_spill: true,
            query_memory_tracking: Some(query_mem.clone()),
            ..Default::default()
        };
        assert!(settings.check_spill());
    }

    #[test]
    fn no_spill_when_both_levels_disabled() {
        let settings = MemorySettings {
            enable_global_level_spill: false,
            enable_query_level_spill: false,
            ..Default::default()
        };
        assert!(!settings.check_spill());
    }

    #[test]
    fn backoff_sleeps_when_spill_triggered_and_query_is_low_memory() {
        let query_mem = create_mem_stat(40);
        let global_mem = create_static_mem_stat(100);
        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_memory_usage: 100,
            query_memory_tracking: Some(query_mem),
            spill_backoff: Some(SpillBackoffSettings {
                max_sleep_ms: 500,
                min_query_memory_usage: 50,
            }),
            ..Default::default()
        };

        let mut backoff_state = SpillBackoffState::default();

        assert_eq!(
            settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::Sleep(200)
        );
        assert_eq!(
            settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::Sleep(300)
        );
        assert_eq!(
            settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::SpillNow
        );
    }

    #[test]
    fn backoff_spills_immediately_when_query_is_not_low_memory() {
        let query_mem = create_mem_stat(60);
        let global_mem = create_static_mem_stat(100);
        let settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: global_mem,
            max_memory_usage: 100,
            query_memory_tracking: Some(query_mem),
            spill_backoff: Some(SpillBackoffSettings {
                max_sleep_ms: 500,
                min_query_memory_usage: 50,
            }),
            ..Default::default()
        };

        let mut backoff_state = SpillBackoffState::default();

        assert_eq!(
            settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::SpillNow
        );
    }

    #[test]
    fn backoff_resets_after_pressure_is_relieved() {
        let query_mem = create_mem_stat(40);
        let pressured_global_mem = create_static_mem_stat(100);
        let relaxed_global_mem = create_static_mem_stat(80);

        let pressured_settings = MemorySettings {
            enable_global_level_spill: true,
            global_memory_tracking: pressured_global_mem,
            max_memory_usage: 100,
            query_memory_tracking: Some(query_mem.clone()),
            spill_backoff: Some(SpillBackoffSettings {
                max_sleep_ms: 1000,
                min_query_memory_usage: 50,
            }),
            ..Default::default()
        };

        let relaxed_settings = MemorySettings {
            global_memory_tracking: relaxed_global_mem,
            ..pressured_settings.clone()
        };

        let mut backoff_state = SpillBackoffState::default();

        assert_eq!(
            pressured_settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::Sleep(200)
        );
        assert_eq!(
            relaxed_settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::NoSpill
        );
        assert_eq!(
            pressured_settings.check_spill_with_backoff(&mut backoff_state),
            SpillDecision::Sleep(200)
        );
    }
}
