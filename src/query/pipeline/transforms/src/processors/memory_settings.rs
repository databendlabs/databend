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
            .field("spill_unit_size", &self.spill_unit_size)
            .finish()
    }
}

pub struct MemorySettingsBuilder {
    enable_global_level_spill: bool,
    max_memory_usage: Option<usize>,

    enable_group_spill: bool,

    enable_query_level_spill: bool,
    max_query_memory_usage: Option<usize>,
    query_memory_tracking: Option<Arc<MemStat>>,

    spill_unit_size: Option<usize>,
}

impl MemorySettingsBuilder {
    pub fn with_max_memory_usage(mut self, max: usize) -> Self {
        self.enable_global_level_spill = true;
        self.max_memory_usage = Some(max);
        self
    }

    pub fn with_max_query_memory_usage(
        mut self,
        max: usize,
        tracking: Option<Arc<MemStat>>,
    ) -> Self {
        self.enable_query_level_spill = true;
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

    pub fn build(self) -> MemorySettings {
        MemorySettings {
            enable_group_spill: self.enable_group_spill,
            max_memory_usage: self.max_memory_usage.unwrap_or(usize::MAX),
            enable_global_level_spill: self.enable_global_level_spill,
            global_memory_tracking: &GLOBAL_MEM_STAT,
            enable_query_level_spill: self.enable_query_level_spill,
            max_query_memory_usage: self.max_query_memory_usage.unwrap_or(usize::MAX),
            query_memory_tracking: self.query_memory_tracking,
            spill_unit_size: self.spill_unit_size.unwrap_or(0),
        }
    }
}

impl MemorySettings {
    pub fn builder() -> MemorySettingsBuilder {
        MemorySettingsBuilder {
            enable_global_level_spill: false,
            max_memory_usage: None,

            enable_group_spill: true,

            enable_query_level_spill: false,
            max_query_memory_usage: None,
            query_memory_tracking: None,

            spill_unit_size: None,
        }
    }

    pub fn check_spill(&self) -> bool {
        if self.enable_global_level_spill
            && self.global_memory_tracking.get_memory_usage() >= self.max_memory_usage
        {
            return true;
        }

        if self.enable_group_spill
            && let Some(workload_group) = ThreadTracker::workload_group()
        {
            let workload_group_memory_usage = workload_group.mem_stat.get_memory_usage();
            let max_memory_usage = workload_group.max_memory_usage.load(Ordering::Relaxed);

            if max_memory_usage != 0 && workload_group_memory_usage >= max_memory_usage {
                return true;
            }
        }

        let Some(query_memory_tracking) = self.query_memory_tracking.as_ref() else {
            return false;
        };

        self.enable_query_level_spill
            && query_memory_tracking.get_memory_usage() >= self.max_query_memory_usage
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

        let query_memory_tracking = self.query_memory_tracking.as_ref()?;
        let usage = query_memory_tracking.get_memory_usage();

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
}
