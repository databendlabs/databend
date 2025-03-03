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

use std::sync::Arc;

use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::GLOBAL_MEM_STAT;

#[derive(Clone)]
pub struct MemorySettings {
    pub max_memory_usage: usize,
    pub enable_global_level_spill: bool,
    pub global_memory_tracking: &'static MemStat,

    pub max_query_memory_usage: usize,
    pub query_memory_tracking: Option<Arc<MemStat>>,
    pub enable_query_level_spill: bool,

    pub spill_unit_size: usize,
}

impl MemorySettings {
    pub fn disable_spill() -> MemorySettings {
        MemorySettings {
            spill_unit_size: 0,
            max_memory_usage: usize::MAX,
            enable_global_level_spill: false,
            max_query_memory_usage: usize::MAX,
            query_memory_tracking: None,
            enable_query_level_spill: false,
            global_memory_tracking: &GLOBAL_MEM_STAT,
        }
    }

    pub fn check_spill(&self) -> bool {
        if self.enable_global_level_spill
            && self.global_memory_tracking.get_memory_usage() >= self.max_memory_usage
        {
            return true;
        }

        let Some(query_memory_tracking) = self.query_memory_tracking.as_ref() else {
            return false;
        };

        self.enable_query_level_spill
            && query_memory_tracking.get_memory_usage() >= self.max_query_memory_usage
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::GlobalUniqName;

    use super::*;

    impl Default for MemorySettings {
        fn default() -> Self {
            Self {
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
        let mem_stat = MemStat::create(GlobalUniqName::unique());
        let _ = mem_stat.record_memory::<false>(usage as i64, 0);
        Box::leak(Box::new(Arc::into_inner(mem_stat).unwrap()))
    }

    fn create_mem_stat(usage: usize) -> Arc<MemStat> {
        let mem_stat = MemStat::create(GlobalUniqName::unique());
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
