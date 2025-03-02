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
            && self.global_memory_tracking.get_memory_usage() >= self.max_query_memory_usage
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
