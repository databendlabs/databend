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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_config::QueryConfig;

// MemStat rounds smaller positive limits up to 256 MiB. Never admit a smaller grant:
// otherwise accounting would promise less memory than the executor can actually use.
pub(super) const MIN_UNIT_MEMORY: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct MaintenanceBudget {
    pub capacity: u64,
    pub soft_memory_limit: u64,
    pub hard_memory_limit: u64,
    pub threads: u64,
    pub headroom: u64,
}

impl MaintenanceBudget {
    pub fn reached_soft_limit(&self, maintenance_used: u64) -> bool {
        maintenance_used >= self.soft_memory_limit
    }

    /// Reserve additional room for foreground bursts as node usage rises from
    /// 60% to 70%. It affects new admissions only, and is independent of the
    /// background soft limit and any explicit background hard limit.
    pub fn with_node_pressure(self, used: u64) -> Self {
        let pressure_start = percent_of(self.capacity, 60);
        let extra = used
            .saturating_sub(pressure_start)
            .min(percent_of(self.capacity, 10));
        Self {
            headroom: self.headroom.saturating_add(extra).min(self.capacity),
            ..self
        }
    }

    pub fn available_memory(
        &self,
        used: u64,
        maintenance_used: u64,
        unspent: u64,
        reserved: u64,
    ) -> u64 {
        let available = self
            .capacity
            .saturating_sub(used)
            .saturating_sub(self.headroom)
            .saturating_sub(unspent);
        if self.hard_memory_limit == 0 {
            return available;
        }
        // Released units can leave tracked allocations behind. Do not let those
        // allocations offset a different unit's unused commitment.
        let committed = maintenance_used.saturating_add(unspent).max(reserved);
        available.min(self.hard_memory_limit.saturating_sub(committed))
    }
}

/// Capacity is detected once at startup; an enforced global tracker limit can lower it.
/// The soft water mark gates new work, while grants share live node headroom according
/// to ready demand. Existing grants do not shrink when the node comes under pressure.
pub(super) struct MaintenancePolicy {
    pub max_concurrency: usize,
    pub max_threads: u64,
    capacity: u64,
    threads: u64,
    absolute_memory: u64,
    memory_ratio: u64,
    headroom_ratio: u64,
}

impl MaintenancePolicy {
    pub fn create(config: &QueryConfig, capacity: u64) -> Self {
        let config = &config.common;
        let detected = (num_cpus::get() as u64)
            .min(std::thread::available_parallelism().map_or(1, |n| n.get() as u64))
            .max(1);
        let cores = if config.num_cpus == 0 {
            detected
        } else {
            detected.min(config.num_cpus)
        };
        Self {
            max_concurrency: if config.table_hook_async_max_concurrency == 0 {
                cores.div_ceil(4) as usize
            } else {
                config.table_hook_async_max_concurrency
            },
            max_threads: if config.table_maintenance_max_threads == 0 {
                cores
            } else {
                cores.min(config.table_maintenance_max_threads)
            },
            capacity,
            threads: (cores / 4).max(1),
            absolute_memory: config.table_maintenance_max_memory_usage,
            memory_ratio: config.table_maintenance_memory_ratio,
            headroom_ratio: config.table_maintenance_memory_headroom_ratio,
        }
    }

    pub fn budget(&self) -> MaintenanceBudget {
        let global_limit = GLOBAL_MEM_STAT.get_limit().max(0) as u64;
        let capacity = match (self.capacity, global_limit) {
            (0, limit) => limit,
            (capacity, 0) => capacity,
            (capacity, limit) => capacity.min(limit),
        };
        MaintenanceBudget {
            capacity,
            soft_memory_limit: percent_of(capacity, self.memory_ratio),
            hard_memory_limit: self.absolute_memory,
            threads: self.threads,
            headroom: percent_of(capacity, self.headroom_ratio),
        }
    }
}

fn percent_of(capacity: u64, ratio: u64) -> u64 {
    (capacity as u128 * ratio as u128 / 100) as u64
}
