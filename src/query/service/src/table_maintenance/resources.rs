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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::MemStat;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::Settings;

/// Node-local accounting, also used by remote maintenance fragments. Reservations are
/// acquired before planning, retained by the query context and released on every drop path.
/// This is not a warehouse-wide semaphore or a remote load-probing protocol.
pub(crate) struct MaintenanceResources {
    memory: Arc<MemStat>,
    reserved: AtomicU64,
    total: u64,
    unit: u64,
}

pub(crate) struct MaintenanceReservation {
    resources: Arc<MaintenanceResources>,
}

impl MaintenanceResources {
    pub(crate) fn init(total: u64, concurrency: usize) {
        let memory = MemStat::create_workload_group();
        memory.set_limit(total as i64, false);
        GlobalInstance::set(Arc::new(Self {
            memory,
            reserved: AtomicU64::new(0),
            total,
            unit: total / concurrency as u64,
        }));
    }

    pub(crate) fn reserve() -> Result<Arc<MaintenanceReservation>> {
        let resources = GlobalInstance::get::<Arc<Self>>();
        let limit = GLOBAL_MEM_STAT.get_limit();
        let used = GLOBAL_MEM_STAT.get_memory_usage() as u64;
        let result =
            resources
                .reserved
                .try_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                    let next = reserved.checked_add(resources.unit)?;
                    (next <= resources.total
                        && (limit <= 0 || used.saturating_add(next) <= limit as u64))
                        .then_some(next)
                });
        if result.is_err() {
            return Err(ErrorCode::MemoryExceedsLimit(
                "Maintenance memory reservation unavailable",
            ));
        }
        Ok(Arc::new(MaintenanceReservation { resources }))
    }
}

impl MaintenanceReservation {
    pub(crate) fn memory(&self) -> Arc<MemStat> {
        self.resources.memory.clone()
    }

    pub(crate) fn constrain(&self, settings: &Settings) -> Result<()> {
        let config = GlobalConfig::instance();
        let threads = settings
            .get_max_threads()?
            .min(config.query.common.table_maintenance_max_threads);
        settings.set_max_threads(threads.max(1))?;
        settings.set_max_storage_io_requests(
            settings
                .get_max_storage_io_requests()?
                .min(threads.max(1) * 2),
        )?;
        settings.set_max_query_memory_usage(self.resources.unit)?;
        settings.set_setting("query_out_of_memory_behavior".into(), "throw".into())?;
        settings.set_setting("allow_query_exceeded_limit".into(), "0".into())?;
        settings.set_setting(
            "recluster_block_size".into(),
            (self.resources.unit / 4).to_string(),
        )?;
        Ok(())
    }
}

impl Drop for MaintenanceReservation {
    fn drop(&mut self) {
        self.resources
            .reserved
            .fetch_sub(self.resources.unit, Ordering::AcqRel);
    }
}
