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
use std::sync::Weak;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_config::QueryConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::add_reserved_memory;
use databend_common_settings::Settings;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::sessions::QueryContextShared;

// MemStat rounds smaller positive limits up to 256 MiB. Never admit a smaller grant.
const MIN_UNIT_MEMORY: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy)]
struct AdmissionBudget {
    capacity: u64,
    soft_memory_limit: u64,
    hard_memory_limit: u64,
    threads: u64,
    headroom: u64,
}

impl AdmissionBudget {
    fn reached_soft_limit(&self, maintenance_used: u64) -> bool {
        maintenance_used >= self.soft_memory_limit
    }

    /// Add foreground headroom as node usage rises from 60% to 70%.
    /// Existing grants are unchanged.
    fn with_node_pressure(self, used: u64) -> Self {
        let extra = used
            .saturating_sub(percent_of(self.capacity, 60))
            .min(percent_of(self.capacity, 10));
        Self {
            headroom: self.headroom.saturating_add(extra).min(self.capacity),
            ..self
        }
    }

    fn available_memory(
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

/// Startup limits. Each admission computes a budget using the current global tracker limit.
pub(super) struct MaintenancePolicy {
    pub(super) max_concurrency: usize,
    capacity: u64,
    threads: u64,
    absolute_memory: u64,
}

impl MaintenancePolicy {
    pub(super) fn create(config: &QueryConfig, capacity: u64) -> Self {
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
            capacity,
            threads: (cores / 4).max(1),
            absolute_memory: config.table_maintenance_max_memory_usage,
        }
    }

    fn budget(&self) -> AdmissionBudget {
        let global_limit = GLOBAL_MEM_STAT.get_limit().max(0) as u64;
        let capacity = match (self.capacity, global_limit) {
            (0, limit) => limit,
            (capacity, 0) => capacity,
            (capacity, limit) => capacity.min(limit),
        };
        AdmissionBudget {
            capacity,
            soft_memory_limit: percent_of(capacity, 50),
            hard_memory_limit: self.absolute_memory,
            threads: self.threads,
            headroom: percent_of(capacity, 10),
        }
    }
}

fn percent_of(capacity: u64, ratio: u64) -> u64 {
    (capacity as u128 * ratio as u128 / 100) as u64
}

/// Per-node admission and ownership, shared by coordinators and remote fragments.
/// The policy soft limit stops NEW work. Existing grants survive ordinary load changes;
/// cancellation is reserved for shutdown and the existing execution/memory protections.
/// This is not a warehouse-wide semaphore, worker join or physical memory reservation.
pub(crate) struct MaintenanceResources {
    memory: Arc<MemStat>,
    policy: MaintenancePolicy,
    state: Mutex<MaintenanceState>,
    released: Notify,
}

struct MaintenanceState {
    closed: bool,
    reserved: u64,
    threads: u64,
    grants: Vec<ActiveGrant>,
    contexts: Vec<Weak<QueryContextShared>>,
}

/// Tracks how much of a live grant is still unspent. After release, remaining
/// allocations still count in the parent group.
struct ActiveGrant {
    memory: Arc<MemStat>,
    reserved: u64,
}

pub(crate) struct MaintenanceReservation {
    resources: Arc<MaintenanceResources>,
    tracking: Arc<MemStat>,
    memory: u64,
    threads: u64,
    reserved: bool,
}

pub(super) enum MaintenanceAdmission {
    Granted(Arc<MaintenanceReservation>),
    Busy,
    Closed,
}

impl MaintenanceResources {
    pub(super) fn create(policy: MaintenancePolicy) -> Arc<Self> {
        let budget = policy.budget();
        add_reserved_memory(0);
        if budget.soft_memory_limit < MIN_UNIT_MEMORY {
            log::warn!(memory = budget.soft_memory_limit, minimum = MIN_UNIT_MEMORY;
                "Maintenance soft limit is below the minimum local unit size");
        }
        let memory = MemStat::create_workload_group();
        // Only an explicitly configured byte ceiling is enforced on the parent tracker.
        memory.set_limit(budget.hard_memory_limit as i64, false);
        Arc::new(Self {
            memory,
            policy,
            state: Mutex::new(MaintenanceState {
                closed: false,
                reserved: 0,
                threads: 0,
                grants: Vec::new(),
                contexts: Vec::new(),
            }),
            released: Notify::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<Arc<MaintenanceReservation>> {
        Self::reservation_result(self.try_reserve(1))
    }

    /// Fragments must reserve the coordinator's planned memory envelope in full. Silently
    /// lowering it on a busier/smaller peer cannot make an already planned round fit.
    pub(crate) fn reserve_remote(
        self: &Arc<Self>,
        settings: &Settings,
    ) -> Result<Arc<MaintenanceReservation>> {
        Self::reservation_result(self.admit(
            1,
            Some(settings.get_max_query_memory_usage()?),
            settings.get_max_threads()?,
        ))
    }

    fn reservation_result(admission: MaintenanceAdmission) -> Result<Arc<MaintenanceReservation>> {
        match admission {
            MaintenanceAdmission::Granted(reservation) => Ok(reservation),
            MaintenanceAdmission::Busy => Err(ErrorCode::NoResourcesAvailable(
                "Maintenance resource reservation unavailable; retry at a unit boundary",
            )),
            MaintenanceAdmission::Closed => Err(ErrorCode::AbortedQuery(
                "Node stopped maintenance admission",
            )),
        }
    }

    /// True only when the batch's own retained allocations prevent another grant,
    /// even if all other work on the node releases its memory.
    pub(super) fn hard_limit_blocks_continuation(&self, retained_bytes: u64) -> bool {
        let limit = self.memory.get_limit() as u64;
        limit != 0 && retained_bytes != 0 && limit.saturating_sub(retained_bytes) < MIN_UNIT_MEMORY
    }

    /// Ready demand is an ephemeral scheduling hint, not the configured maximum or a
    /// persistent entitlement. Ready units share available node headroom, without resizing
    /// grants already issued to running units.
    pub(super) fn try_reserve(self: &Arc<Self>, demand: usize) -> MaintenanceAdmission {
        self.admit(demand, None, u64::MAX)
    }

    fn admit(
        self: &Arc<Self>,
        demand: usize,
        required_memory: Option<u64>,
        max_threads: u64,
    ) -> MaintenanceAdmission {
        // Drop order matters: an unwind must unlock state before a charged grant's
        // destructor re-enters it. Leave the grant uninitialized on cheap Busy paths.
        let mut reservation;
        let mut state = self.state.lock();
        if state.closed {
            return MaintenanceAdmission::Closed;
        }
        let budget = self.policy.budget();
        let active = state.grants.len();
        if active >= self.policy.max_concurrency {
            return MaintenanceAdmission::Busy;
        }
        // The allocator already maintains these counters. No session traversal, OS
        // sampling loop or external metrics scrape is needed to make an admission.
        let maintenance_used = self.memory.get_memory_usage() as u64;
        if budget.reached_soft_limit(maintenance_used) {
            return MaintenanceAdmission::Busy;
        }
        // Only allocations belonging to a grant can spend that grant. A released
        // round's retained metadata must not offset another round's unused commitment.
        let unspent = state.grants.iter().fold(0u64, |total, grant| {
            let tracked = grant.memory.get_memory_usage() as u64;
            total.saturating_add(grant.reserved.saturating_sub(tracked))
        });
        let used = GLOBAL_MEM_STAT.get_memory_usage() as u64;
        let base_free_memory =
            budget.available_memory(used, maintenance_used, unspent, state.reserved);
        let free_threads = budget.threads.saturating_sub(state.threads);
        if base_free_memory < MIN_UNIT_MEMORY || free_threads == 0 {
            return MaintenanceAdmission::Busy;
        }
        // Keep the same share count under added pressure: reducing the denominator
        // could give one new unit *more* memory or threads than without the reserve.
        let shares = demand
            .saturating_sub(active)
            .max(1)
            .min(self.policy.max_concurrency - active)
            .min(free_threads as usize)
            .min((base_free_memory / MIN_UNIT_MEMORY) as usize) as u64;
        let memory_for = |free_memory: u64| {
            // The soft limit caps a single local grant, not the sum. Remote fragments
            // must always take their entire planned grant or defer.
            required_memory.unwrap_or_else(|| {
                (free_memory / shares)
                    .max(MIN_UNIT_MEMORY)
                    .min(budget.soft_memory_limit)
            })
        };
        let base_memory = memory_for(base_free_memory);
        if base_memory < MIN_UNIT_MEMORY || base_memory > base_free_memory {
            return MaintenanceAdmission::Busy;
        }
        let free_memory = budget.with_node_pressure(used).available_memory(
            used,
            maintenance_used,
            unspent,
            state.reserved,
        );
        let memory = memory_for(free_memory);
        if memory > free_memory {
            return MaintenanceAdmission::Busy;
        }
        let threads = (free_threads / shares).min(max_threads).max(1);
        // Allocate before charging. If allocation unwinds, no ownerless charge remains.
        let tracking = MemStat::create_child(None, 0, ParentMemStat::Normal(self.memory.clone()));
        tracking.set_limit(memory as i64, false);
        reservation = Arc::new(MaintenanceReservation {
            resources: self.clone(),
            tracking,
            memory,
            threads,
            reserved: false,
        });
        state.grants.push(ActiveGrant {
            memory: reservation.tracking.clone(),
            reserved: memory,
        });
        Arc::get_mut(&mut reservation)
            .expect("unpublished maintenance reservation")
            .reserved = true;
        state.reserved += memory;
        state.threads += threads;
        add_reserved_memory(memory as i64);
        MaintenanceAdmission::Granted(reservation)
    }

    /// Covers both coordinators and remote fragments. Registration racing shutdown is
    /// rejected under the same lock, so a late context cannot escape cancellation.
    pub(crate) fn close(&self) {
        let _guard = LimitMemGuard::enter_unlimited();
        let count = {
            let mut state = self.state.lock();
            if state.closed {
                return;
            }
            state.closed = true;
            state.contexts.len()
        };
        for index in 0..count {
            let context = self.state.lock().contexts[index].upgrade();
            if let Some(context) = context {
                context.kill(ErrorCode::AbortedQuery(
                    "Node is stopping table maintenance",
                ));
            }
        }
        self.released.notify_waiters();
    }

    pub(crate) async fn drain(&self) {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            {
                let state = self.state.lock();
                if state.grants.is_empty() {
                    return;
                }
            }
            released.await;
        }
    }
}

impl MaintenanceReservation {
    pub(crate) fn register(&self, context: Weak<QueryContextShared>) -> Result<()> {
        let mut state = self.resources.state.lock();
        if state.closed {
            return Err(ErrorCode::AbortedQuery(
                "Node stopped maintenance admission",
            ));
        }
        state.contexts.retain(|context| context.strong_count() != 0);
        state.contexts.push(context);
        Ok(())
    }

    pub(crate) fn memory(&self) -> Arc<MemStat> {
        self.tracking.clone()
    }

    pub(crate) fn constrain(&self, settings: &Settings) -> Result<()> {
        let threads = settings.get_max_threads()?.min(self.threads).max(1);
        settings.set_max_threads(threads)?;
        settings.set_max_storage_io_requests(
            settings
                .get_max_storage_io_requests()?
                .min(threads.saturating_mul(2)),
        )?;
        settings.set_max_query_memory_usage(self.memory)?;
        settings.set_setting("query_out_of_memory_behavior".into(), "throw".into())?;
        settings.set_setting("allow_query_exceeded_limit".into(), "0".into())?;
        settings.set_setting("recluster_block_size".into(), (self.memory / 4).to_string())?;
        Ok(())
    }
}

impl Drop for MaintenanceReservation {
    fn drop(&mut self) {
        if !self.reserved {
            return;
        }
        let mut state = self.resources.state.lock();
        let index = state
            .grants
            .iter()
            .position(|grant| Arc::ptr_eq(&grant.memory, &self.tracking))
            .expect("live reservation must have an active grant");
        state.grants.swap_remove(index);
        state.reserved -= self.memory;
        state.threads -= self.threads;
        add_reserved_memory(-(self.memory as i64));
        drop(state);
        self.resources.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::QueryContext;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shutdown_cancels_shared_contexts_and_rejects_late_registration() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        // Admission reads allocator counters; synthetic capacity keeps this ownership
        // and shutdown contract independent of the test host's physical memory size.
        let mut config = databend_common_config::QueryConfig::default();
        config.common.table_hook_async_max_concurrency = 1;
        let capacity = (GLOBAL_MEM_STAT.get_memory_usage() as u64)
            .saturating_mul(2)
            .saturating_add(16 * 1024 * 1024 * 1024);
        let resources = MaintenanceResources::create(MaintenancePolicy::create(&config, capacity));
        let reservation = resources.reserve()?;
        assert!(matches!(
            resources.try_reserve(1),
            MaintenanceAdmission::Busy
        ));
        ctx.set_maintenance_reservation(reservation.clone())?;
        let derived = QueryContext::create_from(&ctx);
        let shared = resources.state.lock().contexts[0].upgrade().unwrap();
        drop(ctx);
        assert!(shared.check_aborting().is_ok());
        resources.close();
        assert!(
            shared.check_aborting().is_err(),
            "derived contexts must also be interrupted"
        );
        assert!(resources.reserve().is_err());

        let late = fixture.new_query_ctx().await?;
        assert!(
            late.set_maintenance_reservation(reservation.clone())
                .is_err()
        );
        drop(derived);
        drop(shared);
        drop(reservation);
        assert_eq!(resources.state.lock().reserved, 0);
        Ok(())
    }
}
