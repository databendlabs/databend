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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::add_reserved_memory;
use databend_common_metrics::table_maintenance::record_memory_pressure_deferral;
use databend_common_metrics::table_maintenance::record_memory_pressure_grant_reduction;
use databend_common_metrics::table_maintenance::record_memory_soft_limit_deferral;
use databend_common_metrics::table_maintenance::set_resource_budget;
use databend_common_settings::Settings;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::policy::MIN_UNIT_MEMORY;
use super::policy::MaintenancePolicy;
use crate::sessions::QueryContextShared;

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
    active: usize,
    commitments: Vec<MemoryCommitment>,
    contexts: Vec<Weak<QueryContextShared>>,
}

/// The unused part of one live grant. After release, remaining allocations still
/// count in the parent group; no context or execution grant is retained here.
struct MemoryCommitment {
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

pub(crate) enum MaintenanceAdmission {
    Granted(Arc<MaintenanceReservation>),
    Busy,
    Closed,
}

impl MaintenanceResources {
    pub(super) fn create(policy: MaintenancePolicy) -> Arc<Self> {
        let budget = policy.budget();
        add_reserved_memory(0);
        set_resource_budget(budget.soft_memory_limit, budget.threads);
        if budget.soft_memory_limit < MIN_UNIT_MEMORY {
            log::warn!(memory = budget.soft_memory_limit, minimum = MIN_UNIT_MEMORY;
                "Maintenance soft limit is below the minimum local unit size; increase the ratio or node capacity");
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
                active: 0,
                commitments: Vec::new(),
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

    /// Ready demand is an ephemeral scheduling hint, not the configured maximum or a
    /// persistent entitlement. Ready units share available node headroom, without resizing
    /// grants already issued to running units.
    pub(crate) fn try_reserve(self: &Arc<Self>, demand: usize) -> MaintenanceAdmission {
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
        set_resource_budget(budget.soft_memory_limit, budget.threads);
        if state.active >= self.policy.max_concurrency {
            return MaintenanceAdmission::Busy;
        }
        // The allocator already maintains these counters. No session traversal, OS
        // sampling loop or external metrics scrape is needed to make an admission.
        let maintenance_used = self.memory.get_memory_usage() as u64;
        if budget.reached_soft_limit(maintenance_used) {
            record_memory_soft_limit_deferral();
            return MaintenanceAdmission::Busy;
        }
        // Only allocations belonging to a grant can spend that grant. A released
        // round's retained metadata must not offset another round's unused commitment.
        let unspent = state.commitments.iter().fold(0u64, |total, commitment| {
            let tracked = commitment.memory.get_memory_usage() as u64;
            total.saturating_add(commitment.reserved.saturating_sub(tracked))
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
            .saturating_sub(state.active)
            .max(1)
            .min(self.policy.max_concurrency - state.active)
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
            record_memory_pressure_deferral();
            return MaintenanceAdmission::Busy;
        }
        let threads = (free_threads / shares)
            .min(self.policy.max_threads)
            .min(max_threads)
            .max(1);
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
        state.commitments.push(MemoryCommitment {
            memory: reservation.tracking.clone(),
            reserved: memory,
        });
        Arc::get_mut(&mut reservation)
            .expect("unpublished maintenance reservation")
            .reserved = true;
        state.reserved += memory;
        state.threads += threads;
        state.active += 1;
        add_reserved_memory(memory as i64);
        if memory < base_memory {
            record_memory_pressure_grant_reduction();
        }
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
            set_resource_budget(0, 0);
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
                if state.active == 0 {
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
            .commitments
            .iter()
            .position(|commitment| Arc::ptr_eq(&commitment.memory, &self.tracking))
            .expect("live reservation must have a memory commitment");
        state.commitments.swap_remove(index);
        state.reserved -= self.memory;
        state.threads -= self.threads;
        state.active -= 1;
        add_reserved_memory(-(self.memory as i64));
        drop(state);
        self.resources.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::QueryContext;
    use crate::table_maintenance::policy::MaintenanceBudget;
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
        let policy = MaintenancePolicy::create(&config, capacity);
        let budget = policy.budget();
        assert_eq!(budget.soft_memory_limit, budget.capacity / 2);
        let resources = MaintenanceResources::create(policy);
        assert_eq!(resources.memory.get_limit(), 0);
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
        resources.close();

        // Commitments can reach the watermark without blocking new work. The
        // unused 32 MiB still reduces the headroom available for another grant.
        let mib = 1024 * 1024;
        let budget = MaintenanceBudget {
            capacity: 1024 * mib,
            soft_memory_limit: 512 * mib,
            hard_memory_limit: 0,
            threads: 2,
            headroom: 102 * mib,
        };
        assert_eq!(
            budget.available_memory(540 * mib, 480 * mib, 32 * mib, 512 * mib),
            350 * mib
        );
        assert!(!budget.reached_soft_limit(480 * mib));
        assert!(budget.reached_soft_limit(512 * mib));
        assert!(budget.reached_soft_limit(513 * mib));
        assert!(budget.available_memory(880 * mib, 0, 0, 0) < MIN_UNIT_MEMORY);
        let capped = MaintenanceBudget {
            hard_memory_limit: 600 * mib,
            ..budget
        };
        assert_eq!(
            capped.available_memory(540 * mib, 480 * mib, 32 * mib, 512 * mib),
            88 * mib
        );
        Ok(())
    }
}
