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

//! Profiling targets registered at runtime, e.g. by the admin API.
//!
//! `EXPLAIN PERF` marks the threads of its statement through the tracking payload the statement
//! is built with. A query that is already running cannot be marked that way, its payloads are
//! fixed. Instead, a target is registered here, and every time a thread switches its tracking
//! payload, the payload's query id is looked up and the thread-local profiling state is updated.
//! Threads switch payloads whenever a processor or a task is polled, so a running query is picked
//! up right away. When nothing is registered, the lookup is a single atomic load.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;

use crate::runtime::AllocProfile;
use crate::runtime::TrackingPayload;

static ACTIVE_TARGETS: AtomicUsize = AtomicUsize::new(0);

static QUERY_TARGETS: RwLock<Option<HashMap<String, QueryTarget>>> = RwLock::new(None);

/// Samples the allocations of every thread running a query.
static NODE_ALLOC_PROFILE: RwLock<Option<Arc<AllocProfile>>> = RwLock::new(None);

#[derive(Default)]
struct QueryTarget {
    cpu: usize,
    alloc: Option<Arc<AllocProfile>>,
}

/// The profiling state a thread derives from its tracking payload.
pub(crate) struct ThreadProfiling {
    pub cpu: bool,
    pub alloc: Option<Arc<AllocProfile>>,
}

pub struct PerfTargets;

impl PerfTargets {
    /// Marks the threads of `query_id` for CPU sampling until the guard is dropped.
    pub fn register_query_cpu(query_id: &str) -> PerfTargetGuard {
        let mut targets = QUERY_TARGETS.write();
        targets
            .get_or_insert_with(HashMap::new)
            .entry(query_id.to_string())
            .or_default()
            .cpu += 1;
        ACTIVE_TARGETS.fetch_add(1, Ordering::SeqCst);
        PerfTargetGuard(Target::QueryCpu(query_id.to_string()))
    }

    /// Samples the allocations of `query_id` into `profile` until the guard is dropped.
    ///
    /// Fails if the allocations of the query are already being sampled.
    pub fn register_query_alloc(
        query_id: &str,
        profile: Arc<AllocProfile>,
    ) -> Result<PerfTargetGuard, String> {
        let mut targets = QUERY_TARGETS.write();
        let target = targets
            .get_or_insert_with(HashMap::new)
            .entry(query_id.to_string())
            .or_default();
        if target.alloc.is_some() {
            return Err(format!(
                "the allocations of query {query_id} are already being sampled"
            ));
        }
        target.alloc = Some(profile);
        ACTIVE_TARGETS.fetch_add(1, Ordering::SeqCst);
        Ok(PerfTargetGuard(Target::QueryAlloc(query_id.to_string())))
    }

    /// Samples the allocations of every thread running a query until the guard is dropped.
    ///
    /// Fails if the allocations of the node are already being sampled.
    pub fn register_node_alloc(profile: Arc<AllocProfile>) -> Result<PerfTargetGuard, String> {
        let mut node = NODE_ALLOC_PROFILE.write();
        if node.is_some() {
            return Err("the allocations of this node are already being sampled".to_string());
        }
        *node = Some(profile);
        ACTIVE_TARGETS.fetch_add(1, Ordering::SeqCst);
        Ok(PerfTargetGuard(Target::NodeAlloc))
    }

    /// The profiling state of a thread running with `payload`.
    #[inline]
    pub(crate) fn resolve(payload: &TrackingPayload) -> ThreadProfiling {
        let mut profiling = ThreadProfiling {
            cpu: payload.perf_enabled,
            alloc: payload.alloc_profile.clone(),
        };

        if ACTIVE_TARGETS.load(Ordering::Relaxed) == 0 {
            return profiling;
        }

        if let Some(query_id) = payload.query_id.as_deref()
            && let Some(target) = QUERY_TARGETS
                .read()
                .as_ref()
                .and_then(|targets| targets.get(query_id))
        {
            profiling.cpu |= target.cpu > 0;
            if profiling.alloc.is_none() {
                profiling.alloc = target.alloc.clone();
            }
        }

        if profiling.alloc.is_none() && payload.query_id.is_some() {
            profiling.alloc = NODE_ALLOC_PROFILE.read().clone();
        }
        profiling
    }
}

enum Target {
    QueryCpu(String),
    QueryAlloc(String),
    NodeAlloc,
}

/// Unregisters a profiling target when dropped.
pub struct PerfTargetGuard(Target);

impl Drop for PerfTargetGuard {
    fn drop(&mut self) {
        match &self.0 {
            Target::QueryCpu(query_id) | Target::QueryAlloc(query_id) => {
                let mut targets = QUERY_TARGETS.write();
                if let Some(targets) = targets.as_mut()
                    && let Some(target) = targets.get_mut(query_id)
                {
                    match &self.0 {
                        Target::QueryCpu(_) => target.cpu = target.cpu.saturating_sub(1),
                        _ => target.alloc = None,
                    }
                    if target.cpu == 0 && target.alloc.is_none() {
                        targets.remove(query_id);
                    }
                }
            }
            Target::NodeAlloc => *NODE_ALLOC_PROFILE.write() = None,
        }
        ACTIVE_TARGETS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::ThreadTracker;

    fn payload(query_id: &str) -> TrackingPayload {
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.query_id = Some(query_id.to_string());
        payload
    }

    // The targets are global, the tests use distinct query ids and do not register node targets
    // at the same time.

    #[test]
    fn test_query_targets() {
        let other = payload("perf-targets-other");
        let query = payload("perf-targets-query");
        assert!(!PerfTargets::resolve(&query).cpu);

        let cpu = PerfTargets::register_query_cpu("perf-targets-query");
        assert!(PerfTargets::resolve(&query).cpu);
        assert!(!PerfTargets::resolve(&other).cpu);

        let profile = AllocProfile::create();
        let alloc =
            PerfTargets::register_query_alloc("perf-targets-query", profile.clone()).unwrap();
        assert!(
            PerfTargets::register_query_alloc("perf-targets-query", AllocProfile::create())
                .is_err()
        );
        let resolved = PerfTargets::resolve(&query).alloc.unwrap();
        assert!(Arc::ptr_eq(&resolved, &profile));

        drop(cpu);
        assert!(!PerfTargets::resolve(&query).cpu);
        assert!(PerfTargets::resolve(&query).alloc.is_some());

        drop(alloc);
        assert!(PerfTargets::resolve(&query).alloc.is_none());
    }

    #[test]
    fn test_payload_profile_takes_precedence() {
        let payload_profile = AllocProfile::create();
        let mut query = payload("perf-targets-precedence");
        query.alloc_profile = Some(payload_profile.clone());

        let _guard =
            PerfTargets::register_query_alloc("perf-targets-precedence", AllocProfile::create())
                .unwrap();
        let resolved = PerfTargets::resolve(&query).alloc.unwrap();
        assert!(Arc::ptr_eq(&resolved, &payload_profile));
    }
}
