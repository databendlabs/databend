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

//! Test-only hooks for recursive CTE execution.
//!
//! This module is intended to make race conditions reproducible by providing
//! deterministic pause/resume points in the recursive CTE executor.
//!
//! By default no hooks are installed and the hook checks are no-ops.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

static HOOKS: OnceLock<Arc<RcteHookRegistry>> = OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GateKey {
    query_id: String,
    step: usize,
}

impl GateKey {
    fn new(query_id: &str, step: usize) -> Self {
        Self {
            query_id: query_id.to_string(),
            step,
        }
    }
}

#[derive(Default)]
pub struct RcteHookRegistry {
    gates: Mutex<HashMap<GateKey, Arc<PauseGate>>>,
}

impl RcteHookRegistry {
    pub fn global() -> Arc<RcteHookRegistry> {
        HOOKS
            .get_or_init(|| Arc::new(RcteHookRegistry::default()))
            .clone()
    }

    pub fn install_pause_before_step(&self, query_id: &str, step: usize) -> Arc<PauseGate> {
        let mut gates = self.gates.lock().unwrap();
        let key = GateKey::new(query_id, step);
        gates
            .entry(key)
            .or_insert_with(|| Arc::new(PauseGate::new(step)))
            .clone()
    }

    fn get_gate(&self, query_id: &str, step: usize) -> Option<Arc<PauseGate>> {
        let key = GateKey::new(query_id, step);
        self.gates.lock().unwrap().get(&key).cloned()
    }
}

/// A reusable pause gate for a single step number.
///
/// When the code hits the hook point, it increments `arrived` and blocks until
/// the test releases the same hit index via `release(hit_no)`.
pub struct PauseGate {
    step: usize,
    arrived: AtomicUsize,
    released: AtomicUsize,
    arrived_notify: Notify,
    released_notify: Notify,
}

impl PauseGate {
    fn new(step: usize) -> Self {
        Self {
            step,
            arrived: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            arrived_notify: Notify::new(),
            released_notify: Notify::new(),
        }
    }

    pub fn step(&self) -> usize {
        self.step
    }

    pub fn arrived(&self) -> usize {
        self.arrived.load(Ordering::Acquire)
    }

    pub async fn wait_arrived_at_least(&self, n: usize) {
        loop {
            let notified = self.arrived_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.arrived() >= n {
                return;
            }

            // Re-check after registration to avoid missing a notify between
            // condition check and awaiting.
            if self.arrived() >= n {
                return;
            }

            notified.await;
        }
    }

    /// Release the `hit_no`-th arrival (1-based).
    pub fn release(&self, hit_no: usize) {
        // Monotonic release.
        let mut cur = self.released.load(Ordering::Acquire);
        while cur < hit_no {
            match self
                .released
                .compare_exchange(cur, hit_no, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
        self.released_notify.notify_waiters();
    }

    async fn hit(&self) {
        let hit_no = self.arrived.fetch_add(1, Ordering::AcqRel) + 1;
        self.arrived_notify.notify_waiters();

        loop {
            let notified = self.released_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let released = self.released.load(Ordering::Acquire);
            if released >= hit_no {
                return;
            }

            // Re-check after registration to avoid missing a notify between
            // condition check and awaiting.
            let released = self.released.load(Ordering::Acquire);
            if released >= hit_no {
                return;
            }

            notified.await;
        }
    }
}

/// Called from the recursive CTE executor.
///
/// If a pause gate is installed for `step`, this call will block until released.
#[async_backtrace::framed]
pub async fn maybe_pause_before_step(query_id: &str, step: usize) {
    let Some(registry) = HOOKS.get() else {
        return;
    };
    let Some(gate) = registry.get_gate(query_id, step) else {
        return;
    };
    gate.hit().await;
}
