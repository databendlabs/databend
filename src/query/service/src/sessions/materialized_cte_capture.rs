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

use databend_common_sql::MetadataRef;
use parking_lot::Mutex;

use crate::physical_plans::PhysicalPlan;

/// One captured materialized-CTE producer execution.
///
/// The producer's profiling batch is NOT stored inline here. It lives in the
/// shared `QueryProfiles` map keyed by `profile_execution_id`; consumers
/// (EXPLAIN ANALYZE formatter) fetch it on demand via
/// `QueryContext::get_query_profiles_with_execution_id(&self.profile_execution_id)`.
/// This keeps a single source of truth for profile data and lets the outer
/// `databend::log::profile` record include CTE producers automatically.
pub struct CapturedCteExecution {
    pub cte_name: String,
    pub temp_table_name: String,
    pub plan: PhysicalPlan,
    pub metadata: MetadataRef,
    /// Executor-instance id that produced this CTAS; `None` for plain EXPLAIN
    /// (no pipeline was executed, so no profile was emitted).
    pub profile_execution_id: Option<String>,
}

/// Scratch slot the CTAS interpreter writes to and the binder drains after
/// `execute_query_with_sql_string` returns. Only the bits that can't be
/// reconstructed afterwards live here.
pub struct PendingCtasCapture {
    pub plan: PhysicalPlan,
    pub metadata: MetadataRef,
    pub profile_execution_id: Option<String>,
}

/// Capture state attached to `QueryContextShared` — ordered list of
/// completed producers plus a single-slot staging area for the CTAS currently
/// running.
pub struct MaterializedCteCapture {
    captured: Vec<CapturedCteExecution>,
    pending: Option<PendingCtasCapture>,
}

impl MaterializedCteCapture {
    pub fn new() -> Self {
        Self {
            captured: Vec::new(),
            pending: None,
        }
    }

    pub fn into_parts(self) -> Vec<CapturedCteExecution> {
        self.captured
    }
}

impl Default for MaterializedCteCapture {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle the binder / interpreter share. `Mutex` because writes happen both
/// from the interpreter's `on_finished` (may be on an executor thread) and
/// from `block_on` inside `m_cte_to_temp_table`.
#[derive(Clone, Default)]
pub struct MaterializedCteCaptureSlot {
    inner: Arc<Mutex<Option<MaterializedCteCapture>>>,
}

impl MaterializedCteCaptureSlot {
    pub fn begin(&self) {
        *self.inner.lock() = Some(MaterializedCteCapture::new());
    }

    pub fn finish(&self) -> Option<MaterializedCteCapture> {
        self.inner.lock().take()
    }

    pub fn is_active(&self) -> bool {
        self.inner.lock().is_some()
    }

    pub fn set_pending(&self, pending: PendingCtasCapture) {
        if let Some(capture) = self.inner.lock().as_mut() {
            capture.pending = Some(pending);
        }
    }

    pub fn with_pending<F>(&self, f: F)
    where F: FnOnce(&mut PendingCtasCapture) {
        if let Some(capture) = self.inner.lock().as_mut()
            && let Some(pending) = capture.pending.as_mut()
        {
            f(pending);
        }
    }

    pub fn take_pending(&self) -> Option<PendingCtasCapture> {
        self.inner.lock().as_mut().and_then(|c| c.pending.take())
    }

    pub fn push_captured(&self, entry: CapturedCteExecution) {
        if let Some(capture) = self.inner.lock().as_mut() {
            capture.captured.push(entry);
        }
    }
}
