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
use std::sync::OnceLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use petgraph::stable_graph::NodeIndex;

type WakeCallback = Box<dyn Fn(NodeIndex, usize) -> Result<()> + Send + Sync>;

/// Waker for waking up processors from outside the executor.
///
/// # Usage
/// 1. Waker is automatically created when Pipeline is created
/// 2. Processor obtains and holds the waker via pipeline.get_waker()
/// 3. Executor calls waker.bind() to bindthe callback when created
/// 4. External code calls waker.wake() to wake up the processor during execution
///
/// # Note
/// The callback passed to bind should not hold any strong references to avoid circular references
pub struct ExecutorWaker {
    callback: OnceLock<WakeCallback>,
}

impl ExecutorWaker {
    pub fn create() -> Arc<Self> {
        Arc::new(Self {
            callback: OnceLock::new(),
        })
    }

    /// Bindthe wake callback, can only be called once
    ///
    /// # Note
    /// The callback should use weak references internally to avoid circular references
    pub fn bind(&self, callback: WakeCallback) {
        let _ = self.callback.set(callback);
    }

    /// Wake up the specified processor
    ///
    /// # Arguments
    /// * `processor_id` - The NodeIndex of the processor in the graph
    /// * `expect_worker_id` - The expected worker id to handle this task
    ///
    /// # Errors
    /// - If the waker is not bound (executor not created yet)
    /// - If the executor has been dropped (weak reference upgrade failed)
    #[inline]
    pub fn wake(&self, processor_id: NodeIndex, expect_worker_id: usize) -> Result<()> {
        match self.callback.get() {
            Some(cb) => cb(processor_id, expect_worker_id),
            None => Err(ErrorCode::Internal("ExecutorWaker is not bound")),
        }
    }

    /// Check if the waker is bound
    #[inline]
    pub fn is_bound(&self) -> bool {
        self.callback.get().is_some()
    }
}
