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

use databend_common_exception::Result;

use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::pipeline::OptimizerTraceCollector;

/// Trait defining the interface for query optimizers.
#[async_trait::async_trait]
pub trait Optimizer: Send + Sync {
    /// Returns a unique identifier for this optimizer.
    fn name(&self) -> String;

    /// Optimize the given expression and return the optimized version.
    async fn optimize(&mut self, expr: &SExpr) -> Result<SExpr>;

    /// Get the memo if this optimizer maintains one.
    /// Default implementation returns None for optimizers that don't use a memo.
    fn memo(&self) -> Option<&Memo> {
        None
    }

    /// Set the trace collector for this optimizer.
    /// Default implementation does nothing.
    fn set_trace_collector(&mut self, _collector: Arc<OptimizerTraceCollector>) {
        // Default implementation does nothing
    }
}
