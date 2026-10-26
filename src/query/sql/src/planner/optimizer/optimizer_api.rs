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

use crate::optimizer::ir::SExpr;
use crate::optimizer::pipeline::OptimizerTraceCollector;

/// Interface for same-type rewrite passes. Logical passes use SExpr by default;
/// physical passes use PExpr. Logical-to-physical search has a separate entry point.
#[async_trait::async_trait]
pub trait Optimizer<Expr = SExpr>: Send + Sync {
    /// Returns a unique identifier for this optimizer.
    fn name(&self) -> String;

    /// Consume the given expression and return the optimized version.
    async fn optimize(&mut self, expr: Expr) -> Result<Expr>;

    /// Set the trace collector for this optimizer.
    /// Default implementation does nothing.
    fn set_trace_collector(&mut self, _collector: Arc<OptimizerTraceCollector>) {
        // Default implementation does nothing
    }
}
