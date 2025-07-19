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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::Optimizer;
use crate::plans::RelOperator;

/// Optimizer that removes unused CTEs from the query plan.
/// This optimizer should be applied at the end of the optimization pipeline
/// to clean up any CTEs that are not referenced by any CTEConsumer.
pub struct CleanupUnusedCTEOptimizer;

impl CleanupUnusedCTEOptimizer {
    /// Collect all CTE names that are referenced by CTEConsumer nodes
    fn collect_referenced_ctes(s_expr: &SExpr) -> Result<HashSet<String>> {
        let mut referenced_ctes = HashSet::new();
        Self::collect_referenced_ctes_recursive(s_expr, &mut referenced_ctes)?;
        Ok(referenced_ctes)
    }

    /// Recursively traverse the expression tree to find CTEConsumer nodes
    fn collect_referenced_ctes_recursive(
        s_expr: &SExpr,
        referenced_ctes: &mut HashSet<String>,
    ) -> Result<()> {
        // Check if current node is a CTEConsumer
        if let RelOperator::CTEConsumer(consumer) = s_expr.plan() {
            referenced_ctes.insert(consumer.cte_name.clone());
        }

        // Recursively process children
        for child in s_expr.children() {
            Self::collect_referenced_ctes_recursive(child, referenced_ctes)?;
        }

        Ok(())
    }

    /// Remove unused CTEs from the expression tree
    fn remove_unused_ctes(s_expr: &SExpr, referenced_ctes: &HashSet<String>) -> Result<SExpr> {
        if let RelOperator::MaterializedCTE(m_cte) = s_expr.plan() {
            // If this CTE is not referenced, remove it by returning the right child
            if !referenced_ctes.contains(&m_cte.cte_name) {
                // Return the right child (main query) and skip the left child (CTE definition)
                let right_child = s_expr.child(1)?;
                return Self::remove_unused_ctes(right_child, referenced_ctes);
            }
        }

        // Process children recursively
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let optimized_child = Self::remove_unused_ctes(child, referenced_ctes)?;
            optimized_children.push(Arc::new(optimized_child));
        }

        // Create new expression with optimized children
        let mut new_expr = s_expr.clone();
        new_expr.children = optimized_children;
        Ok(new_expr)
    }
}

#[async_trait::async_trait]
impl Optimizer for CleanupUnusedCTEOptimizer {
    fn name(&self) -> String {
        "CleanupUnusedCTEOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        // Collect all referenced CTEs
        let referenced_ctes = Self::collect_referenced_ctes(s_expr)?;

        // Remove unused CTEs
        Self::remove_unused_ctes(s_expr, &referenced_ctes)
    }
}
