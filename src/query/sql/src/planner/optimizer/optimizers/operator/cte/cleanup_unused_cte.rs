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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::Optimizer;
use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;

/// Optimizer that removes unused CTEs from the query plan.
/// This optimizer should be applied at the end of the optimization pipeline
/// to clean up any CTEs that are not referenced by any MaterializeCTERef.
pub struct CleanupUnusedCTEOptimizer;

impl CleanupUnusedCTEOptimizer {
    /// Collect all CTE names that are referenced by MaterializeCTERef nodes and count their references
    fn collect_referenced_ctes(s_expr: &SExpr) -> Result<HashMap<String, usize>> {
        let mut referenced_ctes = HashMap::new();
        Self::collect_referenced_ctes_recursive(s_expr, &mut referenced_ctes)?;
        Ok(referenced_ctes)
    }

    /// Recursively traverse the expression tree to find MaterializeCTERef nodes and count references
    #[recursive::recursive]
    fn collect_referenced_ctes_recursive(
        s_expr: &SExpr,
        referenced_ctes: &mut HashMap<String, usize>,
    ) -> Result<()> {
        // Check if current node is a MaterializeCTERef
        if let RelOperator::MaterializedCTERef(consumer) = s_expr.plan() {
            *referenced_ctes
                .entry(consumer.cte_name.clone())
                .or_insert(0) += 1;
        }

        // Recursively process children
        for child in s_expr.children() {
            Self::collect_referenced_ctes_recursive(child, referenced_ctes)?;
        }

        Ok(())
    }

    /// Remove unused CTEs from the expression tree and update ref_count
    #[recursive::recursive]
    fn remove_unused_ctes(
        s_expr: &SExpr,
        referenced_ctes: &HashMap<String, usize>,
    ) -> Result<SExpr> {
        if let RelOperator::Sequence(_) = s_expr.plan() {
            let left_child = s_expr.child(0)?.plan().clone();
            if let RelOperator::MaterializedCTE(mut cte) = left_child {
                let ref_count = referenced_ctes.get(&cte.cte_name).cloned().unwrap_or(0);
                if ref_count == 0 {
                    // Return the right child (main query) and skip the left child (CTE definition)
                    let right_child = s_expr.child(1)?;
                    return Self::remove_unused_ctes(right_child, referenced_ctes);
                } else {
                    cte.ref_count = ref_count;
                    // Rebuild the left child with updated ref_count
                    let left_child_expr =
                        SExpr::create_unary(cte, Arc::new(s_expr.child(0)?.child(0)?.clone()));
                    let right_child_expr =
                        Self::remove_unused_ctes(s_expr.child(1)?, referenced_ctes)?;
                    let new_expr = SExpr::create_binary(
                        s_expr.plan().clone(),
                        Arc::new(left_child_expr),
                        Arc::new(right_child_expr),
                    );
                    return Ok(new_expr);
                }
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
        // Collect all referenced CTEs and their ref_count
        let referenced_ctes = Self::collect_referenced_ctes(s_expr)?;

        // Remove unused CTEs and update ref_count
        Self::remove_unused_ctes(s_expr, &referenced_ctes)
    }
}
