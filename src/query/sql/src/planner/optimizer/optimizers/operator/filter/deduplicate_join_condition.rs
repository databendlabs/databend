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

use crate::optimizer::ir::SExpr;
use crate::optimizer::Optimizer;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::ScalarExpr;

// The DeduplicateJoinConditionOptimizer uses the Union-Find algorithm to remove duplicate join conditions.
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t2.id = t3.id AND t3.id = t1.id
//
// Initial join tree:
//    Join [t2.id = t3.id, t3.id = t1.id]
//    /  \
//   t3   \
//       Join [t1.id = t2.id]
//       /  \
//      t1  t2
//
// How Union-Find works:
//
// 1. Initial state:
//    Each column is its own group representative:
//    column_group = {} (empty, each column points to itself by default)
//
// 2. Process condition "t1.id = t2.id":
//    - find(t1.id) returns t1.id (its own group)
//    - find(t2.id) returns t2.id (its own group)
//    - Different groups, so union(t1.id, t2.id) merges them
//    - column_group = {t2.id → t1.id} (t2.id now belongs to t1.id's group)
//    - Keep this condition
//
// 3. Process condition "t2.id = t3.id":
//    - find(t2.id) returns t1.id (follows column_group[t2.id])
//    - find(t3.id) returns t3.id (its own group)
//    - Different groups, so union(t1.id, t3.id) merges them
//    - column_group = {t2.id → t1.id, t3.id → t1.id} (t3.id now belongs to t1.id's group)
//    - Keep this condition
//
// 4. Process condition "t3.id = t1.id":
//    - find(t3.id) returns t1.id (follows column_group[t3.id])
//    - find(t1.id) returns t1.id (its own group)
//    - Same group! This means the condition is redundant
//    - Skip this condition
//
// Final state:
//    column_group = {t2.id → t1.id, t3.id → t1.id}
//    All three columns are in the same group with t1.id as the representative
//
// Final join tree after optimization:
//    Join [t2.id = t3.id]
//    /  \
//   t3   \
//       Join [t1.id = t2.id]
//       /  \
//      t1  t2
//
pub struct DeduplicateJoinConditionOptimizer {
    /// Maps scalar expressions to unique indices
    expr_to_index: HashMap<ScalarExpr, usize>,
    /// Maps each column index to its group representative index
    /// If column_group[x] = x, then x is a group representative
    /// Otherwise, column_group[x] points to another column in the same group
    column_group: HashMap<usize, usize>,
    /// Next available index
    next_index: usize,
}

impl Default for DeduplicateJoinConditionOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicateJoinConditionOptimizer {
    pub fn new() -> Self {
        Self {
            expr_to_index: HashMap::new(),
            column_group: HashMap::new(),
            next_index: 0,
        }
    }

    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.deduplicate(s_expr)
    }

    #[recursive::recursive]
    pub fn deduplicate(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            // Only optimize filtering joins that don't preserve nulls
            RelOperator::Join(join) if join.join_type.is_filtering_join() => {
                self.optimize_filtering_join(s_expr, join)
            }
            // Recursively process other nodes
            _ => self.deduplicate_children(s_expr),
        }
    }

    /// Optimize filtering joins (inner/semi/anti) by removing redundant equi-conditions
    fn optimize_filtering_join(&mut self, s_expr: &SExpr, join: &Join) -> Result<SExpr> {
        debug_assert!(join.join_type.is_filtering_join());

        // Recursively optimize left and right subtrees
        let left = self.deduplicate(s_expr.child(0)?)?;
        let right = self.deduplicate(s_expr.child(1)?)?;

        let mut join = join.clone();
        let mut non_redundant_conditions = Vec::new();
        // Anti / Semi joins should not contribute new equivalence to ancestor nodes.
        let snapshot = if matches!(
            join.join_type,
            JoinType::LeftAnti | JoinType::RightAnti | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            Some(self.snapshot())
        } else {
            None
        };

        // Check each equi-join condition
        for condition in &join.equi_conditions {
            let left_idx = self.get_or_create_index(&condition.left);
            let right_idx = self.get_or_create_index(&condition.right);

            // Find the group representatives for both columns
            let left_group = self.find(left_idx);
            let right_group = self.find(right_idx);

            // If columns are not in the same group, this condition is not redundant
            if left_group != right_group {
                // Merge the two groups
                self.union(left_group, right_group);
                non_redundant_conditions.push(condition.clone());
            }
            // If already in the same group, the condition is redundant and can be skipped
        }

        // Update join conditions if any were removed
        if non_redundant_conditions.len() < join.equi_conditions.len() {
            join.equi_conditions = non_redundant_conditions;
        }

        // Restore union-find state for anti joins to avoid leaking equivalence upward.
        if let Some(snapshot) = snapshot {
            self.restore(snapshot);
        }

        // Create new expression
        let new_plan = Arc::new(RelOperator::Join(join));
        let new_children = vec![Arc::new(left), Arc::new(right)];

        Ok(s_expr.replace_plan(new_plan).replace_children(new_children))
    }

    /// Recursively process children nodes
    fn deduplicate_children(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.arity());
        let mut children_changed = false;

        for child in s_expr.children() {
            let optimized_child = self.deduplicate(child)?;
            if !optimized_child.eq(child) {
                children_changed = true;
            }
            children.push(Arc::new(optimized_child));
        }

        if !children_changed {
            return Ok(s_expr.clone());
        }

        Ok(s_expr.replace_children(children))
    }

    /// Get index for expression, creating a new one if it doesn't exist
    fn get_or_create_index(&mut self, expr: &ScalarExpr) -> usize {
        if let Some(&idx) = self.expr_to_index.get(expr) {
            return idx;
        }

        let idx = self.next_index;
        self.expr_to_index.insert(expr.clone(), idx);
        self.next_index += 1;
        idx
    }

    /// Union-Find: Find the group representative for a column
    ///
    /// This function follows the chain of group pointers until it finds the
    /// representative (root) of the group. It also performs path compression,
    /// which makes future lookups faster by pointing all nodes directly to the root.
    ///
    /// For example, if we have:
    ///   column_group = {t2.id → t1.id, t3.id → t2.id}
    /// Then find(t3.id) would:
    ///   1. Look up column_group[t3.id] = t2.id
    ///   2. Look up column_group[t2.id] = t1.id
    ///   3. Look up column_group[t1.id] (not found, so t1.id is its own representative)
    ///   4. Update column_group[t3.id] = t1.id (path compression)
    ///   5. Return t1.id as the group representative
    fn find(&mut self, idx: usize) -> usize {
        // Get a copy of the group index to avoid multiple mutable borrows
        let group_idx = *self.column_group.entry(idx).or_insert(idx);

        if group_idx != idx {
            // Not a root node, recursively find the group representative
            let root = self.find(group_idx);
            // Path compression: point directly to the root
            self.column_group.insert(idx, root);
            root
        } else {
            // This is a root node (its own representative)
            idx
        }
    }

    /// Union-Find: Merge two groups by making one representative the parent of the other
    ///
    /// After union(a, b), columns in group b will have a as their representative.
    /// This effectively merges the equivalence groups.
    ///
    /// For example, if we have:
    ///   column_group = {t2.id → t1.id} (t2.id belongs to t1.id's group)
    ///   column_group = {t4.id → t3.id} (t4.id belongs to t3.id's group)
    /// Then union(t1.id, t3.id) would:
    ///   1. Set column_group[t3.id] = t1.id
    ///   2. Result in column_group = {t2.id → t1.id, t3.id → t1.id, t4.id → t3.id}
    ///   3. After path compression, it would become: {t2.id → t1.id, t3.id → t1.id, t4.id → t1.id}
    fn union(&mut self, idx1: usize, idx2: usize) {
        self.column_group.insert(idx2, idx1);
    }

    /// Snapshot the current union-find state so we can rollback after
    /// optimizing an anti join.
    fn snapshot(&self) -> UfSnapshot {
        UfSnapshot {
            expr_to_index: self.expr_to_index.clone(),
            column_group: self.column_group.clone(),
            next_index: self.next_index,
        }
    }

    fn restore(&mut self, snapshot: UfSnapshot) {
        self.expr_to_index = snapshot.expr_to_index;
        self.column_group = snapshot.column_group;
        self.next_index = snapshot.next_index;
    }
}

struct UfSnapshot {
    expr_to_index: HashMap<ScalarExpr, usize>,
    column_group: HashMap<usize, usize>,
    next_index: usize,
}

#[async_trait::async_trait]
impl Optimizer for DeduplicateJoinConditionOptimizer {
    fn name(&self) -> String {
        "DeduplicateJoinConditionOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
