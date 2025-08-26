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
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::OptimizerContext;
use crate::plans::walk_expr_mut;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Sequence;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::Visibility;

const ID: RuleID = RuleID::HierarchicalGroupingSetsToUnion;

/// True hierarchical optimization for GROUPING SETS with multi-layer dependency analysis.
///
/// This implements genuine hierarchical aggregation where higher-level groupings
/// are computed from lower-level aggregated results, not from raw data.
///
/// Example for CUBE(a,b,c):
/// Level 0: Raw data -> GROUP BY (a,b,c) -> CTE_level_3
/// Level 1: CTE_level_3 -> GROUP BY (a,b), (a,c), (b,c) -> CTE_level_2_*
/// Level 2: CTE_level_2_* -> GROUP BY (a), (b), (c) -> CTE_level_1_*
/// Level 3: CTE_level_1_* -> GROUP BY () -> CTE_level_0
/// Final: UNION ALL of all results
pub struct RuleHierarchicalGroupingSetsToUnion {
    id: RuleID,
    matchers: Vec<Matcher>,
    ctx: Arc<OptimizerContext>,
}

impl RuleHierarchicalGroupingSetsToUnion {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        Self {
            id: ID,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::Leaf],
                }],
            }],
            ctx,
        }
    }

    /// Analyzes grouping sets to build a true hierarchical dependency DAG
    fn build_hierarchy_dag(&self, grouping_sets: &[Vec<IndexType>]) -> HierarchyDAG {
        let mut levels: Vec<GroupingLevel> = grouping_sets
            .iter()
            .enumerate()
            .map(|(idx, set)| GroupingLevel {
                set_index: idx,
                columns: set.clone(),
                direct_children: Vec::new(),
                possible_parents: Vec::new(),
                chosen_parent: None,
                level: set.len(),
            })
            .collect();

        // Sort by specificity (most detailed first)
        levels.sort_by(|a, b| b.level.cmp(&a.level));

        // Build parent-child relationships
        for i in 0..levels.len() {
            for j in (i + 1)..levels.len() {
                // Check if levels[j] is a proper subset of levels[i]
                if is_proper_subset(&levels[j].columns, &levels[i].columns) {
                    levels[j].direct_children.push(i);
                    levels[i].possible_parents.push(j);
                }
            }
        }

        // Choose optimal parents to minimize CTE count
        self.optimize_hierarchy(&mut levels);

        HierarchyDAG { levels }
    }

    /// Optimize the hierarchy to minimize intermediate CTEs while maximizing reuse
    fn optimize_hierarchy(&self, levels: &mut [GroupingLevel]) {
        // For each level, choose the less detailed parent that can generate it
        for i in 0..levels.len() {
            if !levels[i].possible_parents.is_empty() {
                // Choose the parent with minimum columns (less detailed)
                // possible_parents contains indices into the levels array
                let best_parent_level_idx = *levels[i]
                    .possible_parents
                    .iter()
                    .min_by_key(|&&parent_idx| levels[parent_idx].level)
                    .unwrap();

                // Store the set_index of the chosen parent, not the level index
                levels[i].chosen_parent = Some(levels[best_parent_level_idx].set_index);
            }
        }
    }

    /// Check if aggregates are suitable for hierarchical optimization
    fn can_optimize_hierarchically(&self, agg_functions: &[ScalarItem]) -> bool {
        agg_functions.iter().all(|item| {
            if let ScalarExpr::AggregateFunction(agg) = &item.scalar {
                // Only functions that support hierarchical aggregation
                matches!(agg.func_name.as_str(), "sum" | "count" | "min" | "max")
            } else {
                false
            }
        })
    }

    /// Build the complete hierarchical plan with true layering
    fn build_true_hierarchical_plan(
        &self,
        eval_scalar: &EvalScalar,
        agg: &Aggregate,
        agg_input: &SExpr,
        hierarchy: HierarchyDAG,
        base_cte_name: String,
        agg_input_columns: &[IndexType],
    ) -> Result<SExpr> {
        // Step 1: Create base CTE for most detailed aggregation
        let base_cte = self.create_base_cte(&base_cte_name, agg_input, &hierarchy, agg)?;

        // Step 2: Build intermediate CTEs in dependency order
        let intermediate_ctes =
            self.build_intermediate_ctes(&hierarchy, agg, &base_cte_name, agg_input_columns)?;

        // Step 3: Build final union branches
        let union_branches =
            self.build_final_union_branches(eval_scalar, &hierarchy, agg, &base_cte_name)?;

        // Step 4: Assemble the complete plan
        let union_result = self.create_union_all(&union_branches, eval_scalar)?;

        // Step 5: Chain all CTEs in correct dependency order
        let mut result = union_result;

        // Add intermediate CTEs (in dependency order - parents first)
        for cte in intermediate_ctes {
            result = SExpr::create_binary(Sequence, cte, result);
        }

        // Add base CTE
        result = SExpr::create_binary(Sequence, base_cte, result);

        Ok(result)
    }

    /// Create the base CTE that materializes the most detailed aggregation
    fn create_base_cte(
        &self,
        cte_name: &str,
        agg_input: &SExpr,
        hierarchy: &HierarchyDAG,
        agg: &Aggregate,
    ) -> Result<SExpr> {
        let channel_size = self
            .ctx
            .get_table_ctx()
            .get_settings()
            .get_grouping_sets_channel_size()
            .unwrap_or(2);

        // Get the most detailed level (largest grouping set)
        let most_detailed_level = hierarchy
            .levels
            .iter()
            .max_by_key(|level| level.level)
            .unwrap();

        // Create aggregate plan for the most detailed level
        let mut base_agg = agg.clone();
        base_agg.grouping_sets = None;
        base_agg
            .group_items
            .retain(|item| most_detailed_level.columns.contains(&item.index));

        // IMPORTANT: Remove any grouping_id related aggregate functions
        // Hierarchical optimization doesn't materialize grouping_id in CTEs
        base_agg.aggregate_functions.retain(|func| {
            // Only keep actual aggregate functions, not synthetic ones like grouping_id
            if let ScalarExpr::AggregateFunction(agg_func) = &func.scalar {
                matches!(
                    agg_func.func_name.as_str(),
                    "sum" | "count" | "min" | "max" | "avg"
                )
            } else {
                true // Keep other expressions
            }
        });

        let base_aggregation = SExpr::create_unary(base_agg, agg_input.clone());

        Ok(SExpr::create_unary(
            MaterializedCTE::new(cte_name.to_string(), None, Some(channel_size as usize)),
            base_aggregation,
        ))
    }

    /// Build intermediate CTEs that perform hierarchical aggregation
    fn build_intermediate_ctes(
        &self,
        hierarchy: &HierarchyDAG,
        agg: &Aggregate,
        base_cte_name: &str,
        _agg_input_columns: &[IndexType],
    ) -> Result<Vec<SExpr>> {
        let mut ctes = Vec::new();
        let mut level_to_cte_name = HashMap::new();

        // Base level CTE name (most detailed level)
        let most_detailed_idx = hierarchy.get_most_detailed_level();
        level_to_cte_name.insert(most_detailed_idx, base_cte_name.to_string());

        // Sort levels by decreasing specificity for proper topological order
        let mut sorted_levels = hierarchy.levels.clone();
        sorted_levels.sort_by(|a, b| b.level.cmp(&a.level));

        // Build CTEs in topological order (most detailed first, excluding base level)
        for level in sorted_levels.iter() {
            // Skip the most detailed level (base CTE)
            if level.set_index == most_detailed_idx {
                continue;
            }

            // Get parent's set_index and find its CTE name
            let parent_set_idx = level.chosen_parent.unwrap_or(most_detailed_idx);
            let parent_cte_name = level_to_cte_name
                .get(&parent_set_idx)
                .cloned()
                .unwrap_or_else(|| base_cte_name.to_string());

            let current_cte_name = self.generate_unique_cte_name(base_cte_name, &level.columns);

            // Build aggregation from parent CTE
            let aggregation_plan = self.build_hierarchical_aggregation(
                agg,
                &level.columns,
                &parent_cte_name,
                &self.get_parent_output_columns_safe(hierarchy, parent_set_idx, agg),
            )?;

            // Always create CTE for intermediate levels (not the final union branches)
            let cte = SExpr::create_unary(
                MaterializedCTE::new(current_cte_name.clone(), None, Some(2)),
                aggregation_plan,
            );
            ctes.push(cte);
            level_to_cte_name.insert(level.set_index, current_cte_name);
        }

        Ok(ctes)
    }

    /// Build hierarchical aggregation that consumes parent's pre-aggregated results
    fn build_hierarchical_aggregation(
        &self,
        agg: &Aggregate,
        group_columns: &[IndexType],
        parent_cte_name: &str,
        parent_output_columns: &[IndexType],
    ) -> Result<SExpr> {
        // Build column mapping: parent CTE column indices -> expected column indices
        let mut column_mapping = HashMap::new();
        for (i, &col_idx) in parent_output_columns.iter().enumerate() {
            column_mapping.insert(col_idx, i);
        }

        // Create consumer for parent CTE
        let parent_consumer = SExpr::create_leaf(MaterializedCTERef {
            cte_name: parent_cte_name.to_string(),
            output_columns: parent_output_columns.to_vec(),
            def: SExpr::create_leaf(Arc::new(DummyTableScan.into())),
            column_mapping,
        });

        // Build re-aggregation plan
        let mut reagg_plan = agg.clone();
        reagg_plan.grouping_sets = None;
        reagg_plan
            .group_items
            .retain(|x| group_columns.contains(&x.index));

        // Replace column references in group items to point to parent CTE output positions
        for group_item in &mut reagg_plan.group_items {
            // For group items, if they exist in parent output, replace entirely with BoundColumnRef
            if let Some(_pos) = parent_output_columns
                .iter()
                .position(|&idx| idx == group_item.index)
            {
                // Replace the entire scalar expression with a direct column reference
                // Use the original data type but make it nullable to handle CTE output
                let original_type = group_item.scalar.data_type()?;
                group_item.scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        format!("group_item_{}", group_item.index),
                        group_item.index,
                        original_type.into(),
                        Visibility::Visible,
                    )
                    .build(),
                });
            } else {
                // If not found in parent output, use the visitor for complex expressions
                self.replace_column_refs_with_parent_indices(
                    &mut group_item.scalar,
                    parent_output_columns,
                );
            }
        }

        // Transform aggregate functions for hierarchical aggregation
        for (_, item) in reagg_plan.aggregate_functions.iter_mut().enumerate() {
            if let ScalarExpr::AggregateFunction(ref mut agg_func) = item.scalar {
                // Replace column references in aggregate function arguments
                for arg in &mut agg_func.args {
                    self.replace_column_refs_with_parent_indices(arg, parent_output_columns);
                }

                // The key insight: we're now aggregating pre-aggregated values
                match agg_func.func_name.as_str() {
                    "sum" => {
                        // SUM(pre_computed_sum) -> SUM
                        // This works because sum is associative
                    }
                    "count" => {
                        // COUNT(*) from pre-aggregated -> SUM(pre_computed_count)
                        agg_func.func_name = "sum".to_string();
                    }
                    "min" => {
                        // MIN(pre_computed_min) -> MIN
                        // This works because min is associative
                    }
                    "max" => {
                        // MAX(pre_computed_max) -> MAX
                        // This works because max is associative
                    }
                    _ => {
                        // Other functions - leave as is for now
                    }
                }
            }
        }

        Ok(SExpr::create_unary(reagg_plan, parent_consumer))
    }

    /// Replace column references with parent CTE column indices using VisitorMut
    fn replace_column_refs_with_parent_indices(
        &self,
        expr: &mut ScalarExpr,
        parent_output_columns: &[IndexType],
    ) {
        let mut visitor = ColumnReferenceReplacer {
            parent_output_columns,
        };
        let _ = visitor.visit(expr);
    }

    /// Generate a unique CTE name based on the grouping columns
    fn generate_unique_cte_name(&self, base_name: &str, columns: &[IndexType]) -> String {
        if columns.is_empty() {
            return format!("{}_empty", base_name);
        }

        // Create a unique identifier based on the actual column indices
        let mut columns_sorted = columns.to_vec();
        columns_sorted.sort();
        let columns_str = columns_sorted
            .iter()
            .map(|idx| idx.to_string())
            .collect::<Vec<_>>()
            .join("_");

        format!("{}_cols_{}", base_name, columns_str)
    }

    /// Safe version that handles missing parent indices
    fn get_parent_output_columns_safe(
        &self,
        hierarchy: &HierarchyDAG,
        parent_set_idx: usize,
        agg: &Aggregate,
    ) -> Vec<IndexType> {
        // Find the level with matching set_index
        for level in &hierarchy.levels {
            if level.set_index == parent_set_idx {
                let mut columns = Vec::new();

                // Always add ALL group columns that the parent CTE outputs (not just the ones this level needs)
                // This ensures MaterializedCTERef specifies all columns that the source CTE outputs
                for group_item in &agg.group_items {
                    columns.push(group_item.index);
                }
                // Add aggregate result columns
                for agg_func in &agg.aggregate_functions {
                    columns.push(agg_func.index);
                }

                return columns;
            }
        }

        // Fallback: return all group columns + aggregate columns
        let mut columns = Vec::new();
        for group_item in &agg.group_items {
            columns.push(group_item.index);
        }
        for agg_func in &agg.aggregate_functions {
            columns.push(agg_func.index);
        }
        columns
    }

    /// Get all output columns for a specific CTE (regardless of what the current level needs)
    fn get_cte_output_columns(
        &self,
        hierarchy: &HierarchyDAG,
        cte_set_idx: usize,
        agg: &Aggregate,
    ) -> Vec<IndexType> {
        // For the base CTE (most detailed level), return all its output columns
        let most_detailed_idx = hierarchy.get_most_detailed_level();
        if cte_set_idx == most_detailed_idx {
            // Base CTE always contains all group columns + all aggregate columns
            let mut columns = Vec::new();
            // Add all group columns (in their original order)
            for group_item in &agg.group_items {
                columns.push(group_item.index);
            }
            // Add all aggregate result columns
            for agg_func in &agg.aggregate_functions {
                columns.push(agg_func.index);
            }
            return columns;
        }

        // For intermediate CTEs, they also contain all original columns
        // The structure is: all group columns + all aggregate columns (same as base)
        // No projection is done, only ID remapping
        let mut columns = Vec::new();
        // Add all group columns (in their original order)
        for group_item in &agg.group_items {
            columns.push(group_item.index);
        }
        // Add all aggregate result columns
        for agg_func in &agg.aggregate_functions {
            columns.push(agg_func.index);
        }
        columns
    }

    /// Build final union branches with proper grouping sets semantics
    fn build_final_union_branches(
        &self,
        eval_scalar: &EvalScalar,
        hierarchy: &HierarchyDAG,
        agg: &Aggregate,
        base_cte_name: &str,
    ) -> Result<Vec<SExpr>> {
        let mut branches = Vec::new();
        let mut level_to_cte_name = HashMap::new();

        // Map levels to their CTE names
        level_to_cte_name.insert(
            hierarchy.get_most_detailed_level(),
            base_cte_name.to_string(),
        );
        for level in &hierarchy.levels {
            if level.chosen_parent.is_some() {
                let cte_name = self.generate_unique_cte_name(base_cte_name, &level.columns);
                level_to_cte_name.insert(level.set_index, cte_name);
            }
        }

        // Build final branch for each grouping set
        for level in &hierarchy.levels {
            let (source_cte_name, source_cte_set_idx) =
                if level.set_index == hierarchy.get_most_detailed_level() {
                    // Most detailed level - read from base CTE
                    (
                        base_cte_name.to_string(),
                        hierarchy.get_most_detailed_level(),
                    )
                } else {
                    // Other levels - read from their own CTE if it exists
                    if let Some(cte_name) = level_to_cte_name.get(&level.set_index).cloned() {
                        (cte_name, level.set_index)
                    } else {
                        // If no CTE exists for this level, read from base CTE
                        (
                            base_cte_name.to_string(),
                            hierarchy.get_most_detailed_level(),
                        )
                    }
                };

            let branch = self.build_final_branch(
                eval_scalar,
                &level.columns,
                &source_cte_name,
                &self.get_cte_output_columns(hierarchy, source_cte_set_idx, agg),
                level.set_index,
                agg,
            )?;

            branches.push(branch);
        }

        Ok(branches)
    }

    /// Build a final branch that applies grouping sets NULL semantics
    fn build_final_branch(
        &self,
        eval_scalar: &EvalScalar,
        group_columns: &[IndexType],
        source_cte_name: &str,
        source_output_columns: &[IndexType],
        set_index: usize,
        agg: &Aggregate,
    ) -> Result<SExpr> {
        // Build column mapping for source CTE
        let mut column_mapping = HashMap::new();
        for (i, &col_idx) in source_output_columns.iter().enumerate() {
            column_mapping.insert(col_idx, i);
        }

        // Create consumer for source CTE
        let source_consumer = SExpr::create_leaf(MaterializedCTERef {
            cte_name: source_cte_name.to_string(),
            output_columns: source_output_columns.to_vec(),
            def: SExpr::create_leaf(Arc::new(DummyTableScan.into())),
            column_mapping,
        });

        // Apply grouping sets NULL semantics in EvalScalar
        let mut eval_scalar_plan = eval_scalar.clone();
        self.apply_grouping_sets_semantics(&mut eval_scalar_plan, group_columns, agg, set_index)?;

        Ok(SExpr::create_unary(eval_scalar_plan, source_consumer))
    }

    /// Apply NULL semantics for columns not in the current grouping set
    fn apply_grouping_sets_semantics(
        &self,
        eval_scalar: &mut EvalScalar,
        group_columns: &[IndexType],
        agg: &Aggregate,
        _set_index: usize,
    ) -> Result<()> {
        let grouping_id = self.calculate_grouping_id(group_columns, &agg.group_items);

        let null_group_ids: Vec<IndexType> = agg
            .group_items
            .iter()
            .map(|i| i.index)
            .filter(|index| !group_columns.contains(index))
            .collect();

        let mut visitor = GroupingSetsNullVisitor {
            group_indexes: group_columns.to_vec(),
            exclude_group_indexes: null_group_ids,
            // Don't try to access grouping_id from CTE - it's not materialized there
            grouping_id_index: IndexType::MAX, /* Use invalid index to disable grouping_id replacement */
            grouping_id_value: grouping_id,
        };

        for scalar in eval_scalar.items.iter_mut() {
            visitor.visit(&mut scalar.scalar)?;
        }

        Ok(())
    }

    /// Create UNION ALL combining all final branches
    fn create_union_all(&self, branches: &[SExpr], eval_scalar: &EvalScalar) -> Result<SExpr> {
        if branches.is_empty() {
            return Err(databend_common_exception::ErrorCode::Internal(
                "No branches for union".to_string(),
            ));
        }

        let mut result = branches[0].clone();
        for branch in branches.iter().skip(1) {
            let left_outputs: Vec<(IndexType, Option<ScalarExpr>)> =
                eval_scalar.items.iter().map(|x| (x.index, None)).collect();
            let right_outputs = left_outputs.clone();

            let union_plan = UnionAll {
                left_outputs,
                right_outputs,
                cte_scan_names: vec![],
                output_indexes: eval_scalar.items.iter().map(|x| x.index).collect(),
            };
            result = SExpr::create_binary(Arc::new(union_plan.into()), result, branch.clone());
        }

        Ok(result)
    }

    fn calculate_grouping_id(&self, group_columns: &[IndexType], all_groups: &[ScalarItem]) -> u32 {
        let mask = (1 << all_groups.len()) - 1;
        let mut id = 0;

        for (i, group_item) in all_groups.iter().enumerate() {
            if group_columns.contains(&group_item.index) {
                id |= 1 << i;
            }
        }
        !id & mask
    }
}

impl Rule for RuleHierarchicalGroupingSetsToUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        let agg: Aggregate = s_expr.child(0)?.plan().clone().try_into()?;

        if agg.mode != AggregateMode::Initial {
            return Ok(());
        }

        let Some(grouping_sets) = &agg.grouping_sets else {
            return Ok(());
        };

        // Reduce the requirement to at least 2 grouping sets for meaningful hierarchy
        if grouping_sets.sets.len() < 2 {
            return Ok(());
        }

        // Check if aggregates support hierarchical optimization
        if !self.can_optimize_hierarchically(&agg.aggregate_functions) {
            return Ok(());
        }

        let agg_input = s_expr.child(0)?.child(0)?;
        let agg_input_columns: Vec<IndexType> = RelExpr::with_s_expr(agg_input)
            .derive_relational_prop()?
            .output_columns
            .iter()
            .cloned()
            .collect();

        // Build hierarchy DAG
        let hierarchy = self.build_hierarchy_dag(&grouping_sets.sets);

        // Check if we have meaningful hierarchical structure
        let hierarchical_levels = hierarchy
            .levels
            .iter()
            .filter(|level| level.chosen_parent.is_some())
            .count();

        if hierarchical_levels < 1 {
            return Ok(()); // Not enough hierarchy to justify optimization
        }

        // Generate unique base CTE name
        let mut hasher = DefaultHasher::new();
        agg.grouping_sets.hash(&mut hasher);
        let hash = hasher.finish();
        let base_cte_name = format!("cte_hierarchical_groupingsets_{}", hash);

        // Build the true hierarchical plan
        let result = self.build_true_hierarchical_plan(
            &eval_scalar,
            &agg,
            agg_input,
            hierarchy,
            base_cte_name,
            &agg_input_columns,
        )?;

        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

#[derive(Debug, Clone)]
struct GroupingLevel {
    set_index: usize,
    columns: Vec<IndexType>,
    direct_children: Vec<usize>,
    possible_parents: Vec<usize>,
    chosen_parent: Option<usize>,
    level: usize,
}

#[derive(Debug)]
struct HierarchyDAG {
    levels: Vec<GroupingLevel>,
}

impl HierarchyDAG {
    fn get_most_detailed_level(&self) -> usize {
        self.levels
            .iter()
            .max_by_key(|level| level.level)
            .map(|level| level.set_index)
            .unwrap_or(0)
    }
}

/// Check if subset is a proper subset of superset
fn is_proper_subset(subset: &[IndexType], superset: &[IndexType]) -> bool {
    subset.len() < superset.len() && subset.iter().all(|item| superset.contains(item))
}

struct GroupingSetsNullVisitor {
    group_indexes: Vec<IndexType>,
    exclude_group_indexes: Vec<IndexType>,
    grouping_id_index: IndexType,
    grouping_id_value: u32,
}

impl VisitorMut<'_> for GroupingSetsNullVisitor {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        let old = expr.clone();

        if let ScalarExpr::BoundColumnRef(col) = expr {
            if self.group_indexes.contains(&col.column.index) {
                // Keep the column but make it nullable
                *expr = ScalarExpr::CastExpr(CastExpr {
                    argument: Box::new(old),
                    is_try: true,
                    target_type: Box::new(col.column.data_type.wrap_nullable()),
                    span: col.span,
                });
            } else if self.exclude_group_indexes.contains(&col.column.index) {
                // Replace with NULL for excluded grouping columns
                *expr = ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        value: Scalar::Null,
                        span: col.span,
                    },
                    col.column.data_type.wrap_nullable(),
                );
            } else if self.grouping_id_index != IndexType::MAX
                && self.grouping_id_index == col.column.index
            {
                // Only replace grouping ID if we have a valid index
                // (This case should not occur in hierarchical optimization since grouping_id is not materialized in CTEs)
                *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Number(NumberScalar::UInt32(self.grouping_id_value)),
                    span: col.span,
                });
            }
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
}

struct ColumnReferenceReplacer<'a> {
    parent_output_columns: &'a [IndexType],
}

impl VisitorMut<'_> for ColumnReferenceReplacer<'_> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::BoundColumnRef(col_ref) => {
                // Find the position of this column in parent output
                if let Some(pos) = self
                    .parent_output_columns
                    .iter()
                    .position(|&idx| idx == col_ref.column.index)
                {
                    // Replace with new column index that points to parent CTE output
                    col_ref.column.index = pos;
                }
                Ok(())
            }
            _ => {
                // Continue visiting child expressions
                walk_expr_mut(self, expr)
            }
        }
    }
}
