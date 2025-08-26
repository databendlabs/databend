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
use crate::plans::RelOperator;
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
                    // levels[j] (less detailed) is a child of levels[i] (more detailed)
                    // So levels[i] is a possible parent for levels[j]
                    levels[i].direct_children.push(j);
                    levels[j].possible_parents.push(i);
                }
            }
        }

        // Choose optimal parents to minimize CTE count
        self.optimize_hierarchy(&mut levels);

        HierarchyDAG { levels }
    }

    /// Optimize the hierarchy to minimize intermediate CTEs while maximizing reuse
    fn optimize_hierarchy(&self, levels: &mut [GroupingLevel]) {
        // For each level, choose the most detailed parent that can generate it
        for i in 0..levels.len() {
            if !levels[i].possible_parents.is_empty() {
                // Choose the parent with maximum columns (most detailed)
                // This ensures we reuse the most specific aggregation available
                let best_parent_level_idx = *levels[i]
                    .possible_parents
                    .iter()
                    .max_by_key(|&&parent_idx| levels[parent_idx].level)
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
    ) -> Result<SExpr> {
        // Step 1: Create base CTE for most detailed aggregation
        let base_cte = self.create_base_cte(&base_cte_name, agg_input, &hierarchy, agg)?;

        // Step 2: Build intermediate CTEs in dependency order
        let intermediate_ctes =
            self.build_intermediate_ctes(&hierarchy, agg, &base_cte, &base_cte_name)?;

        // Step 3: Build final union branches
        let union_branches = self.build_final_union_branches(
            eval_scalar,
            &hierarchy,
            agg,
            &base_cte_name,
            &base_cte,
            &intermediate_ctes,
        )?;

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
        base_cte: &SExpr,
        base_cte_name: &str,
    ) -> Result<Vec<SExpr>> {
        let mut ctes = Vec::new();
        let mut cte_names = Vec::new();
        let mut level_to_cte_name = HashMap::new();

        // Base level CTE name (most detailed level)
        let most_detailed_idx = hierarchy.get_most_detailed_level();
        level_to_cte_name.insert(most_detailed_idx, base_cte_name.to_string());

        // Sort levels by decreasing specificity for proper topological order
        let mut sorted_levels = hierarchy.levels.clone();
        sorted_levels.sort_by(|a, b| b.level.cmp(&a.level));

        // Build CTEs in topological order (most detailed first, excluding base level)
        for level in sorted_levels.iter() {
            println!("Processing level in build_intermediate_ctes: set_index={}, columns={:?}, chosen_parent={:?}",
                     level.set_index, level.columns, level.chosen_parent);

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
            let parent_group_level = hierarchy.get_by_set_index(parent_set_idx).unwrap();

            // Build aggregation from parent CTE
            let actual_output_columns: Vec<IndexType> = {
                let mut columns = Vec::new();
                for agg in &agg.aggregate_functions {
                    columns.push(agg.index);
                }
                columns.extend_from_slice(&parent_group_level.columns);
                columns
            };

            let aggregation_plan = self.build_hierarchical_aggregation(
                agg,
                &level.columns,
                &parent_cte_name,
                &actual_output_columns,
            )?;

            // Always create CTE for intermediate levels (not the final union branches)
            let cte = SExpr::create_unary(
                MaterializedCTE::new(current_cte_name.clone(), None, Some(2)),
                aggregation_plan,
            );
            ctes.push(cte);
            cte_names.push(current_cte_name.clone());
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
        let mut column_mapping = HashMap::new();
        for logical_index in parent_output_columns.iter() {
            column_mapping.insert(*logical_index, *logical_index);
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
            }
        }

        // Transform aggregate functions for hierarchical aggregation
        for (_, item) in reagg_plan.aggregate_functions.iter_mut().enumerate() {
            if let ScalarExpr::AggregateFunction(ref mut agg_func) = item.scalar {
                // Replace column references in aggregate function arguments
                for arg in &mut agg_func.args {
                    if let ScalarExpr::BoundColumnRef(col_ref) = arg {
                        // For sum(number), we need to replace 'number' (#0) with the pre-aggregated sum (#6)
                        // Find the corresponding aggregate function result in parent output
                        for parent_agg in &agg.aggregate_functions {
                            if let ScalarExpr::AggregateFunction(parent_agg_func) =
                                &parent_agg.scalar
                            {
                                // Check if this aggregate function's arguments match
                                if parent_agg_func.func_name == agg_func.func_name
                                    && parent_agg_func.args.len() == 1
                                {
                                    if let ScalarExpr::BoundColumnRef(parent_arg) =
                                        &parent_agg_func.args[0]
                                    {
                                        if parent_arg.column.index == col_ref.column.index {
                                            // Replace with parent's aggregate result
                                            col_ref.column.index = parent_agg.index;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
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

    /// Generate a unique CTE name based on the grouping columns
    fn generate_base_cte_name(&self, agg: &Aggregate) -> String {
        let mut hasher = DefaultHasher::new();
        agg.grouping_sets.hash(&mut hasher);
        let hash = hasher.finish();
        format!("cte_hierarchical_groupingsets_{}", hash)
    }

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

    /// Build final union branches with proper grouping sets semantics
    fn build_final_union_branches(
        &self,
        eval_scalar: &EvalScalar,
        hierarchy: &HierarchyDAG,
        agg: &Aggregate,
        base_cte_name: &str,
        base_cte: &SExpr,
        intermediate_ctes: &[SExpr],
    ) -> Result<Vec<SExpr>> {
        let mut branches = Vec::new();
        let mut level_to_cte_name = HashMap::new();
        let mut level_to_cte_expr = HashMap::new();

        // Map levels to their CTE names and CTE expressions
        level_to_cte_name.insert(
            hierarchy.get_most_detailed_level(),
            base_cte_name.to_string(),
        );
        level_to_cte_expr.insert(hierarchy.get_most_detailed_level(), base_cte);

        // Map intermediate CTEs
        for level in &hierarchy.levels {
            if level.chosen_parent.is_some() {
                let cte_name = self.generate_unique_cte_name(base_cte_name, &level.columns);
                level_to_cte_name.insert(level.set_index, cte_name.clone());

                // Find the corresponding CTE expression
                for cte in intermediate_ctes {
                    if let RelOperator::MaterializedCTE(mat_cte) = cte.plan() {
                        if mat_cte.cte_name == cte_name {
                            level_to_cte_expr.insert(level.set_index, cte);
                            break;
                        }
                    }
                }
            }
        }

        // Build final branch for each grouping set
        for level in &hierarchy.levels {
            let (source_cte_name, source_cte_expr) =
                if level.set_index == hierarchy.get_most_detailed_level() {
                    // Most detailed level - read from base CTE
                    (base_cte_name.to_string(), base_cte)
                } else {
                    // Other levels - read from their own CTE if it exists
                    if let Some(cte_name) = level_to_cte_name.get(&level.set_index).cloned() {
                        let cte_expr = level_to_cte_expr.get(&level.set_index).unwrap();
                        (cte_name, *cte_expr)
                    } else {
                        // If no CTE exists for this level, read from base CTE
                        (base_cte_name.to_string(), base_cte)
                    }
                };

            // Get actual output columns from the CTE expression
            let actual_output_columns: Vec<IndexType> = {
                // For intermediate CTE, calculate output columns based on what the hierarchical aggregation produces
                let mut output_cols = Vec::new();
                // Add aggregate function output columns
                // For intermediate CTEs, these would be the aggregate results from the parent level
                for agg_item in &agg.aggregate_functions {
                    output_cols.push(agg_item.index);
                }
                // Add group columns (those that are in this level)
                for &col_idx in &level.columns {
                    output_cols.push(col_idx);
                }
                output_cols
            };

            let branch = self.build_final_branch(
                eval_scalar,
                &level.columns,
                &source_cte_name,
                &actual_output_columns,
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
        let mut column_mapping = HashMap::new();
        for logical_index in source_output_columns.iter() {
            column_mapping.insert(*logical_index, *logical_index);
        }
        println!("column_mapping: {:?}", column_mapping);

        // Create consumer for source CTE
        let source_consumer = SExpr::create_leaf(MaterializedCTERef {
            cte_name: source_cte_name.to_string(),
            output_columns: source_output_columns.to_vec(),
            def: SExpr::create_leaf(Arc::new(DummyTableScan.into())),
            column_mapping,
        });

        // Apply grouping sets NULL semantics in EvalScalar
        let mut eval_scalar_plan = eval_scalar.clone();

        // Check if this is an intermediate CTE (not the base CTE)
        let is_intermediate_cte = source_cte_name != &self.generate_base_cte_name(&agg);

        println!(
            "DEBUG: source_cte_name = {}, base_cte_name = {}, is_intermediate_cte = {}",
            source_cte_name,
            self.generate_base_cte_name(&agg),
            is_intermediate_cte
        );

        self.apply_grouping_sets_semantics(
            &mut eval_scalar_plan,
            group_columns,
            agg,
            set_index,
            is_intermediate_cte,
            source_output_columns,
        )?;

        Ok(SExpr::create_unary(eval_scalar_plan, source_consumer))
    }

    /// Apply NULL semantics for columns not in the current grouping set
    fn apply_grouping_sets_semantics(
        &self,
        eval_scalar: &mut EvalScalar,
        group_columns: &[IndexType],
        agg: &Aggregate,
        set_index: usize,
        is_intermediate_cte: bool,
        source_output_columns: &[IndexType],
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
            is_intermediate_cte,
            source_output_columns: source_output_columns.to_vec(),
        };

        for scalar_item in eval_scalar.items.iter_mut() {
            visitor.visit(&mut scalar_item.scalar)?;
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
        let base_cte_name = self.generate_base_cte_name(&agg);

        // Build the true hierarchical plan
        let agg_input = s_expr.child(0)?.child(0)?;
        let result = self.build_true_hierarchical_plan(
            &eval_scalar,
            &agg,
            agg_input,
            hierarchy,
            base_cte_name,
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

    fn get_by_set_index(&self, set_index: usize) -> Option<&GroupingLevel> {
        self.levels
            .iter()
            .find(|level| level.set_index == set_index)
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
    is_intermediate_cte: bool,
    source_output_columns: Vec<IndexType>,
}

impl VisitorMut<'_> for GroupingSetsNullVisitor {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        let old = expr.clone();

        println!("    GroupingSetsNullVisitor processing: {:?}", expr);

        if let ScalarExpr::BoundColumnRef(col) = expr {
            println!("    Found BoundColumnRef with index: {}", col.column.index);
            println!("    group_indexes: {:?}", self.group_indexes);
            println!(
                "    exclude_group_indexes: {:?}",
                self.exclude_group_indexes
            );

            if self.group_indexes.contains(&col.column.index) {
                if self.is_intermediate_cte {
                    println!(
                        "    Leaving column {} unchanged (intermediate CTE)",
                        col.column.index
                    );
                    // For intermediate CTEs, don't add CastExpr as the data is already properly typed
                    // The intermediate CTE output should already be nullable from the aggregation
                } else {
                    println!("    Making column {} nullable (base CTE)", col.column.index);
                    // For base CTE, we need to cast to nullable, but use the correct target type
                    // The target type should be UInt64 to match the CTE's actual output type
                    *expr = ScalarExpr::CastExpr(CastExpr {
                        argument: Box::new(old),
                        is_try: true,
                        target_type: Box::new(col.column.data_type.wrap_nullable()),
                        span: col.span,
                    });
                }
            } else if self.exclude_group_indexes.contains(&col.column.index) {
                println!(
                    "    Replacing column {} with NULL (in exclude_group_indexes)",
                    col.column.index
                );
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
                println!(
                    "    Replacing column {} with grouping_id value: {}",
                    col.column.index, self.grouping_id_value
                );
                // Only replace grouping ID if we have a valid index
                // (This case should not occur in hierarchical optimization since grouping_id is not materialized in CTEs)
                *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Number(NumberScalar::UInt32(self.grouping_id_value)),
                    span: col.span,
                });
            } else {
                println!("    Leaving column {} unchanged", col.column.index);
            }
            println!("    Result after transformation: {:?}", expr);
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
                // Check if this column exists in parent output
                if self.parent_output_columns.contains(&col_ref.column.index) {
                    // Column exists in parent output - keep the logical index unchanged
                    // The column_mapping in MaterializedCTERef will handle physical position mapping
                    // No change needed here
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
