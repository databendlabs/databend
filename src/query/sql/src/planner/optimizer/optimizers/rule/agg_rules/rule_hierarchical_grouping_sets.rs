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
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;

use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::Visibility;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Sequence;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

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
    cte_channel_size: usize,
}

impl RuleHierarchicalGroupingSetsToUnion {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        let cte_channel_size = ctx
            .get_table_ctx()
            .get_settings()
            .get_grouping_sets_channel_size()
            .unwrap();
        Self {
            id: ID,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::Leaf],
                }],
            }],
            cte_channel_size: cte_channel_size as usize,
        }
    }

    /// Analyzes grouping sets to build a true hierarchical dependency DAG
    fn build_hierarchy_dag(
        &self,
        grouping_sets: &[Vec<IndexType>],
        agg: &Aggregate,
    ) -> HierarchyDAG {
        let mut levels: Vec<GroupingLevel> = grouping_sets
            .iter()
            .enumerate()
            .map(|(idx, set)| {
                // Sort columns according to their order in group_items for consistent schema ordering
                let mut sorted_columns = set.clone();
                sorted_columns.sort_by_key(|&col_idx| {
                    agg.group_items
                        .iter()
                        .position(|item| item.index == col_idx)
                        .unwrap_or(usize::MAX) // Put unknown columns at the end
                });
                GroupingLevel {
                    set_index: idx,
                    columns: sorted_columns,
                    direct_children: Vec::new(),
                    possible_parents: Vec::new(),
                    chosen_parent: None,
                    level: set.len(),
                }
            })
            .collect();

        // Add a special "original input" level that represents the raw data before any grouping
        // This will be the ultimate parent for any grouping sets that don't have natural parents
        let original_input_level = GroupingLevel {
            set_index: usize::MAX, // Use a special index to represent original input
            columns: Vec::new(),   // Empty means "all columns available" (no grouping yet)
            direct_children: Vec::new(),
            possible_parents: Vec::new(),
            chosen_parent: None,
            level: usize::MAX, // Highest possible level (most detailed)
        };
        levels.push(original_input_level);

        // Sort by specificity (most detailed first)
        levels.sort_by(|a, b| b.level.cmp(&a.level));

        // Build parent-child relationships
        for i in 0..levels.len() {
            for j in (i + 1)..levels.len() {
                // Skip the original input level when checking subset relationships
                if levels[i].set_index == usize::MAX || levels[j].set_index == usize::MAX {
                    continue;
                }

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

        // For any grouping sets without chosen parents, make them children of original input
        for level in levels.iter_mut() {
            if level.set_index != usize::MAX && level.chosen_parent.is_none() {
                level.chosen_parent = Some(usize::MAX); // Point to original input
            }
        }

        // Optimization: If original input is only referenced by one grouping set,
        // we can eliminate the original input CTE and use that grouping set as the base
        let original_children: Vec<_> = levels
            .iter()
            .filter(|level| level.chosen_parent == Some(usize::MAX))
            .collect();

        if original_children.len() == 1 {
            let only_child_set_index = original_children[0].set_index;

            // Remove the original input level
            levels.retain(|level| level.set_index != usize::MAX);

            // Update the only child to have no parent (it becomes the base)
            for level in levels.iter_mut() {
                if level.set_index == only_child_set_index {
                    level.chosen_parent = None;
                }
                // Update other levels that might reference the original input
                if level.chosen_parent == Some(usize::MAX) {
                    level.chosen_parent = Some(only_child_set_index);
                }
            }
        }

        HierarchyDAG { levels }
    }

    /// Calculate the dependency depth of a level in the hierarchy
    /// Depth 0 = no dependencies (base level)
    /// Depth 1 = depends on base level
    /// Depth N = depends on level with depth N-1
    fn calculate_dependency_depth(level: &GroupingLevel, hierarchy: &HierarchyDAG) -> usize {
        match level.chosen_parent {
            None => 0,             // Base level
            Some(usize::MAX) => 1, // Depends on original input
            Some(parent_set_idx) => {
                // Find parent level and add 1 to its depth
                let parent_level = hierarchy
                    .levels
                    .iter()
                    .find(|l| l.set_index == parent_set_idx)
                    .unwrap();
                Self::calculate_dependency_depth(parent_level, hierarchy) + 1
            }
        }
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
                matches!(
                    agg.func_name.as_str(),
                    "sum" | "count" | "min" | "max" | "sum0"
                ) && !agg.distinct
                    && agg.args.len() <= 1
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
        grouping_id_index: IndexType,
    ) -> Result<SExpr> {
        let mut all_ctes = Vec::new();

        // Step 1: Check if we need an original input CTE
        let has_original_input = hierarchy
            .levels
            .iter()
            .any(|level| level.set_index == usize::MAX);

        if has_original_input {
            // Create original input CTE (just materializes the raw data)
            let original_cte_name = format!("{}_original", base_cte_name);
            let original_cte = self.create_original_input_cte(&original_cte_name, agg_input)?;
            all_ctes.push(CteInfo {
                name: original_cte_name,
                set_index: usize::MAX,
                cte: original_cte,
            });
        }

        // Step 2: Create CTEs for grouping sets, organizing by dependency levels for parallelization
        let agg_input_columns: Vec<IndexType> = RelExpr::with_s_expr(agg_input)
            .derive_relational_prop()?
            .output_columns
            .iter()
            .cloned()
            .collect();

        // Group levels by their dependency depth for potential parallelization
        let mut sorted_levels = hierarchy.levels.clone();
        sorted_levels.retain(|level| level.set_index != usize::MAX); // Remove original input from sorting

        let mut levels_by_depth: std::collections::BTreeMap<usize, Vec<&GroupingLevel>> =
            std::collections::BTreeMap::new();
        for level in &sorted_levels {
            let depth = Self::calculate_dependency_depth(level, &hierarchy);
            levels_by_depth.entry(depth).or_default().push(level);
        }

        // Process levels by depth, creating parallel structures where possible
        for (depth, levels_at_depth) in levels_by_depth {
            if levels_at_depth.len() > 1 && depth == 1 {
                // Multiple levels at depth 1 (depend only on original) - can be parallelized with UnionAll

                // Create individual CTEs for each level
                for level in &levels_at_depth {
                    let cte_name = Self::generate_unique_cte_name(&base_cte_name, &level.columns);

                    let cte = if let Some(parent_set_idx) = level.chosen_parent {
                        if parent_set_idx == usize::MAX {
                            // Parent is original input
                            let original_cte_name = all_ctes
                                .iter()
                                .find(|cte| cte.set_index == usize::MAX)
                                .unwrap()
                                .name
                                .clone();

                            self.create_cte_from_original(
                                agg_input,
                                &cte_name,
                                &original_cte_name,
                                level,
                                agg,
                                &agg_input_columns,
                            )?
                        } else {
                            // Parent is another grouping set CTE
                            let parent = hierarchy.get_by_set_index(parent_set_idx).unwrap();
                            let parent_cte = all_ctes
                                .iter()
                                .find(|cte| cte.set_index == parent_set_idx)
                                .unwrap();

                            self.create_hierarchical_cte(&cte_name, parent_cte, parent, level, agg)?
                        }
                    } else {
                        // No parent - this is a base level
                        self.create_base_cte_for_level(&cte_name, agg_input, level, agg)?
                    };

                    all_ctes.push(CteInfo {
                        name: cte_name,
                        set_index: level.set_index,
                        cte,
                    });
                }
            } else {
                // Single level or higher depth - process normally
                for level in levels_at_depth {
                    let cte_name = if level.columns.is_empty() {
                        format!("{}_empty", base_cte_name)
                    } else {
                        Self::generate_unique_cte_name(&base_cte_name, &level.columns)
                    };

                    let cte = if let Some(parent_set_idx) = level.chosen_parent {
                        if parent_set_idx == usize::MAX {
                            // Parent is original input
                            let original_cte_name = all_ctes
                                .iter()
                                .find(|cte| cte.set_index == usize::MAX)
                                .unwrap()
                                .name
                                .clone();

                            self.create_cte_from_original(
                                agg_input,
                                &cte_name,
                                &original_cte_name,
                                level,
                                agg,
                                &agg_input_columns,
                            )?
                        } else {
                            // Parent is another grouping set CTE
                            let parent_level = hierarchy.get_by_set_index(parent_set_idx).unwrap();
                            let parent_cte = all_ctes
                                .iter()
                                .find(|cte| cte.set_index == parent_set_idx)
                                .unwrap();
                            self.create_hierarchical_cte(
                                &cte_name,
                                parent_cte,
                                parent_level,
                                level,
                                agg,
                            )?
                        }
                    } else {
                        // No parent - this is a base level
                        self.create_base_cte_for_level(&cte_name, agg_input, level, agg)?
                    };

                    all_ctes.push(CteInfo {
                        name: cte_name.clone(),
                        set_index: level.set_index,
                        cte,
                    });
                }
            }
        }

        // Step 3: Build final union branches
        let union_branches = self.build_final_union_branches(
            &all_ctes,
            eval_scalar,
            &hierarchy,
            agg,
            grouping_id_index,
        )?;

        // Step 4: Assemble the complete plan
        let union_result = self.create_union_all(&union_branches, eval_scalar)?;

        // Step 5: Chain all CTEs in correct dependency order
        // Sequence semantics: left executes first, right executes after
        // Dependencies must execute before dependents
        let mut result = union_result;
        for cte in all_ctes.into_iter().rev() {
            result = SExpr::create_binary(Sequence, cte.cte, result);
        }

        Ok(result)
    }

    /// Create original input CTE that just materializes raw data
    fn create_original_input_cte(&self, cte_name: &str, agg_input: &SExpr) -> Result<SExpr> {
        Ok(SExpr::create_unary(
            Arc::new(
                MaterializedCTE {
                    cte_name: cte_name.to_string(),
                    cte_output_columns: None,
                    ref_count: 1,
                    channel_size: Some(self.cte_channel_size),
                }
                .into(),
            ),
            agg_input.clone(),
        ))
    }

    /// Create CTE from original input (performs aggregation on raw data)
    fn create_cte_from_original(
        &self,
        agg_input: &SExpr,
        cte_name: &str,
        original_cte_name: &str,
        level: &GroupingLevel,
        agg: &Aggregate,
        agg_input_columns: &[IndexType],
    ) -> Result<SExpr> {
        // Create aggregate plan for this grouping set
        let mut group_agg = agg.clone();
        group_agg.grouping_sets = None;
        group_agg
            .group_items
            .retain(|item| level.columns.contains(&item.index));

        // Remove grouping_id functions
        group_agg.aggregate_functions.retain(|func| {
            !matches!(
                func.scalar,
                ScalarExpr::AggregateFunction(ref agg_func) if agg_func.func_name == "grouping"
            )
        });

        // Create consumer for original CTE
        let original_consumer = SExpr::create_leaf(Arc::new(
            MaterializedCTERef {
                cte_name: original_cte_name.to_string(),
                output_columns: agg_input_columns.to_vec(), // Will be populated based on original input
                def: agg_input.clone(),
                column_mapping: agg_input_columns.iter().map(|col| (*col, *col)).collect(), // Identity mapping
            }
            .into(),
        ));

        let agg_plan = SExpr::create_unary(Arc::new(group_agg.into()), original_consumer);
        Ok(SExpr::create_unary(
            Arc::new(
                MaterializedCTE {
                    cte_name: cte_name.to_string(),
                    cte_output_columns: None,
                    ref_count: 1,
                    channel_size: Some(self.cte_channel_size),
                }
                .into(),
            ),
            agg_plan,
        ))
    }

    /// Create base CTE for a specific level (no parent)
    fn create_base_cte_for_level(
        &self,
        cte_name: &str,
        agg_input: &SExpr,
        level: &GroupingLevel,
        agg: &Aggregate,
    ) -> Result<SExpr> {
        // Create aggregate plan for this level
        let mut level_agg = agg.clone();
        level_agg.grouping_sets = None;
        level_agg
            .group_items
            .retain(|item| level.columns.contains(&item.index));

        level_agg.aggregate_functions.retain(|func| {
            !matches!(
                func.scalar,
                ScalarExpr::AggregateFunction(ref agg_func) if agg_func.func_name == "grouping"
            )
        });

        let agg_plan = SExpr::create_unary(Arc::new(level_agg.into()), agg_input.clone());

        let final_plan = agg_plan;

        Ok(SExpr::create_unary(
            Arc::new(
                MaterializedCTE {
                    cte_name: cte_name.to_string(),
                    cte_output_columns: None,
                    ref_count: 1,
                    channel_size: Some(self.cte_channel_size),
                }
                .into(),
            ),
            final_plan,
        ))
    }

    /// Build final union branches using the new CTE mapping
    fn build_final_union_branches(
        &self,
        all_ctes: &[CteInfo],
        eval_scalar: &EvalScalar,
        hierarchy: &HierarchyDAG,
        agg: &Aggregate,
        grouping_id_index: IndexType,
    ) -> Result<Vec<SExpr>> {
        let mut branches = Vec::new();

        for level in &hierarchy.levels {
            if level.set_index == usize::MAX {
                continue; // Skip original input level
            }

            let source_cte = all_ctes
                .iter()
                .find(|cte| cte.set_index == level.set_index)
                .unwrap();

            // Calculate output columns for this CTE
            let mut source_output_columns = Vec::new();
            for agg_item in &agg.aggregate_functions {
                source_output_columns.push(agg_item.index);
            }
            for &col_idx in &level.columns {
                source_output_columns.push(col_idx);
            }

            let branch = self.build_final_branch(
                eval_scalar,
                &level.columns,
                source_cte,
                &source_output_columns,
                level.set_index,
                agg,
                grouping_id_index,
            )?;

            branches.push(branch);
        }

        Ok(branches)
    }

    /// Create a hierarchical CTE that aggregates from a parent CTE
    fn create_hierarchical_cte(
        &self,
        cte_name: &str,
        parent_cte: &CteInfo,
        parent_level: &GroupingLevel,
        level: &GroupingLevel,
        agg: &Aggregate,
    ) -> Result<SExpr> {
        // Create aggregate plan for this specific grouping set
        let mut hierarchical_agg = agg.clone();
        hierarchical_agg.grouping_sets = None;

        // Filter group items to only include columns in this level
        hierarchical_agg
            .group_items
            .retain(|item| level.columns.contains(&item.index));

        // IMPORTANT: Replace group item expressions with BoundColumnRef for CTE consumption
        // This ensures we read from the parent CTE instead of recalculating expressions
        for group_item in hierarchical_agg.group_items.iter_mut() {
            group_item.scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    format!("group_item_{}", group_item.index),
                    group_item.index,
                    group_item.scalar.data_type()?.into(),
                    Visibility::Visible,
                )
                .build(),
            });
        }

        // Transform aggregate functions for re-aggregation
        for agg_func in hierarchical_agg.aggregate_functions.iter_mut() {
            // Get the original data type before modifying the function
            let original_data_type = agg_func.scalar.data_type()?;
            if let ScalarExpr::AggregateFunction(ref mut func) = &mut agg_func.scalar {
                match func.func_name.as_str() {
                    "count" => {
                        // COUNT(*) from pre-aggregated -> sum0(pre_computed_count)
                        // Important: Keep the original UInt64 return type to maintain type compatibility
                        func.func_name = "sum0".to_string();
                        // Replace argument with BoundColumnRef to pre-aggregated result
                        if func.args.len() <= 1 {
                            func.args = vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                                span: None,
                                column: ColumnBindingBuilder::new(
                                    format!("agg_result_{}", agg_func.index),
                                    agg_func.index,
                                    original_data_type.clone().into(), // Keep original UInt64 type, not nullable
                                    Visibility::Visible,
                                )
                                .build(),
                            })];
                        }
                    }
                    "sum0" | "sum" | "min" | "max" => {
                        // These functions are naturally associative
                        // Replace argument with BoundColumnRef to pre-aggregated result
                        func.args = vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: ColumnBindingBuilder::new(
                                format!("agg_result_{}", agg_func.index),
                                agg_func.index,
                                original_data_type.into(),
                                Visibility::Visible,
                            )
                            .build(),
                        })];
                    }
                    _ => {
                        // Other functions - keep as is for now
                    }
                }
            }
        }

        // Create parent CTE consumer
        let parent_output_columns: Vec<IndexType> = {
            let mut output_cols = Vec::new();
            // First: aggregate function output columns
            for agg_item in &agg.aggregate_functions {
                output_cols.push(agg_item.index);
            }
            // Then: parent level columns (already sorted from build_hierarchy_dag)
            for &col_idx in &parent_level.columns {
                output_cols.push(col_idx);
            }
            output_cols
        };

        let mut column_mapping = HashMap::new();
        for &col_idx in &parent_output_columns {
            column_mapping.insert(col_idx, col_idx);
        }

        let parent_consumer = SExpr::create_leaf(Arc::new(
            MaterializedCTERef {
                cte_name: parent_cte.name.clone(),
                output_columns: parent_output_columns,
                def: parent_cte.cte.child(0)?.clone(),
                column_mapping,
            }
            .into(),
        ));

        let agg_plan = SExpr::create_unary(Arc::new(hierarchical_agg.into()), parent_consumer);

        // Create MaterializedCTE node that wraps the aggregation
        Ok(SExpr::create_unary(
            Arc::new(
                MaterializedCTE {
                    cte_name: cte_name.to_string(),
                    cte_output_columns: None,
                    ref_count: 1,
                    channel_size: Some(self.cte_channel_size),
                }
                .into(),
            ),
            agg_plan,
        ))
    }

    /// Generate a unique CTE name based on the grouping columns
    fn generate_base_cte_name(&self, agg: &Aggregate) -> String {
        let mut hasher = DefaultHasher::new();
        agg.grouping_sets.hash(&mut hasher);
        let hash = hasher.finish();
        format!("cte_hierarchical_groupingsets_{}", hash)
    }

    fn generate_unique_cte_name(base_name: &str, columns: &[IndexType]) -> String {
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

    /// Build a final branch that applies grouping sets NULL semantics
    fn build_final_branch(
        &self,
        eval_scalar: &EvalScalar,
        group_columns: &[IndexType],
        source_cte: &CteInfo,
        source_output_columns: &[IndexType],
        _set_index: usize,
        agg: &Aggregate,
        grouping_id_index: IndexType,
    ) -> Result<SExpr> {
        let mut column_mapping = HashMap::new();

        // Create modified output columns with proper types for count functions
        let mut modified_output_columns = Vec::new();
        for &logical_index in source_output_columns.iter() {
            // For other columns, use a dummy binding (will be filled by actual schema)
            modified_output_columns.push(
                ColumnBindingBuilder::new(
                    format!("col_{}", logical_index),
                    logical_index,
                    DataType::Number(NumberDataType::UInt64).into(), // Placeholder
                    Visibility::Visible,
                )
                .build(),
            );
            column_mapping.insert(logical_index, logical_index);
        }

        // Create consumer for source CTE
        let source_consumer = SExpr::create_leaf(MaterializedCTERef {
            cte_name: source_cte.name.clone(),
            output_columns: source_output_columns.to_vec(),
            def: source_cte.cte.child(0)?.clone(),
            column_mapping,
        });

        // Apply grouping sets NULL semantics in EvalScalar
        let mut eval_scalar_plan = eval_scalar.clone();
        self.apply_grouping_sets_semantics(
            &mut eval_scalar_plan,
            &source_cte.name,
            group_columns,
            agg,
            grouping_id_index,
        )?;

        Ok(SExpr::create_unary(eval_scalar_plan, source_consumer))
    }

    /// Apply NULL semantics for columns not in the current grouping set
    fn apply_grouping_sets_semantics(
        &self,
        eval_scalar: &mut EvalScalar,
        _source_cte_name: &str,
        group_columns: &[IndexType],
        agg: &Aggregate,
        grouping_id_index: IndexType,
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
            grouping_id_index,
            grouping_id_value: grouping_id,
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
        let can_optimize = self.can_optimize_hierarchically(&agg.aggregate_functions);
        if !can_optimize {
            return Ok(());
        }

        // Build hierarchy DAG
        let hierarchy = self.build_hierarchy_dag(&grouping_sets.sets, &agg);
        // Check if we have meaningful hierarchical structure
        let hierarchical_levels = hierarchy
            .levels
            .iter()
            .filter(|level| level.chosen_parent.is_some())
            .count();

        if hierarchical_levels <= 1 {
            return Ok(()); // Not enough hierarchy to justify optimization
        }

        // Generate unique base CTE name
        let base_cte_name = self.generate_base_cte_name(&agg);

        // Build the true hierarchical plan
        let agg_input = s_expr.child(0)?.child(0)?;
        let grouping_id_index = grouping_sets.grouping_id_index;
        let result = self.build_true_hierarchical_plan(
            &eval_scalar,
            &agg,
            agg_input,
            hierarchy,
            base_cte_name,
            grouping_id_index,
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

struct CteInfo {
    name: String,
    set_index: usize,
    cte: SExpr,
}

#[derive(Debug)]
struct HierarchyDAG {
    levels: Vec<GroupingLevel>,
}

impl HierarchyDAG {
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

/// Set other columns to NULL and current group by columns to be nullable
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
                // For base CTE, we need to cast to nullable, but use the correct target type
                // The target type should be UInt64 to match the CTE's actual output type
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
            } else if self.grouping_id_index == col.column.index {
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
