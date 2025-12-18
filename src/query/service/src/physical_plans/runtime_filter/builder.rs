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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::TypeCheck;

use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;

type ScalarExprId = usize;

#[derive(Default)]
pub struct JoinEquivalenceClasses {
    expr_to_id: HashMap<ScalarExpr, ScalarExprId>,
    uf: UnionFind,
    next_id: ScalarExprId,
}

impl JoinEquivalenceClasses {
    pub fn build_from_sexpr(s_expr: &SExpr) -> Self {
        let mut ec = Self::default();
        ec.collect_equi_conditions_recursive(s_expr);
        ec
    }

    fn collect_equi_conditions_recursive(&mut self, s_expr: &SExpr) {
        if let RelOperator::Join(join) = s_expr.plan() {
            if matches!(join.join_type, JoinType::Inner) {
                for cond in &join.equi_conditions {
                    let left_id = self.get_or_create_id(&cond.left);
                    let right_id = self.get_or_create_id(&cond.right);
                    self.uf.union(left_id, right_id);
                }
            }
        }

        for child in s_expr.children() {
            self.collect_equi_conditions_recursive(child);
        }
    }

    fn get_or_create_id(&mut self, expr: &ScalarExpr) -> ScalarExprId {
        if let Some(&id) = self.expr_to_id.get(expr) {
            return id;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.expr_to_id.insert(expr.clone(), id);
        id
    }

    pub fn are_equivalent(&mut self, expr1: &ScalarExpr, expr2: &ScalarExpr) -> bool {
        let id1 = self.expr_to_id.get(expr1);
        let id2 = self.expr_to_id.get(expr2);

        match (id1, id2) {
            (Some(&id1), Some(&id2)) => self.uf.find(id1) == self.uf.find(id2),
            _ => false,
        }
    }
}

/// Type alias for probe keys with runtime filter information
/// Contains: (RemoteExpr, scan_id, table_index, column_idx)
type ProbeKeysWithRuntimeFilter = Vec<Option<(RemoteExpr<String>, usize, usize, IndexType)>>;

/// Check if a data type is supported for bloom filter
///
/// Currently supports: numbers and strings
pub fn is_type_supported_for_bloom_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_string()
}

/// Check if a data type is supported for min-max filter
///
/// Currently supports: numbers, dates, and strings
pub fn is_type_supported_for_min_max_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_date() || data_type.is_string()
}

/// Check if the join type is supported for runtime filter
///
/// Runtime filters are only applicable to certain join types where
/// filtering the probe side can reduce processing
pub fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}

/// Build runtime filters for a join operation
///
/// This is the legacy method that creates one runtime filter per probe key.
/// For equivalence class propagation, use the enhanced version in physical_hash_join.rs
///
/// # Arguments
/// * `ctx` - Table context
/// * `metadata` - Metadata reference
/// * `join` - Join plan
/// * `s_expr` - SExpr for the join
/// * `build_keys` - Build side keys
/// * `probe_keys` - Probe side keys with scan_id, table_index, and column_idx
///
/// # Returns
/// Collection of runtime filters to be applied
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &SExpr,
    build_keys: &[RemoteExpr],
    build_keys_scalar: &[ScalarExpr],
    probe_keys: ProbeKeysWithRuntimeFilter,
    build_table_indexes: Vec<Option<IndexType>>,
    runtime_filter_anchors: &[Arc<SExpr>],
    join_equivalence_classes: &mut JoinEquivalenceClasses,
) -> Result<PhysicalRuntimeFilters> {
    if !ctx.get_settings().get_enable_join_runtime_filter()? {
        return Ok(Default::default());
    }

    let build_side = s_expr.build_side_child();
    let build_side_data_distribution = build_side.get_data_distribution()?;
    if build_side_data_distribution.as_ref().is_some_and(|e| {
        !matches!(
            e,
            Exchange::Broadcast | Exchange::NodeToNodeHash(_) | Exchange::Merge
        )
    }) {
        return Ok(Default::default());
    }

    let mut filters = Vec::new();

    // Process each probe key that has runtime filter information
    for (
        build_key,
        build_key_scalar,
        _probe_key,
        _scan_id,
        _table_index,
        _column_idx,
        build_table_index,
    ) in build_keys
        .iter()
        .zip(build_keys_scalar.iter())
        .zip(probe_keys.into_iter())
        .zip(build_table_indexes.into_iter())
        .filter_map(|(((b, b_scalar), p), table_idx)| {
            p.map(|(p, scan_id, table_index, column_idx)| {
                (b, b_scalar, p, scan_id, table_index, column_idx, table_idx)
            })
        })
    {
        let mut probe_targets = Vec::new();

        for anchor in runtime_filter_anchors
            .iter()
            .map(|a| a.as_ref())
            .chain(supported_join_type_for_runtime_filter(&join.join_type).then_some(s_expr))
        {
            // Get anchor's equi_conditions
            let anchor_join = if let RelOperator::Join(anchor_join) = anchor.plan() {
                anchor_join
            } else {
                continue;
            };

            let anchor_probe_key = anchor_join
                .equi_conditions
                .iter()
                .find(|cond| join_equivalence_classes.are_equivalent(build_key_scalar, &cond.right))
                .map(|cond| &cond.left);

            let anchor_probe_key = match anchor_probe_key {
                Some(key) => key,
                None => continue,
            };

            // First, add the anchor's probe key itself as a probe target
            if let Some((remote_expr, scan_id, _)) =
                scalar_to_remote_expr(metadata, anchor_probe_key)?
            {
                if is_valid_probe_target(&remote_expr) {
                    probe_targets.push((remote_expr, scan_id));
                }
            }

            // Then search only in the anchor's probe side subtree
            let anchor_probe_side = anchor.probe_side_child();
            find_probe_targets_in_tree(
                anchor_probe_side,
                anchor_probe_key,
                join_equivalence_classes,
                metadata,
                &mut probe_targets,
            )?;
        }

        if probe_targets.is_empty() {
            continue;
        }

        let build_table_rows =
            get_build_table_rows(ctx.clone(), metadata, build_table_index).await?;

        let data_type = build_key
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .remove_nullable();
        let id = metadata.write().next_runtime_filter_id();

        let enable_bloom_runtime_filter = is_type_supported_for_bloom_filter(&data_type);

        let enable_min_max_runtime_filter = is_type_supported_for_min_max_filter(&data_type);

        // Create and add the runtime filter
        let runtime_filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
}

async fn get_build_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    build_table_index: Option<IndexType>,
) -> Result<Option<u64>> {
    if let Some(table_index) = build_table_index {
        let table = {
            let metadata_read = metadata.read();
            metadata_read.table(table_index).table().clone()
        };

        let table_stats = table.table_statistics(ctx, false, None).await?;
        return Ok(table_stats.and_then(|s| s.num_rows));
    }

    Ok(None)
}

/// Check if a RemoteExpr is valid for probe target
///
/// Valid probe targets are:
/// 1. Direct column reference
/// 2. Cast from not null to nullable type (e.g. CAST(col AS Nullable(T)))
fn is_valid_probe_target(remote_expr: &RemoteExpr<String>) -> bool {
    match remote_expr {
        RemoteExpr::ColumnRef { .. } => true,
        RemoteExpr::Cast {
            expr: box RemoteExpr::ColumnRef { data_type, .. },
            dest_type,
            ..
        } if &dest_type.remove_nullable() == data_type => true,
        _ => false,
    }
}

/// Find probe targets in an SExpr tree by checking equivalence with the target probe key
fn find_probe_targets_in_tree(
    s_expr: &SExpr,
    target_probe_key: &ScalarExpr,
    join_equivalence_classes: &mut JoinEquivalenceClasses,
    metadata: &MetadataRef,
    results: &mut Vec<(RemoteExpr<String>, usize)>,
) -> Result<()> {
    if let RelOperator::Join(join) = s_expr.plan() {
        for cond in &join.equi_conditions {
            // Check if left (probe) key is equivalent to target
            if join_equivalence_classes.are_equivalent(target_probe_key, &cond.left) {
                if let Some((remote_expr, scan_id, _)) =
                    scalar_to_remote_expr(metadata, &cond.left)?
                {
                    if is_valid_probe_target(&remote_expr) {
                        results.push((remote_expr, scan_id));
                    }
                }
            }
            // Check if right (build) key is equivalent to target
            if join_equivalence_classes.are_equivalent(target_probe_key, &cond.right) {
                if let Some((remote_expr, scan_id, _)) =
                    scalar_to_remote_expr(metadata, &cond.right)?
                {
                    if is_valid_probe_target(&remote_expr) {
                        results.push((remote_expr, scan_id));
                    }
                }
            }
        }
    }

    // Recursively traverse children
    for child in s_expr.children() {
        find_probe_targets_in_tree(
            child,
            target_probe_key,
            join_equivalence_classes,
            metadata,
            results,
        )?;
    }

    Ok(())
}

fn scalar_to_remote_expr(
    metadata: &MetadataRef,
    scalar: &ScalarExpr,
) -> Result<Option<(RemoteExpr<String>, usize, IndexType)>> {
    if scalar.used_columns().iter().all(|idx| {
        matches!(
            metadata.read().column(*idx),
            ColumnEntry::BaseTableColumn(_)
        )
    }) {
        if let Some(column_idx) = scalar.used_columns().iter().next() {
            let scan_id = metadata.read().base_column_scan_id(*column_idx);

            if let Some(scan_id) = scan_id {
                let remote_expr = scalar
                    .as_raw_expr()
                    .type_check(&*metadata.read())?
                    .project_column_ref(|col| Ok(col.column_name.clone()))?
                    .as_remote_expr();

                return Ok(Some((remote_expr, scan_id, *column_idx)));
            }
        }
    }

    Ok(None)
}

#[derive(Default)]
struct UnionFind {
    parent: HashMap<IndexType, IndexType>,
}

impl UnionFind {
    fn find(&mut self, x: IndexType) -> IndexType {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
            return x;
        }

        let parent = *self.parent.get(&x).unwrap();
        if parent != x {
            let root = self.find(parent);
            self.parent.insert(x, root);
        }
        *self.parent.get(&x).unwrap()
    }

    fn union(&mut self, x: IndexType, y: IndexType) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent.insert(root_x, root_y);
        }
    }
}
