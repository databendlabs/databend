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
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::TypeCheck;

use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;

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
    probe_keys: ProbeKeysWithRuntimeFilter,
) -> Result<PhysicalRuntimeFilters> {
    if !ctx.get_settings().get_enable_join_runtime_filter()? {
        return Ok(Default::default());
    }

    if !supported_join_type_for_runtime_filter(&join.join_type) {
        return Ok(Default::default());
    }

    let mut filters = Vec::new();

    let build_side = s_expr.build_side_child();
    let build_side_data_distribution = build_side.get_data_distribution()?;
    let probe_side = s_expr.probe_side_child();

    // Process each probe key that has runtime filter information
    for (build_key, probe_key, scan_id, _table_index, column_idx) in build_keys
        .iter()
        .zip(probe_keys.into_iter())
        .filter_map(|(b, p)| {
            p.map(|(p, scan_id, table_index, column_idx)| (b, p, scan_id, table_index, column_idx))
        })
    {
        // Skip if not a column reference
        if probe_key.as_column_ref().is_none() {
            continue;
        }

        let probe_targets =
            find_probe_targets(metadata, probe_side, &probe_key, scan_id, column_idx)?;

        let data_type = build_key
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .remove_nullable();
        let id = metadata.write().next_runtime_filter_id();

        // Determine which filter types to enable based on data type and statistics
        let enable_bloom_runtime_filter = {
            let enable_in_cluster = build_side_data_distribution
                .as_ref()
                .is_none_or(|e| matches!(e, Exchange::Broadcast));
            let is_supported_type = is_type_supported_for_bloom_filter(&data_type);
            enable_in_cluster && is_supported_type
        };

        let enable_min_max_runtime_filter = build_side_data_distribution
            .as_ref()
            .is_none_or(|e| matches!(e, Exchange::Broadcast | Exchange::Hash(_)))
            && is_type_supported_for_min_max_filter(&data_type);

        let enable_inlist_runtime_filter = build_side_data_distribution
            .as_ref()
            .is_none_or(|e| matches!(e, Exchange::Broadcast | Exchange::Hash(_)));

        // Create and add the runtime filter
        let runtime_filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter,
            enable_min_max_runtime_filter,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
}

fn find_probe_targets(
    metadata: &MetadataRef,
    s_expr: &SExpr,
    probe_key: &RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: IndexType,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    let mut uf = UnionFind::new();
    let mut column_to_remote: HashMap<IndexType, (RemoteExpr<String>, usize)> = HashMap::new();
    column_to_remote.insert(probe_key_col_idx, (probe_key.clone(), probe_scan_id));

    let equi_conditions = collect_equi_conditions(s_expr)?;
    for cond in equi_conditions {
        if let (
            Some((left_remote, left_scan_id, left_idx)),
            Some((right_remote, right_scan_id, right_idx)),
        ) = (
            scalar_to_remote_expr(metadata, &cond.left)?,
            scalar_to_remote_expr(metadata, &cond.right)?,
        ) {
            uf.union(left_idx, right_idx);
            column_to_remote.insert(left_idx, (left_remote, left_scan_id));
            column_to_remote.insert(right_idx, (right_remote, right_scan_id));
        }
    }

    let equiv_class = uf.get_equivalence_class(probe_key_col_idx);

    let mut result = Vec::new();
    for idx in equiv_class {
        if let Some((remote_expr, scan_id)) = column_to_remote.get(&idx) {
            result.push((remote_expr.clone(), *scan_id));
        }
    }

    Ok(result)
}

fn collect_equi_conditions(s_expr: &SExpr) -> Result<Vec<JoinEquiCondition>> {
    let mut conditions = Vec::new();

    if let RelOperator::Join(join) = s_expr.plan() {
        conditions.extend(join.equi_conditions.clone());
    }

    for child in s_expr.children() {
        conditions.extend(collect_equi_conditions(child)?);
    }

    Ok(conditions)
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

struct UnionFind {
    parent: HashMap<IndexType, IndexType>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

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

    fn get_equivalence_class(&mut self, x: IndexType) -> Vec<IndexType> {
        let root = self.find(x);
        let all_keys: Vec<IndexType> = self.parent.keys().copied().collect();
        all_keys
            .into_iter()
            .filter(|&k| self.find(k) == root)
            .collect()
    }
}
