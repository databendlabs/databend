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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;

use super::routing::RuntimeFilterRouting;
use super::routing::RuntimeFilterTarget;
use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;
use super::utils::is_type_supported_for_bloom_filter;
use super::utils::is_type_supported_for_min_max_filter;
use super::utils::supported_join_type_for_runtime_filter;
use crate::physical_plans::runtime_filter::utils::is_valid_probe_key;

/// Type alias for probe keys with runtime filter information
/// Contains: (RemoteExpr, scan_id, table_index, column_idx)
type ProbeKeysWithRuntimeFilter = Vec<Option<(RemoteExpr<String>, usize, usize, IndexType)>>;

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
    build_table_indexes: Vec<Option<IndexType>>,
    routing: Option<&RuntimeFilterRouting>,
) -> Result<PhysicalRuntimeFilters> {
    if !ctx.get_settings().get_enable_join_runtime_filter()? {
        return Ok(Default::default());
    }

    if !supported_join_type_for_runtime_filter(&join.join_type) {
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
    // Safety: routing is always Some if enable_join_runtime_filter is true.
    let routing = routing.unwrap();

    // Process each probe key that has runtime filter information
    for (build_key, probe_key, _scan_id, table_index, column_idx, build_table_index) in build_keys
        .iter()
        .zip(probe_keys.into_iter())
        .zip(build_table_indexes.into_iter())
        .filter_map(|((b, p), table_idx)| {
            p.map(|(p, scan_id, table_index, column_idx)| {
                (b, p, scan_id, table_index, column_idx, table_idx)
            })
        })
    {
        // Skip if the probe expression is neither a direct column reference nor a
        // cast from not null to nullable type (e.g. CAST(col AS Nullable(T))).
        if !is_valid_probe_key(&probe_key) {
            continue;
        }

        let targets = routing.find_targets(s_expr, column_idx)?;

        let probe_targets = cast_probe_targets(targets, column_idx, &probe_key, build_key)?;

        let build_table_rows = get_table_rows(ctx.clone(), metadata, build_table_index).await?;
        let probe_table_rows = get_table_rows(ctx.clone(), metadata, Some(table_index)).await?;

        let build_key_expr = build_key.as_expr(&BUILTIN_FUNCTIONS);
        let data_type = build_key_expr.data_type().remove_nullable();
        let id = metadata.write().next_runtime_filter_id();

        let enable_bloom_runtime_filter = is_type_supported_for_bloom_filter(&data_type);

        let enable_min_max_runtime_filter = is_type_supported_for_min_max_filter(&data_type);

        // Create and add the runtime filter
        let runtime_filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            probe_table_rows,
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
}

async fn get_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    table_index: Option<IndexType>,
) -> Result<Option<u64>> {
    if let Some(table_index) = table_index {
        let table = {
            let metadata_read = metadata.read();
            metadata_read.table(table_index).table().clone()
        };

        let table_stats = table.table_statistics(ctx, false, None).await?;
        return Ok(table_stats.and_then(|s| s.num_rows));
    }

    Ok(None)
}

fn cast_probe_targets(
    targets: Vec<RuntimeFilterTarget>,
    current_column_idx: IndexType,
    current_probe_key: &RemoteExpr<String>,
    build_key: &RemoteExpr,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    let build_expr = build_key.as_expr(&BUILTIN_FUNCTIONS);
    let build_type = build_expr.data_type().clone();
    let mut dedup = HashSet::new();
    let mut result = Vec::new();

    for target in targets {
        if !dedup.insert((target.scan_id, target.column_idx)) {
            continue;
        }
        let expr = if target.column_idx == current_column_idx {
            current_probe_key.clone()
        } else {
            let target_expr = target.expr.as_expr(&BUILTIN_FUNCTIONS);
            let casted = check_cast(
                target_expr.span(),
                false,
                target_expr,
                &build_type,
                &BUILTIN_FUNCTIONS,
            )?;
            casted.as_remote_expr()
        };
        result.push((expr, target.scan_id));
    }

    Ok(result)
}
