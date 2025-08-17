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

use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::TableStatistics;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_storages_common_table_meta::table::get_change_type;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct PhysicalRuntimeFilters {
    pub filters: Vec<PhysicalRuntimeFilter>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalRuntimeFilter {
    pub id: usize,
    pub build_key: RemoteExpr,
    pub probe_key: RemoteExpr<String>,
    pub scan_id: usize,
    pub enable_bloom_runtime_filter: bool,
    pub enable_inlist_runtime_filter: bool,
    pub enable_min_max_runtime_filter: bool,
}

/// JoinRuntimeFilter handles the creation and adjustment of runtime filters for join operations
pub struct JoinRuntimeFilter;

impl JoinRuntimeFilter {
    /// Adjust the bloom runtime filter based on the table statistics
    async fn adjust_bloom_runtime_filter(
        ctx: Arc<dyn TableContext>,
        metadata: &MetadataRef,
        table_index: Option<IndexType>,
        s_expr: &SExpr,
    ) -> Result<bool> {
        // Early return if bloom runtime filter is disabled in settings
        if !Self::is_bloom_filter_enabled(ctx.clone())? {
            return Ok(false);
        }

        // Check if we have a valid table index
        if let Some(table_index) = table_index {
            return Self::evaluate_bloom_filter_for_table(ctx, metadata, table_index, s_expr).await;
        }

        Ok(false)
    }

    /// Check if bloom runtime filter is enabled in settings
    fn is_bloom_filter_enabled(ctx: Arc<dyn TableContext>) -> Result<bool> {
        ctx.get_settings().get_bloom_runtime_filter()
    }

    /// Evaluate if bloom filter should be used based on table statistics
    async fn evaluate_bloom_filter_for_table(
        ctx: Arc<dyn TableContext>,
        metadata: &MetadataRef,
        table_index: IndexType,
        s_expr: &SExpr,
    ) -> Result<bool> {
        let table_entry = metadata.read().table(table_index).clone();
        let change_type = get_change_type(table_entry.alias_name());
        let table = table_entry.table();

        // Get table statistics
        if let Some(stats) = table
            .table_statistics(ctx.clone(), true, change_type)
            .await?
        {
            return Self::compare_cardinality_with_stats(stats, s_expr);
        }

        Ok(false)
    }

    /// Compare join cardinality with table statistics to determine if bloom filter is beneficial
    fn compare_cardinality_with_stats(stats: TableStatistics, s_expr: &SExpr) -> Result<bool> {
        const BLOOM_FILTER_SIZE_REDUCTION_FACTOR: u64 = 1000;

        if let Some(num_rows) = stats.num_rows {
            let join_cardinality = RelExpr::with_s_expr(s_expr)
                .derive_cardinality()?
                .cardinality;

            // If the filtered data reduces to less than 1/BLOOM_FILTER_SIZE_REDUCTION_FACTOR of the original dataset,
            // we will enable bloom runtime filter.
            return Ok(join_cardinality <= (num_rows / BLOOM_FILTER_SIZE_REDUCTION_FACTOR) as f64);
        }

        Ok(false)
    }

    /// Check if a data type is supported for bloom filter
    fn is_type_supported_for_bloom_filter(data_type: &DataType) -> bool {
        data_type.is_number() || data_type.is_string()
    }

    /// Check if a data type is supported for min-max filter
    fn is_type_supported_for_min_max_filter(data_type: &DataType) -> bool {
        data_type.is_number() || data_type.is_date() || data_type.is_string()
    }

    /// Check if the join type is supported for runtime filter
    fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
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
    pub async fn build_runtime_filter(
        ctx: Arc<dyn TableContext>,
        metadata: &MetadataRef,
        join: &Join,
        s_expr: &SExpr,
        build_keys: &[RemoteExpr],
        probe_keys: Vec<Option<(RemoteExpr<String>, usize, usize)>>,
    ) -> Result<PhysicalRuntimeFilters> {
        if !ctx.get_settings().get_enable_join_runtime_filter()? {
            return Ok(Default::default());
        }

        if !Self::supported_join_type_for_runtime_filter(&join.join_type) {
            return Ok(Default::default());
        }

        let mut filters = Vec::new();

        let build_side_data_distribution = s_expr.build_side_child().get_data_distribution()?;

        // Process each probe key that has runtime filter information
        for (build_key, probe_key, scan_id, table_index) in build_keys
            .iter()
            .zip(probe_keys.into_iter())
            .filter_map(|(b, p)| p.map(|(p, scan_id, table_index)| (b, p, scan_id, table_index)))
        {
            // Skip if not a column reference
            if probe_key.as_column_ref().is_none() {
                continue;
            }

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
                let is_supported_type = Self::is_type_supported_for_bloom_filter(&data_type);
                let enable_bloom_runtime_filter_based_on_stats = Self::adjust_bloom_runtime_filter(
                    ctx.clone(),
                    metadata,
                    Some(table_index),
                    s_expr,
                )
                .await?;
                enable_in_cluster && is_supported_type && enable_bloom_runtime_filter_based_on_stats
            };

            let enable_min_max_runtime_filter = build_side_data_distribution
                .as_ref()
                .is_none_or(|e| matches!(e, Exchange::Broadcast | Exchange::Hash(_)))
                && Self::is_type_supported_for_min_max_filter(&data_type);

            let enable_inlist_runtime_filter = build_side_data_distribution
                .as_ref()
                .is_none_or(|e| matches!(e, Exchange::Broadcast | Exchange::Hash(_)));

            // Create and add the runtime filter
            let runtime_filter = PhysicalRuntimeFilter {
                id,
                build_key: build_key.clone(),
                probe_key,
                scan_id,
                enable_bloom_runtime_filter,
                enable_inlist_runtime_filter,
                enable_min_max_runtime_filter,
            };
            filters.push(runtime_filter);
        }

        Ok(PhysicalRuntimeFilters { filters })
    }
}
