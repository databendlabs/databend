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

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::RangePrunerCreator;

use super::table::arrow_to_table_schema;
use super::Parquet2Table;
use crate::parquet2::projection::project_parquet_schema;
use crate::parquet2::pruning::build_column_page_pruners;
use crate::parquet2::pruning::PartitionPruner;

impl Parquet2Table {
    pub(crate) fn create_pruner(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
        is_small_file: bool,
    ) -> Result<PartitionPruner> {
        let settings = ctx.get_settings();
        let parquet_fast_read_bytes = if is_small_file {
            0_usize
        } else {
            settings.get_parquet_fast_read_bytes()? as usize
        };
        // `plan.source_info.schema()` is the same as `TableSchema::from(&self.arrow_schema)`
        let projection = if let Some(PushDownInfo {
            projection: Some(prj),
            ..
        }) = &push_down
        {
            prj.clone()
        } else {
            let indices = (0..self.arrow_schema.fields.len()).collect::<Vec<usize>>();
            Projection::Columns(indices)
        };

        let top_k = push_down
            .as_ref()
            .map(|p| p.top_k(&self.table_info.schema(), RangeIndex::supported_type))
            .unwrap_or_default();

        // Currently, arrow2 doesn't support reading stats of a inner column of a nested type.
        // Therefore, if there is inner fields in projection, we skip the row group pruning.
        let skip_pruning = matches!(projection, Projection::InnerColumns(_));

        // Use `projected_column_nodes` to collect stats from row groups for pruning.
        // `projected_column_nodes` contains the smallest column set that is needed for the query.
        // Use `projected_arrow_schema` to create `row_group_pruner` (`RangePruner`).
        //
        // During pruning evaluation,
        // `RangePruner` will use field name to find the offset in the schema,
        // and use the offset to find the column stat from `StatisticsOfColumns` (HashMap<offset, stat>).
        //
        // How the stats are collected can be found in `ParquetReader::collect_row_group_stats`.
        let (projected_arrow_schema, projected_column_nodes, _, columns_to_read) =
            project_parquet_schema(&self.arrow_schema, &self.schema_descr, &projection)?;
        let schema = Arc::new(arrow_to_table_schema(projected_arrow_schema));

        let filter = push_down.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS))
        });

        let inverted_filter = push_down.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.inverted_filter.as_expr(&BUILTIN_FUNCTIONS))
        });

        let top_k = top_k.map(|top_k| {
            let offset = projected_column_nodes
                .column_nodes
                .iter()
                .position(|node| node.leaf_indices[0] == top_k.leaf_id)
                .unwrap();
            (top_k, offset)
        });

        let func_ctx = ctx.get_function_context()?;

        let row_group_pruner = if self.read_options.prune_row_groups() {
            let p1 = RangePrunerCreator::try_create(func_ctx.clone(), &schema, filter.as_ref())?;
            let p2 = RangePrunerCreator::try_create(
                func_ctx.clone(),
                &schema,
                inverted_filter.as_ref(),
            )?;
            Some((p1, p2))
        } else {
            None
        };

        let page_pruners = if self.read_options.prune_pages() && filter.is_some() {
            Some(build_column_page_pruners(
                func_ctx,
                &schema,
                filter.as_ref().unwrap(),
            )?)
        } else {
            None
        };

        Ok(PartitionPruner {
            schema,
            schema_descr: self.schema_descr.clone(),
            schema_from: self.schema_from.clone(),
            row_group_pruner,
            page_pruners,
            columns_to_read,
            column_nodes: projected_column_nodes,
            skip_pruning,
            top_k,
            parquet_fast_read_bytes,
            compression_ratio: self.compression_ratio,
            max_memory_usage: settings.get_max_memory_usage()?,
        })
    }

    #[inline]
    #[async_backtrace::framed]
    pub(super) async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let pruner = self.create_pruner(ctx.clone(), push_down.clone(), false)?;

        let file_locations = match &self.files_to_read {
            Some(files) => files
                .iter()
                .map(|f| (f.path.clone(), f.size))
                .collect::<Vec<_>>(),
            None => if self.operator.info().native_capability().blocking {
                self.files_info.blocking_list(&self.operator, false, None)
            } else {
                self.files_info.list(&self.operator, false, None).await
            }?
            .into_iter()
            .map(|f| (f.path, f.size))
            .collect::<Vec<_>>(),
        };

        pruner
            .read_and_prune_partitions(
                self.operator.clone(),
                &file_locations,
                ctx.get_settings().get_max_threads()? as usize,
                &ctx.get_copy_status(),
                matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable),
            )
            .await
    }
}
