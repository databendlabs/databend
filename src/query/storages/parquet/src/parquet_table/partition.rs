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

use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_functions::BUILTIN_FUNCTIONS;
use storages_common_index::Index;
use storages_common_index::RangeIndex;
use storages_common_pruner::RangePrunerCreator;

use super::table::arrow_to_table_schema;
use crate::parquet_reader::ParquetReader;
use crate::pruning::build_column_page_pruners;
use crate::pruning::PartitionPruner;
use crate::ParquetTable;

impl ParquetTable {
    pub(crate) fn create_pruner(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
        is_small_file: bool,
    ) -> Result<PartitionPruner> {
        let parquet_fast_read_bytes = if is_small_file {
            0_usize
        } else {
            ctx.get_settings().get_parquet_fast_read_bytes()? as usize
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
            .map(|p| p.top_k(&self.table_info.schema(), None, RangeIndex::supported_type))
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
            ParquetReader::do_projection(&self.arrow_schema, &projection)?;
        let schema = Arc::new(arrow_to_table_schema(projected_arrow_schema));

        let filter = push_down
            .as_ref()
            .and_then(|extra| extra.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS)));

        let top_k = top_k.map(|top_k| {
            let offset = projected_column_nodes
                .column_nodes
                .iter()
                .position(|node| node.leaf_indices[0] == top_k.column_id as usize)
                .unwrap();
            (top_k, offset)
        });

        let func_ctx = ctx.get_function_context()?;

        let row_group_pruner = if self.read_options.prune_row_groups() {
            Some(RangePrunerCreator::try_create(
                func_ctx.clone(),
                &schema,
                filter.as_ref(),
            )?)
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
            row_group_pruner,
            page_pruners,
            columns_to_read,
            column_nodes: projected_column_nodes,
            skip_pruning,
            top_k,
            parquet_fast_read_bytes,
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
            None => if self.operator.info().can_blocking() {
                self.files_info.blocking_list(&self.operator, false, None)
            } else {
                self.files_info.list(&self.operator, false, None).await
            }?
            .into_iter()
            .map(|f| (f.path, f.size))
            .collect::<Vec<_>>(),
        };

        pruner
            .read_and_prune_partitions(self.operator.clone(), &file_locations)
            .await
    }
}
