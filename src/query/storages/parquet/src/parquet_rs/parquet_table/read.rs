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

use common_catalog::plan::DataSourcePlan;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_pipeline_core::Pipeline;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use super::ParquetRSTable;
use crate::parquet_rs::source::ParquetSource;
use crate::utils::calc_parallelism;
use crate::ParquetPart;
use crate::ParquetRSPruner;
use crate::ParquetRSReader;

impl ParquetRSTable {
    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let table_schema: TableSchemaRef = self.table_info.schema();
        // If there is a `ParquetFilesPart`, we should create pruner for it.
        // `ParquetFilesPart`s are always staying at the end of `parts`.
        let pruner = if matches!(
            plan.parts
                .partitions
                .last()
                .map(|p| p.as_any().downcast_ref::<ParquetPart>().unwrap()),
            Some(ParquetPart::ParquetFiles(_)),
        ) {
            Some(ParquetRSPruner::try_create(
                ctx.get_function_context()?,
                table_schema.clone(),
                self.leaf_fields.clone(),
                &plan.push_downs,
                self.read_options,
            )?)
        } else {
            None
        };

        let num_threads = calc_parallelism(&ctx, plan)?;

        let mut topk = plan
            .push_downs
            .as_ref()
            .and_then(|p| p.top_k(&self.schema(), None, RangeIndex::supported_type));
        let topk_limit = topk.as_ref().map(|p| p.limit).unwrap_or(0);

        if topk_limit > 0 {
            let row_group_cnt = plan
                .parts
                .partitions
                .iter()
                .filter(|p| {
                    matches!(
                        p.as_any().downcast_ref::<ParquetPart>().unwrap(),
                        ParquetPart::ParquetRSRowGroup(_)
                    )
                })
                .count();
            // Only if the row group parts in each pipeline is greater than the topk limit,
            // can we push down topk.
            // Reason: if not, the topk sorter heap will never be filled up and it will make no sense.
            if row_group_cnt / num_threads <= topk_limit {
                topk = None;
            }
        }

        let builder = ParquetRSReader::builder_with_parquet_schema(
            ctx.clone(),
            self.operator.clone(),
            table_schema,
            self.schema_descr.clone(),
        )
        .with_options(self.read_options)
        .with_push_downs(plan.push_downs.as_ref())
        .with_pruner(pruner);
        let reader = Arc::new(builder.build()?);

        let topk = Arc::new(topk);
        // TODO(parquet):
        // - introduce Top-K optimization.
        pipeline.add_source(
            |output| ParquetSource::create(ctx.clone(), output, reader.clone(), topk.clone()),
            num_threads,
        )
    }
}
