//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::Expr;
use common_expression::TableSchema;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use storages_common_pruner::RangePrunerCreator;

use super::ParquetTable;
use super::TableContext;
use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetRowGroupPart;
use crate::ParquetLocationPart;
use crate::ParquetReader;
use crate::ParquetSource;

impl ParquetTable {
    pub fn create_reader(&self, projection: Projection) -> Result<Arc<ParquetReader>> {
        ParquetReader::create(self.operator.clone(), self.arrow_schema.clone(), projection)
    }

    // Build the prewhere reader.
    fn build_prewhere_reader(&self, plan: &DataSourcePlan) -> Result<Arc<ParquetReader>> {
        match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
            None => {
                let projection =
                    PushDownInfo::projection_of_push_downs(&plan.schema(), &plan.push_downs);
                self.create_reader(projection)
            }
            Some(v) => self.create_reader(v.prewhere_columns),
        }
    }

    // Build the prewhere filter expression.
    fn build_prewhere_filter_expr(
        &self,
        _ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => Arc::new(v.filter.as_expr(&BUILTIN_FUNCTIONS).map(|expr| {
                    expr.project_column_ref(|name| schema.column_with_name(name).unwrap().0)
                })),
            },
        )
    }

    // Build the remain reader.
    fn build_remain_reader(&self, plan: &DataSourcePlan) -> Result<Arc<Option<ParquetReader>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    if v.remain_columns.is_empty() {
                        Arc::new(None)
                    } else {
                        Arc::new(Some((*self.create_reader(v.remain_columns)?).clone()))
                    }
                }
            },
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
        Ok(std::cmp::max(max_threads, max_io_requests))
    }

    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        // Split one partition from one parquet file into multiple row groups.
        let locations = plan
            .parts
            .partitions
            .iter()
            .map(|part| {
                let part = ParquetLocationPart::from_part(part).unwrap();
                part.location.clone()
            })
            .collect::<Vec<_>>();

        // `plan.source_info.schema()` is the same as `TableSchema::from(&self.arrow_schema)`
        let columns_to_read =
            PushDownInfo::projection_of_push_downs(&plan.source_info.schema(), &plan.push_downs);
        let max_io_requests = self.adjust_io_request(&ctx)?;
        let ctx_ref = ctx.clone();

        // do parition at the begin of the whole pipeline.
        let push_downs = plan.push_downs.clone();

        // Currently, arrow2 doesn't support reading stats of a inner column of a nested type.
        // Therefore, if there is inner fields in projection, we skip the row group pruning.
        let skip_pruning = matches!(columns_to_read, Projection::InnerColumns(_));

        // Use `projected_column_leaves` to collect stats from row groups for pruning.
        // `projected_column_leaves` contains the smallest column set that is needed for the query.
        // Use `projected_arrow_schema` to create `row_group_pruner` (`RangePruner`).
        //
        // During evaluation,
        // `RangePruner` will use field name to find the offset in the schema,
        // and use the offset to find the column stat from `StatisticsOfColumns` (HashMap<offset, stat>).
        //
        // How the stats are collected can be found in `ParquetReader::collect_row_group_stats`.
        let (projected_arrow_schema, projected_column_leaves, _, columns_to_read) =
            ParquetReader::do_projection(&self.arrow_schema, &columns_to_read)?;
        let schema = Arc::new(TableSchema::from(&projected_arrow_schema));

        pipeline.set_on_init(move || {
            let mut partitions = Vec::with_capacity(locations.len());

            // build row group pruner.
            let filter_exprs = push_downs.as_ref().map(|extra| {
                extra
                    .filters
                    .iter()
                    .map(|f| f.as_expr(&BUILTIN_FUNCTIONS).unwrap())
                    .collect::<Vec<_>>()
            });
            let row_group_pruner =
                RangePrunerCreator::try_create(&ctx_ref, filter_exprs.as_deref(), &schema)?;

            for location in &locations {
                let file_meta = ParquetReader::read_meta(location)?;
                let mut row_group_pruned = vec![false; file_meta.row_groups.len()];

                if !skip_pruning {
                    // If collecting stats fails or `should_keep` is true, we still read the row group.
                    // Otherwise, the row group will be pruned.
                    if let Ok(row_group_stats) = ParquetReader::collect_row_group_stats(
                        &projected_column_leaves,
                        &file_meta.row_groups,
                    ) {
                        for (idx, (stats, _rg)) in row_group_stats
                            .iter()
                            .zip(file_meta.row_groups.iter())
                            .enumerate()
                        {
                            row_group_pruned[idx] = !row_group_pruner.should_keep(stats);
                        }
                    }
                }

                for (idx, rg) in file_meta.row_groups.iter().enumerate() {
                    if row_group_pruned[idx] {
                        continue;
                    }
                    let mut column_metas = HashMap::with_capacity(columns_to_read.len());
                    for index in &columns_to_read {
                        let c = &rg.columns()[*index];
                        let (offset, length) = c.byte_range();
                        column_metas.insert(*index, ColumnMeta {
                            offset,
                            length,
                            compression: c.compression().into(),
                        });
                    }

                    partitions.push(ParquetRowGroupPart::create(
                        location.clone(),
                        rg.num_rows(),
                        column_metas,
                    ))
                }
            }
            ctx_ref
                .try_set_partitions(Partitions::create(PartitionsShuffleKind::Mod, partitions))?;
            Ok(())
        });

        // If there is a `PrewhereInfo`, the final output should be `PrehwereInfo.output_columns`.
        // `PrewhereInfo.output_columns` should be a subset of `PushDownInfo.projection`.
        let output_projection = match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
            None => {
                PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs)
            }
            Some(v) => v.output_columns,
        };
        let output_schema = Arc::new(DataSchema::from(
            &output_projection.project_schema(&plan.source_info.schema()),
        ));

        let prewhere_reader = self.build_prewhere_reader(plan)?;
        let prewhere_filter =
            self.build_prewhere_filter_expr(ctx.clone(), plan, prewhere_reader.output_schema())?;
        let remain_reader = self.build_remain_reader(plan)?;

        // Add source pipe.
        pipeline.add_source(
            |output| {
                ParquetSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    prewhere_reader.clone(),
                    prewhere_filter.clone(),
                    remain_reader.clone(),
                )
            },
            max_io_requests,
        )?;

        // Resize pipeline to max threads.
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let resize_to = std::cmp::min(max_threads, max_io_requests);

        pipeline.resize(resize_to)
    }
}
