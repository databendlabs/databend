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

use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataSchema;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;

use super::table::arrow_to_table_schema;
use super::ParquetTable;
use super::TableContext;
use crate::parquet_part::ParquetLocationPart;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::ParquetSource;
use crate::pruning::prune_and_set_partitions;

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
        ctx: FunctionContext,
        plan: &DataSourcePlan,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    let expr = v
                        .filter
                        .as_expr(&BUILTIN_FUNCTIONS)
                        .project_column_ref(|name| schema.index_of(name).unwrap());
                    let (expr, _) = ConstantFolder::fold(&expr, ctx, &BUILTIN_FUNCTIONS);
                    Arc::new(Some(expr))
                }
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
            ParquetReader::do_projection(&self.arrow_schema, &columns_to_read)?;
        let schema = Arc::new(arrow_to_table_schema(projected_arrow_schema));
        let filters = push_downs.as_ref().map(|extra| {
            extra
                .filters
                .iter()
                .map(|f| f.as_expr(&BUILTIN_FUNCTIONS))
                .collect::<Vec<_>>()
        });

        let read_options = self.read_options;

        pipeline.set_on_init(move || {
            prune_and_set_partitions(
                &ctx_ref,
                &locations,
                &schema,
                &filters.as_deref(),
                &columns_to_read,
                &projected_column_nodes,
                skip_pruning,
                read_options,
            )
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
        let prewhere_filter = self.build_prewhere_filter_expr(
            ctx.try_get_function_context()?,
            plan,
            prewhere_reader.output_schema(),
        )?;
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
                    self.read_options,
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
