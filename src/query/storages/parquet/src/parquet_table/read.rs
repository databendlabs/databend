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
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataSchema;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;

use super::ParquetTable;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::ParquetSource;

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

    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        // If there is a `PrewhereInfo`, the final output should be `PrehwereInfo.output_columns`.
        // `PrewhereInfo.output_columns` should be a subset of `PushDownInfo.projection`.
        let output_projection = match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
            None => {
                PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs)
            }
            Some(v) => v.output_columns,
        };

        let prewhere_reader = self.build_prewhere_reader(plan)?;
        let prewhere_filter = self.build_prewhere_filter_expr(
            ctx.get_function_context()?,
            plan,
            prewhere_reader.output_schema(),
        )?;
        let remain_reader = self.build_remain_reader(plan)?;

        // Build three kinds of schemas.
        // The schemas are used for `DataBlock::resort` to remove columns that are not needed.
        // Remove columns before `DataBlock::filter` can reduce memory copy.
        // 1. The final output schema.
        let output_schema = Arc::new(DataSchema::from(
            &output_projection.project_schema(&plan.source_info.schema()),
        ));
        // 2. The schema after filter. Remove columns read by prewhere reader but will not be output.
        let output_fields = output_schema.fields();
        let prewhere_schema = prewhere_reader.output_schema();
        let remain_fields = if let Some(reader) = remain_reader.as_ref() {
            reader.output_schema().fields().clone()
        } else {
            vec![]
        };
        let mut after_filter_fields = Vec::with_capacity(output_fields.len() - remain_fields.len());
        // Ensure the order of fields in `after_filter_fields` is the same as `output_fields`.
        // It will reduce the resort times.
        for field in output_fields {
            if prewhere_schema.field_with_name(field.name()).is_ok() {
                after_filter_fields.push(field.clone());
            }
        }
        // 3. The schema after add remain columns.
        let mut after_remain_fields = after_filter_fields.clone();
        after_remain_fields.extend(remain_fields);

        let after_filter_schema = Arc::new(DataSchema::new(after_filter_fields));
        let after_remain_schema = Arc::new(DataSchema::new(after_remain_fields));

        let max_threads = ctx.get_settings().get_max_threads()? as usize;

        // Add source pipe.
        if self.operator.metadata().can_blocking() {
            pipeline.add_source(
                |output| {
                    ParquetSource::create(
                        ctx.clone(),
                        output,
                        (
                            after_filter_schema.clone(),
                            after_remain_schema.clone(),
                            output_schema.clone(),
                        ),
                        prewhere_reader.clone(),
                        prewhere_filter.clone(),
                        remain_reader.clone(),
                        self.read_options,
                    )
                },
                max_threads,
            )
        } else {
            let max_io_requests = std::cmp::max(
                max_threads,
                ctx.get_settings().get_max_storage_io_requests()? as usize,
            );

            pipeline.add_source(
                |output| {
                    ParquetSource::create(
                        ctx.clone(),
                        output,
                        (
                            after_filter_schema.clone(),
                            after_remain_schema.clone(),
                            output_schema.clone(),
                        ),
                        prewhere_reader.clone(),
                        prewhere_filter.clone(),
                        remain_reader.clone(),
                        self.read_options,
                    )
                },
                max_io_requests,
            )?;

            // Resize pipeline to max threads.
            let resize_to = std::cmp::min(max_threads, max_io_requests);
            pipeline.resize(resize_to)
        }
    }
}
