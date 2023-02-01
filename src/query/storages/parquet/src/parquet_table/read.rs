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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;

use super::ParquetTable;
use crate::deserialize_transform::ParquetDeserializeTransform;
use crate::deserialize_transform::ParquetPrewhereInfo;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::AsyncParquetSource;
use crate::parquet_source::SyncParquetSource;

impl ParquetTable {
    pub fn create_reader(&self, projection: Projection) -> Result<Arc<ParquetReader>> {
        ParquetReader::create(self.operator.clone(), self.arrow_schema.clone(), projection)
    }

    fn build_filter(
        ctx: FunctionContext,
        filter: &RemoteExpr<String>,
        schema: &DataSchema,
    ) -> Expr {
        let expr = filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap());
        let (expr, _) = ConstantFolder::fold(&expr, ctx, &BUILTIN_FUNCTIONS);
        expr
    }

    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let source_projection =
            PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs);

        // The front of the src_fields are prewhere columns (if exist).
        // The back of the src_fields are remain columns.
        let mut src_fields = Vec::with_capacity(source_projection.len());

        // The schema of the data block `read_data` output.
        let output_schema: Arc<DataSchema> = Arc::new(plan.schema().into());

        // Build the reader for parquet source.
        let source_reader = ParquetReader::create(
            self.operator.clone(),
            self.arrow_schema.clone(),
            source_projection,
        )?;

        let push_down_prewhere = PushDownInfo::prewhere_of_push_downs(&plan.push_downs);

        // Build remain reader.
        // If there is no prewhere filter, remain reader is the same as source reader  (no prewhere phase, deserialize directly).
        let remain_reader = if let Some(p) = &push_down_prewhere {
            ParquetReader::create(
                self.operator.clone(),
                self.arrow_schema.clone(),
                p.remain_columns.clone(),
            )?
        } else {
            source_reader.clone()
        };

        // Build prewhere info.

        let prewhere_info = Arc::new(
            PushDownInfo::prewhere_of_push_downs(&plan.push_downs)
                .map(|p| {
                    let reader = ParquetReader::create(
                        self.operator.clone(),
                        self.arrow_schema.clone(),
                        p.prewhere_columns,
                    )?;
                    src_fields.extend_from_slice(reader.output_schema.fields());
                    let func_ctx = ctx.get_function_context()?;
                    let filter = Self::build_filter(func_ctx, &p.filter, &reader.output_schema);
                    Ok::<_, ErrorCode>(ParquetPrewhereInfo {
                        func_ctx,
                        reader,
                        filter,
                    })
                })
                .transpose()?,
        );

        src_fields.extend_from_slice(remain_reader.output_schema.fields());
        let src_schema = DataSchemaRefExt::create(src_fields);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;

        // Add source pipe.
        if self.operator.metadata().can_blocking() {
            pipeline.add_source(
                |output| SyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                max_threads,
            )?;
        } else {
            let max_io_requests = std::cmp::max(
                max_threads,
                ctx.get_settings().get_max_storage_io_requests()? as usize,
            );
            pipeline.add_source(
                |output| AsyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                max_io_requests,
            )?;
            pipeline.resize(std::cmp::min(max_threads, max_io_requests))?;
        }
        pipeline.add_transform(|input, output| {
            ParquetDeserializeTransform::create(
                ctx.clone(),
                input,
                output,
                src_schema.clone(),
                output_schema.clone(),
                prewhere_info.clone(),
                remain_reader.clone(),
            )
        })
    }
}
