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
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use parquet::arrow::arrow_to_parquet_schema;

use super::source::ParquetPredicate;
use super::source::ParquetSource;
use super::ParquetRSTable;

impl ParquetRSTable {
    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let table_schema: TableSchemaRef = self.table_info.schema();
        let source_projection =
            PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);
        let schema_descr = arrow_to_parquet_schema(&self.arrow_schema)?;
        let projection = source_projection.to_arrow_projection(&schema_descr)?;
        let predicate = Arc::new(
            PushDownInfo::prewhere_of_push_downs(&plan.push_downs)
                .map(|prewhere| {
                    let schema = prewhere.prewhere_columns.project_schema(&table_schema);
                    let filter = prewhere
                        .filter
                        .as_expr(&BUILTIN_FUNCTIONS)
                        .project_column_ref(|name| schema.index_of(name).unwrap());
                    let projection = prewhere
                        .prewhere_columns
                        .to_arrow_projection(&schema_descr)?;
                    Ok::<_, ErrorCode>(ParquetPredicate { projection, filter })
                })
                .transpose()?,
        );

        // TODOs:
        // - introduce Top-K optimization.
        // - adjust parallelism by data sizes.
        pipeline.add_source(
            |output| {
                ParquetSource::create(
                    ctx.clone(),
                    output,
                    self.operator.clone(),
                    projection.clone(),
                    predicate.clone(),
                )
            },
            ctx.get_settings().get_max_threads()? as usize,
        )
    }
}
