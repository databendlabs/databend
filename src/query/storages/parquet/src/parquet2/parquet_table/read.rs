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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TopKSorter;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::Pipeline;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;

use super::Parquet2Table;
use crate::parquet2::parquet_reader::Parquet2Reader;
use crate::parquet2::processors::AsyncParquet2Source;
use crate::parquet2::processors::Parquet2DeserializeTransform;
use crate::parquet2::processors::SyncParquet2Source;
use crate::utils::calc_parallelism;

#[derive(Clone)]
pub struct Parquet2PrewhereInfo {
    pub func_ctx: FunctionContext,
    pub reader: Arc<Parquet2Reader>,
    pub filter: Expr,
    pub top_k: Option<(usize, TopKSorter)>,
    // the usize is the index of the column in ParquetReader.schema
}

impl Parquet2Table {
    fn build_filter(filter: &RemoteExpr<String>, schema: &DataSchema) -> Expr {
        filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap())
    }

    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let table_schema: TableSchemaRef = self.table_info.schema();
        let source_projection =
            PushDownInfo::projection_of_push_downs(&table_schema, plan.push_downs.as_ref());

        // The front of the src_fields are prewhere columns (if exist).
        // The back of the src_fields are remain columns.
        let mut src_fields = Vec::with_capacity(source_projection.len());

        // The schema of the data block `read_data` output.
        let output_schema: Arc<DataSchema> = Arc::new(plan.schema().into());

        // Build the reader for parquet source.
        let source_reader = Parquet2Reader::create(
            self.operator.clone(),
            &self.arrow_schema,
            &self.schema_descr,
            source_projection,
        )?;

        // build top k information
        let top_k = plan
            .push_downs
            .as_ref()
            .map(|p| p.top_k(&table_schema, RangeIndex::supported_type))
            .unwrap_or_default();

        // Build prewhere info.
        let mut push_down_prewhere = PushDownInfo::prewhere_of_push_downs(plan.push_downs.as_ref());

        let top_k = if let Some((prewhere, top_k)) = push_down_prewhere.as_mut().zip(top_k) {
            // If there is a top k, we need to add the top k columns to the prewhere columns.
            if let RemoteExpr::<String>::ColumnRef { id, .. } =
                &plan.push_downs.as_ref().unwrap().order_by[0].0
            {
                let index = table_schema.index_of(id)?;
                prewhere.remain_columns.remove_col(index);
                prewhere.prewhere_columns.add_col(index);
                Some((id.clone(), top_k))
            } else {
                None
            }
        } else {
            None
        };

        // Build remain reader.
        // If there is no prewhere filter, remain reader is the same as source reader  (no prewhere phase, deserialize directly).
        let remain_reader = if let Some(p) = &push_down_prewhere {
            Parquet2Reader::create(
                self.operator.clone(),
                &self.arrow_schema,
                &self.schema_descr,
                p.remain_columns.clone(),
            )?
        } else {
            source_reader.clone()
        };

        let prewhere_info = push_down_prewhere
            .map(|p| {
                let reader = Parquet2Reader::create(
                    self.operator.clone(),
                    &self.arrow_schema,
                    &self.schema_descr,
                    p.prewhere_columns,
                )?;
                src_fields.extend_from_slice(reader.output_schema.fields());
                let filter = Self::build_filter(&p.filter, &reader.output_schema);
                let top_k = top_k.map(|(name, top_k)| {
                    (
                        reader.output_schema.index_of(&name).unwrap(),
                        TopKSorter::new(top_k.limit, top_k.asc),
                    )
                });
                let func_ctx = ctx.get_function_context()?;
                Ok::<_, ErrorCode>(Parquet2PrewhereInfo {
                    func_ctx,
                    reader,
                    filter,
                    top_k,
                })
            })
            .transpose()?;

        src_fields.extend_from_slice(remain_reader.output_schema.fields());
        let src_schema = DataSchemaRefExt::create(src_fields);
        let is_blocking = self.operator.info().native_capability().blocking;

        let num_deserializer = calc_parallelism(&ctx, plan)?;
        if is_blocking {
            pipeline.add_source(
                |output| SyncParquet2Source::create(ctx.clone(), output, source_reader.clone()),
                num_deserializer,
            )?;
        } else {
            pipeline.add_source(
                |output| AsyncParquet2Source::create(ctx.clone(), output, source_reader.clone()),
                num_deserializer,
            )?;
        };

        pipeline.add_transform(|input, output| {
            Parquet2DeserializeTransform::create(
                ctx.clone(),
                input,
                output,
                src_schema.clone(),
                output_schema.clone(),
                prewhere_info.clone(),
                source_reader.clone(),
                remain_reader.clone(),
                Arc::new(self.create_pruner(ctx.clone(), plan.push_downs.clone(), true)?),
            )
        })
    }
}
