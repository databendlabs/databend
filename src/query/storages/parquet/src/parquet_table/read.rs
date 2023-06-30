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
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use common_expression::TopKSorter;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use super::ParquetTable;
use crate::deserialize_transform::ParquetDeserializeTransform;
use crate::deserialize_transform::ParquetPrewhereInfo;
use crate::parquet_part::ParquetPart;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::AsyncParquetSource;
use crate::parquet_source::SyncParquetSource;

impl ParquetTable {
    pub fn create_reader(&self, projection: Projection) -> Result<Arc<ParquetReader>> {
        ParquetReader::create(
            self.operator.clone(),
            self.arrow_schema.clone(),
            self.schema_descr.clone(),
            projection,
        )
    }

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
            PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);

        // The front of the src_fields are prewhere columns (if exist).
        // The back of the src_fields are remain columns.
        let mut src_fields = Vec::with_capacity(source_projection.len());

        // The schema of the data block `read_data` output.
        let output_schema: Arc<DataSchema> = Arc::new(plan.schema().into());

        // Build the reader for parquet source.
        let source_reader = ParquetReader::create(
            self.operator.clone(),
            self.arrow_schema.clone(),
            self.schema_descr.clone(),
            source_projection,
        )?;

        // build top k information
        let top_k = plan
            .push_downs
            .as_ref()
            .map(|p| p.top_k(&table_schema, None, RangeIndex::supported_type))
            .unwrap_or_default();

        // Build prewhere info.
        let mut push_down_prewhere = PushDownInfo::prewhere_of_push_downs(&plan.push_downs);

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
            ParquetReader::create(
                self.operator.clone(),
                self.arrow_schema.clone(),
                self.schema_descr.clone(),
                p.remain_columns.clone(),
            )?
        } else {
            source_reader.clone()
        };

        let prewhere_info = push_down_prewhere
            .map(|p| {
                let reader = ParquetReader::create(
                    self.operator.clone(),
                    self.arrow_schema.clone(),
                    self.schema_descr.clone(),
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
                Ok::<_, ErrorCode>(ParquetPrewhereInfo {
                    func_ctx,
                    reader,
                    filter,
                    top_k,
                })
            })
            .transpose()?;

        src_fields.extend_from_slice(remain_reader.output_schema.fields());
        let src_schema = DataSchemaRefExt::create(src_fields);
        let is_blocking = self.operator.info().can_blocking();

        let (num_reader, num_deserializer) = calc_parallelism(&ctx, plan, is_blocking)?;
        if is_blocking {
            pipeline.add_source(
                |output| SyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_reader,
            )?;
        } else {
            pipeline.add_source(
                |output| AsyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_reader,
            )?;
        };

        pipeline.try_resize(num_deserializer)?;

        pipeline.add_transform(|input, output| {
            ParquetDeserializeTransform::create(
                ctx.clone(),
                input,
                output,
                src_schema.clone(),
                output_schema.clone(),
                prewhere_info.clone(),
                source_reader.clone(),
                remain_reader.clone(),
                self.create_pruner(ctx.clone(), plan.push_downs.clone(), true)?,
            )
        })
    }
}

fn limit_parallelism_by_memory(max_memory: usize, sizes: &mut [usize]) -> usize {
    sizes.sort_by(|a, b| b.cmp(a));
    let mut mem = 0;
    for (i, s) in sizes.iter().enumerate() {
        // there may be 2 blocks in a pipe.
        mem += s * 2;
        if mem > max_memory {
            return i;
        }
    }
    sizes.len()
}

fn calc_parallelism(
    ctx: &Arc<dyn TableContext>,
    plan: &DataSourcePlan,
    is_blocking: bool,
) -> Result<(usize, usize)> {
    if plan.parts.partitions.is_empty() {
        return Ok((1, 1));
    }
    let settings = ctx.get_settings();
    let num_partitions = plan.parts.partitions.len();
    let max_threads = settings.get_max_threads()? as usize;
    let mut sizes = vec![];
    for p in plan.parts.partitions.iter() {
        sizes.push(ParquetPart::from_part(p)?.uncompressed_size() as usize);
    }
    let num_chunks = ParquetPart::from_part(&plan.parts.partitions[0])?
        .num_io()
        .max(1);
    let max_memory = settings.get_max_memory_usage()? as usize;
    let max_by_memory = limit_parallelism_by_memory(max_memory, &mut sizes).max(1);

    let max_storage_io_requests = settings.get_max_storage_io_requests()? as usize;
    let num_readers = if is_blocking {
        max_storage_io_requests
    } else {
        max_storage_io_requests / num_chunks
    }
    .min(max_by_memory)
    .max(1);

    let num_deserializer = max_threads.min(max_by_memory).max(1);

    tracing::info!(
        "loading {num_partitions} partitions with {num_readers} readers and {num_deserializer} deserializers, blocking = {is_blocking}, according to max_memory={max_memory}, num_chunks={num_chunks}, max_storage_io_requests={max_storage_io_requests}, max_split_size={}, max_threads={max_threads}",
        sizes[0]
    );
    Ok((num_readers, num_deserializer))
}
