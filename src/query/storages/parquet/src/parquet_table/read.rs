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

use common_base::runtime::GLOBAL_MEM_STAT;
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
            &self.arrow_schema,
            &self.schema_descr,
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
            &self.arrow_schema,
            &self.schema_descr,
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
                &self.arrow_schema,
                &self.schema_descr,
                p.remain_columns.clone(),
            )?
        } else {
            source_reader.clone()
        };

        let prewhere_info = push_down_prewhere
            .map(|p| {
                let reader = ParquetReader::create(
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

        let num_deserializer = calc_parallelism(&ctx, plan)?;
        if is_blocking {
            pipeline.add_source(
                |output| SyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_deserializer,
            )?;
        } else {
            pipeline.add_source(
                |output| AsyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_deserializer,
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

fn limit_parallelism_by_memory(max_memory: usize, sizes: &mut [(usize, usize)]) -> usize {
    // there may be 1 block  reading and 2 blocks in deserializer and sink.
    // memory size of a can be as large as 2 * uncompressed_size.
    // e.g. parquet may use 4 bytes for each string offset, but Block use 8 bytes i64.
    // we can refine it later if this leads to too low parallelism.
    let mut mem = 0;
    for (i, (uncompressed, compressed)) in sizes.iter().enumerate() {
        let s = uncompressed * 2 * 2 + compressed;
        mem += s;
        if mem > max_memory {
            return i;
        }
    }
    sizes.len()
}

fn calc_parallelism(ctx: &Arc<dyn TableContext>, plan: &DataSourcePlan) -> Result<usize> {
    if plan.parts.partitions.is_empty() {
        return Ok(1);
    }
    let settings = ctx.get_settings();
    let num_partitions = plan.parts.partitions.len();
    let max_threads = settings.get_max_threads()? as usize;
    let max_memory = settings.get_max_memory_usage()? as usize;

    let mut sizes = vec![];
    for p in plan.parts.partitions.iter() {
        let p = ParquetPart::from_part(p)?;
        sizes.push((p.uncompressed_size() as usize, p.compressed_size() as usize));
    }
    sizes.sort_by(|a, b| b.cmp(a));
    let max_split_size = sizes[0].0;
    // 1. used by other query
    // 2. used for file metas, can be huge when there are many files.
    let used_memory = GLOBAL_MEM_STAT.get_memory_usage();
    let available_memory = max_memory.saturating_sub(used_memory as usize);

    let max_by_memory = limit_parallelism_by_memory(available_memory, &mut sizes).max(1);
    if max_by_memory == 0 {
        return Err(ErrorCode::Overflow(format!(
            "Memory limit exceeded before start copy pipeline: max_memory_usage: {}, used_memory: {}, max_split_size = {}",
            max_memory, used_memory, max_split_size,
        )));
    }
    let num_deserializer = max_threads.min(max_by_memory).max(1);

    tracing::info!(
        "loading {num_partitions} partitions \
        with {num_deserializer} deserializers, \
        according to \
        max_split_size={max_split_size}, \
        max_threads={max_threads}, \
        max_memory={max_memory}, \
        available_memory={available_memory}"
    );
    Ok(num_deserializer)
}
