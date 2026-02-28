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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_sql::IndexType;
use databend_storages_common_io::ReadSettings;
use roaring::RoaringTreemap;

use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::fuse_vortex::read_vortex_with_ranges;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;

pub struct ReadVortexDataTransform {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    read_settings: ReadSettings,
    scan_progress: Arc<Progress>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    prewhere_input_columns: Vec<usize>,
    prewhere_filter: Option<Expr>,
    base_block_ids: Option<databend_common_expression::Scalar>,
    need_reserve_block_info: bool,
}

impl ReadVortexDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        index_reader: Arc<Option<AggIndexReader>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let src_schema: DataSchema = (block_reader.schema().as_ref()).into();
        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();
        let (prewhere_input_columns, prewhere_filter) =
            Self::build_prewhere_filter(plan, table_schema.as_ref(), &src_schema)?;
        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        let func_ctx = ctx.get_function_context()?;

        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadVortexDataTransform {
                func_ctx,
                block_reader,
                index_reader,
                table_schema,
                scan_id,
                context: ctx.clone(),
                read_settings: ReadSettings::from_ctx(&ctx)?,
                scan_progress: ctx.get_scan_progress(),
                src_schema,
                output_schema,
                prewhere_input_columns,
                prewhere_filter,
                base_block_ids: plan.base_block_ids.clone(),
                need_reserve_block_info,
            },
        )))
    }

    fn build_prewhere_filter(
        plan: &DataSourcePlan,
        table_schema: &TableSchema,
        src_schema: &DataSchema,
    ) -> Result<(Vec<usize>, Option<Expr>)> {
        let Some(prewhere) = PushDownInfo::prewhere_of_push_downs(plan.push_downs.as_ref()) else {
            return Ok((Vec::new(), None));
        };

        let prewhere_schema = prewhere.prewhere_columns.project_schema(table_schema);
        let prewhere_schema: DataSchema = (&prewhere_schema).into();
        let prewhere_input_columns = prewhere_schema
            .fields()
            .iter()
            .map(|field| src_schema.index_of(field.name()))
            .collect::<Result<Vec<_>>>()?;
        let prewhere_filter = prewhere
            .filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| {
                prewhere_schema
                    .column_with_name(name)
                    .map(|(index, _)| index)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Unable to find prewhere column '{name}' in vortex read schema"
                        ))
                    })
            })?;

        Ok((prewhere_input_columns, Some(prewhere_filter)))
    }

    fn build_prewhere_input_block(&self, data_block: &DataBlock) -> DataBlock {
        let entries = self
            .prewhere_input_columns
            .iter()
            .map(|&index| data_block.get_by_offset(index).clone())
            .collect();
        DataBlock::new(entries, data_block.num_rows())
    }

    fn offsets_from_bitmap(bitmap: &Bitmap) -> RoaringTreemap {
        RoaringTreemap::from_sorted_iter(
            (0..bitmap.len())
                .filter(|i| unsafe { bitmap.get_bit_unchecked(*i) })
                .map(|i| i as u64),
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadVortexDataTransform {
    const NAME: &'static str = "AsyncReadVortexDataTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let Some(meta) = data.get_meta() else {
            return Err(ErrorCode::Internal(
                "AsyncReadVortexDataTransform get wrong meta data",
            ));
        };

        let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) else {
            return Err(ErrorCode::Internal(
                "AsyncReadVortexDataTransform get wrong meta data",
            ));
        };

        if block_part_meta.part_ptr.len() != 1 {
            return Err(ErrorCode::Internal(format!(
                "Vortex source expects exactly one partition per input, but got {}",
                block_part_meta.part_ptr.len()
            )));
        }

        let part = block_part_meta.part_ptr[0].clone();
        let runtime_filter = ExprRuntimePruner::new(
            self.context
                .get_runtime_filters(self.scan_id)
                .into_iter()
                .flat_map(|entry| {
                    let mut exprs = Vec::new();
                    if let Some(expr) = entry.inlist.clone() {
                        exprs.push(RuntimeFilterExpr {
                            filter_id: entry.id,
                            expr,
                            stats: entry.stats.clone(),
                        });
                    }
                    if let Some(expr) = entry.min_max.clone() {
                        exprs.push(RuntimeFilterExpr {
                            filter_id: entry.id,
                            expr,
                            stats: entry.stats.clone(),
                        });
                    }
                    exprs
                })
                .collect(),
        );
        if runtime_filter.prune(&self.func_ctx, self.table_schema.clone(), &part)? {
            return Ok(DataBlock::empty_with_schema(&self.output_schema));
        }

        let fuse_part = FuseBlockPartInfo::from_part(&part)?;

        let mut from_agg_index = false;
        let mut data_block = if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some((actual_part, data)) = index_reader
                .read_parquet_data_by_merge_io(&self.read_settings, &loc)
                .await
            {
                from_agg_index = true;
                index_reader.deserialize_parquet_data(actual_part, data)?
            } else {
                let (file_size, prefetched_ranges) = self
                    .block_reader
                    .read_vortex_data_by_merge_io(
                        &self.read_settings,
                        &fuse_part.location,
                        &fuse_part.columns_meta,
                    )
                    .await?;
                read_vortex_with_ranges(
                    self.block_reader.schema().as_ref(),
                    file_size,
                    prefetched_ranges,
                )?
            }
        } else {
            let (file_size, prefetched_ranges) = self
                .block_reader
                .read_vortex_data_by_merge_io(
                    &self.read_settings,
                    &fuse_part.location,
                    &fuse_part.columns_meta,
                )
                .await?;
            read_vortex_with_ranges(
                self.block_reader.schema().as_ref(),
                file_size,
                prefetched_ranges,
            )?
        };

        let mut offsets = None;
        if !from_agg_index {
            let mut bitmap_selection = None;
            if let Some(prewhere_filter) = &self.prewhere_filter {
                let prewhere_block = self.build_prewhere_input_block(&data_block);
                let num_rows = prewhere_block.num_rows();
                let evaluator = Evaluator::new(&prewhere_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let filter = evaluator
                    .run(prewhere_filter)?
                    .try_downcast::<BooleanType>()
                    .unwrap();
                let bitmap: Bitmap = FilterHelpers::filter_to_bitmap(filter, num_rows).into();
                data_block = data_block.filter_with_bitmap(&bitmap)?;
                bitmap_selection = Some(bitmap);
            }

            data_block = data_block.resort(&self.src_schema, &self.output_schema)?;

            offsets = if self.block_reader.query_internal_columns() {
                bitmap_selection.as_ref().map(Self::offsets_from_bitmap)
            } else {
                None
            };
        }

        let progress_values = ProgressValues {
            rows: data_block.num_rows(),
            bytes: data_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, data_block.memory_size());

        data_block = add_data_block_meta(
            data_block,
            fuse_part,
            offsets,
            self.base_block_ids.clone(),
            self.block_reader.update_stream_columns(),
            self.block_reader.query_internal_columns(),
            self.need_reserve_block_info,
        )?;

        Ok(data_block)
    }
}
