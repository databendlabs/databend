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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_sql::IndexType;
use databend_storages_common_io::ReadSettings;

use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::fuse_vortex::read_vortex;
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
    output_schema: DataSchema,
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
        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();
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
                output_schema,
                base_block_ids: plan.base_block_ids.clone(),
                need_reserve_block_info,
            },
        )))
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

        let mut data_block = if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some((actual_part, data)) = index_reader
                .read_parquet_data_by_merge_io(&self.read_settings, &loc)
                .await
            {
                index_reader.deserialize_parquet_data(actual_part, data)?
            } else {
                let data = self.block_reader.operator.read(&fuse_part.location).await?;
                read_vortex(
                    self.block_reader.schema().as_ref(),
                    data.to_bytes().as_ref(),
                )?
            }
        } else {
            let data = self.block_reader.operator.read(&fuse_part.location).await?;
            read_vortex(
                self.block_reader.schema().as_ref(),
                data.to_bytes().as_ref(),
            )?
        };

        let progress_values = ProgressValues {
            rows: data_block.num_rows(),
            bytes: data_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, data_block.memory_size());

        data_block = add_data_block_meta(
            data_block,
            fuse_part,
            None,
            self.base_block_ids.clone(),
            self.block_reader.update_stream_columns(),
            self.block_reader.query_internal_columns(),
            self.need_reserve_block_info,
        )?;

        Ok(data_block)
    }
}
