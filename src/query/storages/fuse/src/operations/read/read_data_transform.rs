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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeScanFilters;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_sql::IndexType;

use super::read_block_context::ReadBlockContext;
use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;

pub struct ReadDataTransform {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    runtime_scan_filters: RuntimeScanFilters,
    record_partitions: bool,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        record_partitions: bool,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let runtime_scan_filters = ctx.get_runtime_scan_filters(scan_id);
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadDataTransform {
                func_ctx,
                block_reader,
                read_block_context,
                table_schema,
                scan_id,
                context: ctx,
                runtime_scan_filters,
                record_partitions,
            },
        )))
    }

    fn create_runtime_pruners(&self) -> Result<ExprRuntimePruner> {
        let read_settings = self.read_block_context.read_settings();
        let inlist_bloom_prune_threshold =
            self.context
                .get_settings()
                .get_inlist_runtime_bloom_prune_threshold()? as usize;
        let runtime_filters = self.context.get_runtime_filters(self.scan_id);

        let runtime_filter = ExprRuntimePruner::new(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            self.block_reader.operator(),
            read_settings,
            inlist_bloom_prune_threshold,
            runtime_filters
                .iter()
                .flat_map(RuntimeFilterExpr::from_entry)
                .collect(),
        );

        Ok(runtime_filter)
    }

    async fn read_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let mut read_tasks = Vec::with_capacity(parts.len());
        let expr_runtime_pruner = self.create_runtime_pruners()?;

        for part in parts {
            if !self.runtime_scan_filters.is_empty() {
                let part_info = FuseBlockPartInfo::from_part(&part)?;
                if self
                    .runtime_scan_filters
                    .should_prune(part_info.columns_stat.as_ref())
                {
                    continue;
                }
            }

            if expr_runtime_pruner.prune(&part).await? {
                continue;
            }

            let filters = self.runtime_scan_filters.clone();
            let read_block_context = self.read_block_context.clone();
            read_tasks.push(async move {
                databend_common_base::runtime::spawn(async move {
                    if filters.is_empty() {
                        let source = read_block_context.read_data(part.clone()).await?;
                        return Ok::<_, ErrorCode>(Some((part, source)));
                    }

                    let read = read_block_context.read_data(part.clone());
                    tokio::pin!(read);
                    loop {
                        // Subscribe before checking so a boundary update cannot be missed.
                        let rechecks = filters.recheck_notified();
                        debug_assert!(!rechecks.is_empty());
                        let part_info = FuseBlockPartInfo::from_part(&part)?;
                        if filters.should_prune(part_info.columns_stat.as_ref()) {
                            return Ok::<_, ErrorCode>(None);
                        }

                        tokio::select! {
                            result = &mut read => {
                                let source = result?;
                                return Ok::<_, ErrorCode>(Some((part, source)));
                            }
                            _ = futures::future::select_all(rechecks) => {}
                        }
                    }
                })
                .await?
            });
        }

        let completed_reads = futures::future::try_join_all(read_tasks).await?;
        let mut parts_to_read = Vec::with_capacity(completed_reads.len());
        let mut sources = Vec::with_capacity(completed_reads.len());
        for (part, source) in completed_reads.into_iter().flatten() {
            parts_to_read.push(part);
            sources.push(source);
        }

        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            parts_to_read,
            sources,
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadDataTransform {
    const NAME: &'static str = "AsyncReadDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let parts = data
            .get_meta()
            .and_then(BlockPartitionMeta::downcast_ref_from)
            .and_then(|meta| (!meta.part_ptr.is_empty()).then(|| meta.part_ptr.clone()))
            .ok_or_else(|| ErrorCode::Internal("AsyncReadDataTransform got wrong meta data"))?;

        if self.record_partitions {
            Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, parts.len());
        }

        self.read_parts(parts).await
    }
}
