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
use databend_common_catalog::runtime_filter_info::RuntimeScanStatistics;
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
use databend_common_pipeline_transforms::processors::AsyncBlockingTransform;
use databend_common_pipeline_transforms::processors::AsyncBlockingTransformer;
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
    expr_runtime_pruner: Option<ExprRuntimePruner>,
    record_partitions: bool,
    parts: std::vec::IntoIter<PartInfoPtr>,
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
        Ok(ProcessorPtr::create(AsyncBlockingTransformer::create(
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
                expr_runtime_pruner: None,
                record_partitions,
                parts: Vec::new().into_iter(),
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
}

#[async_trait::async_trait]
impl AsyncBlockingTransform for ReadDataTransform {
    const NAME: &'static str = "AsyncReadDataTransform";

    async fn consume(&mut self, mut data: DataBlock) -> Result<()> {
        let meta = data
            .take_meta()
            .and_then(BlockPartitionMeta::downcast_from)
            .filter(|meta| !meta.part_ptr.is_empty())
            .ok_or_else(|| ErrorCode::Internal("AsyncReadDataTransform got wrong meta data"))?;

        if self.record_partitions {
            Profile::record_usize_profile(
                ProfileStatisticsName::ScanPartitions,
                meta.part_ptr.len(),
            );
        }

        self.expr_runtime_pruner = Some(self.create_runtime_pruners()?);
        self.parts = meta.part_ptr.into_iter();
        Ok(())
    }

    async fn transform(&mut self) -> Result<Option<DataBlock>> {
        let expr_runtime_pruner = self.expr_runtime_pruner.as_ref().unwrap();

        // Batch only the metadata. Return each block before reading the next one so downstream
        // backpressure bounds both the read concurrency and the buffered block data.
        'parts: for part in self.parts.by_ref() {
            let part_info = FuseBlockPartInfo::from_part(&part)?;
            let virtual_stats = part_info
                .block_meta_index
                .as_ref()
                .and_then(|index| index.virtual_block_meta.as_ref())
                .map(|meta| &meta.virtual_column_stats);
            let stats = RuntimeScanStatistics::new(part_info.columns_stat.as_ref(), virtual_stats);
            if self.runtime_scan_filters.should_prune(stats)
                || expr_runtime_pruner.prune(&part).await?
            {
                continue;
            }

            let source = if self.runtime_scan_filters.is_empty() {
                self.read_block_context.read_data(part.clone()).await?
            } else {
                let read = self.read_block_context.read_data(part.clone());
                tokio::pin!(read);
                loop {
                    // Subscribe before checking so a boundary update cannot be missed.
                    let rechecks = self.runtime_scan_filters.recheck_notified();
                    debug_assert!(!rechecks.is_empty());
                    if self.runtime_scan_filters.should_prune(stats) {
                        continue 'parts;
                    }

                    tokio::select! {
                        result = &mut read => break result?,
                        _ = futures::future::select_all(rechecks) => {}
                    }
                }
            };

            return Ok(Some(DataBlock::empty_with_meta(
                DataSourceWithMeta::create(vec![part], vec![source]),
            )));
        }

        self.expr_runtime_pruner = None;
        Ok(None)
    }
}
