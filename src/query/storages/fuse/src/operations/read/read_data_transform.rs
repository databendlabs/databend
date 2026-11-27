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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::runtime_filter_info::RuntimeScanFilters;
use databend_common_catalog::runtime_filter_info::RuntimeScanStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::IndexType;

use super::condition_cache;
use super::condition_cache::CachedCondition;
use super::condition_cache::ConditionKeys;
use super::parquet_data_source::BlockSource;
use super::parquet_data_source::ParquetDataSource;
use super::read_block_context::ReadBlockContext;
use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;

/// Turns pruned partitions into block read sources.
///
/// Partitions that only need a subset of their granules are emitted synchronously as granule
/// reads. Every other partition is read one at a time in `async_process` and emitted before the
/// next read starts, so downstream backpressure bounds both the read concurrency and the amount
/// of buffered block data.
pub struct ReadDataTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    pending_output: VecDeque<DataBlock>,
    remaining_parts: VecDeque<PartInfoPtr>,
    async_output: Option<DataBlock>,
    expr_runtime_pruner: Option<ExprRuntimePruner>,

    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    index_score: Option<InternalColumnType>,
    context: Arc<dyn TableContext>,
    runtime_scan_filters: RuntimeScanFilters,
    record_partitions: bool,
    /// The key of the prewhere filter in the condition cache, if it is cached.
    condition_keys: Option<ConditionKeys>,
    /// Whether the scan outputs columns (or virtual columns), besides the internal ones.
    outputs_columns: bool,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        index_score: Option<InternalColumnType>,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        record_partitions: bool,
        prewhere_info: Option<&PrewhereInfo>,
        outputs_columns: bool,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let condition_keys = match prewhere_info {
            Some(prewhere_info) if ctx.get_settings().get_enable_condition_cache()? => {
                condition_cache::condition_keys(
                    prewhere_info,
                    &block_reader.original_schema,
                    &func_ctx,
                )
            }
            _ => None,
        };
        let runtime_scan_filters = ctx.get_runtime_scan_filters(scan_id);
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            pending_output: VecDeque::new(),
            remaining_parts: VecDeque::new(),
            async_output: None,
            expr_runtime_pruner: None,
            func_ctx,
            block_reader,
            read_block_context,
            table_schema,
            scan_id,
            index_score,
            context: ctx,
            runtime_scan_filters,
            record_partitions,
            condition_keys,
            outputs_columns,
        })))
    }

    fn create_runtime_pruners(&self) -> Result<ExprRuntimePruner> {
        let read_settings = self.read_block_context.read_settings();
        let settings = self.context.get_settings();
        let threshold = settings.get_inlist_runtime_bloom_prune_threshold()? as usize;
        let runtime_filters = self.context.get_runtime_filters(self.scan_id);
        let mut filter_exprs = Vec::new();
        for filter in &runtime_filters {
            filter_exprs.extend(RuntimeFilterExpr::from_entry(filter));
        }

        Ok(ExprRuntimePruner::new(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            self.block_reader.operator(),
            read_settings,
            self.block_reader.policy,
            threshold,
            filter_exprs,
        ))
    }

    /// Runtime scan statistics of a partition, including typed virtual column statistics.
    fn scan_statistics(part_info: &FuseBlockPartInfo) -> RuntimeScanStatistics<'_> {
        let virtual_stats = part_info
            .block_meta_index
            .as_ref()
            .and_then(|index| index.virtual_block_meta.as_ref())
            .map(|meta| &meta.virtual_column_stats);
        RuntimeScanStatistics::new(part_info.columns_stat.as_ref(), virtual_stats)
    }

    fn classify_parts(&mut self, parts: Vec<PartInfoPtr>) -> Result<()> {
        if self.record_partitions {
            Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, parts.len());
        }
        for part in parts {
            let part_info = FuseBlockPartInfo::from_part(&part)?;
            if self
                .runtime_scan_filters
                .should_prune(Self::scan_statistics(part_info))
            {
                continue;
            }

            let Some(groups) = self.read_block_context.granule_groups_if_subset(&part)? else {
                self.remaining_parts.push_back(part);
                continue;
            };

            // A granule read decodes only some rows of the block, so it neither uses nor
            // records the condition entry of the block.
            let block = match self.block_shortcut(&part, part_info) {
                BlockShortcut::Skip => continue,
                BlockShortcut::Output(block) => block,
                BlockShortcut::Read(_) => {
                    DataBlock::empty_with_meta(DataSourceWithMeta::create(vec![part], vec![
                        BlockSource::Granule(groups),
                    ]))
                }
            };
            self.pending_output.push_back(block);
        }
        Ok(())
    }

    /// How a partition that survived pruning is output without reading it, if it can be.
    fn block_shortcut(&self, part: &PartInfoPtr, part_info: &FuseBlockPartInfo) -> BlockShortcut {
        // The index matches are every row this zero-column scan outputs, with
        // their scores: no Parquet read. `index_score` excludes prewhere, so this
        // never competes with the condition cache below.
        if let Some(key) = &self.index_score
            && let Some(matches) = part_info.covering_matches(key)
            && !self.context.has_bloom_runtime_filters(self.scan_id)
        {
            let offsets = matches.rows.iter().map(|&row| u64::from(row)).collect();
            return BlockShortcut::Output(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                vec![part.clone()],
                vec![BlockSource::IndexRows(offsets)],
            )));
        }

        // A block where the prewhere filter selected no row before is not read, nor one
        // where it knows the rows when the scan outputs no column.
        let condition = self
            .condition_keys
            .as_ref()
            .and_then(|keys| condition_cache::lookup_condition(keys, part_info));
        if let Some(condition) = &condition {
            if condition.selects_nothing() {
                return BlockShortcut::Skip;
            }
            if !self.outputs_columns && condition.is_exact() {
                return BlockShortcut::Output(DataBlock::empty_with_meta(
                    DataSourceWithMeta::create(vec![part.clone()], vec![BlockSource::Parquet(
                        ParquetDataSource::skipped(condition.clone()),
                    )]),
                ));
            }
        }
        BlockShortcut::Read(condition)
    }

    /// Read the next remaining partition that survives runtime pruning, or `None` once the
    /// current batch is exhausted.
    async fn read_next_remaining_part(&mut self) -> Result<Option<DataBlock>> {
        if self.expr_runtime_pruner.is_none() {
            self.expr_runtime_pruner = Some(self.create_runtime_pruners()?);
        }

        'parts: while let Some(part) = self.remaining_parts.pop_front() {
            let part_info = FuseBlockPartInfo::from_part(&part)?;
            let stats = Self::scan_statistics(part_info);
            if self.runtime_scan_filters.should_prune(stats)
                || self
                    .expr_runtime_pruner
                    .as_ref()
                    .unwrap()
                    .prune(&part)
                    .await?
            {
                continue;
            }
            let condition = match self.block_shortcut(&part, part_info) {
                BlockShortcut::Skip => continue,
                BlockShortcut::Output(block) => return Ok(Some(block)),
                BlockShortcut::Read(condition) => condition,
            };

            let mut source = match self.read_block_context.granule_groups(&part, None)? {
                // The groups bound the rows decoded at a time. A granule read neither uses
                // nor records the condition entry of the block.
                Some(groups) => {
                    return Ok(Some(DataBlock::empty_with_meta(
                        DataSourceWithMeta::create(vec![part], vec![BlockSource::Granule(groups)]),
                    )));
                }
                None if self.runtime_scan_filters.is_empty() => {
                    self.read_block_context.read_full_data(part.clone()).await?
                }
                None => {
                    let read = self.read_block_context.read_full_data(part.clone());
                    tokio::pin!(read);
                    loop {
                        // Subscribe before checking so a boundary update cannot be missed.
                        let rechecks = self.runtime_scan_filters.recheck_notified();
                        // `select_all` panics on empty input.
                        debug_assert!(!rechecks.is_empty());
                        if self.runtime_scan_filters.should_prune(stats) {
                            continue 'parts;
                        }

                        tokio::select! {
                            result = &mut read => break result?,
                            _ = futures::future::select_all(rechecks) => {}
                        }
                    }
                }
            };

            source.condition = condition;
            return Ok(Some(DataBlock::empty_with_meta(
                DataSourceWithMeta::create(vec![part], vec![BlockSource::Parquet(source)]),
            )));
        }

        self.expr_runtime_pruner = None;
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Processor for ReadDataTransform {
    fn name(&self) -> String {
        String::from("AsyncReadDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        loop {
            if self.output.is_finished() {
                self.input.finish();
                return Ok(Event::Finished);
            }

            if self.runtime_scan_filters.is_finished() {
                self.remaining_parts.clear();
                self.pending_output.clear();
                self.async_output = None;
                self.input.finish();
                self.output.finish();
                return Ok(Event::Finished);
            }

            if !self.output.can_push() {
                self.input.set_not_need_data();
                return Ok(Event::NeedConsume);
            }

            if let Some(block) = self.pending_output.pop_front() {
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if let Some(block) = self.async_output.take() {
                self.output.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if !self.remaining_parts.is_empty() {
                return Ok(Event::Async);
            }

            if self.input.has_data() {
                let mut block = self.input.pull_data().unwrap()?;
                let parts = block
                    .take_meta()
                    .and_then(BlockPartitionMeta::downcast_from)
                    .and_then(|meta| (!meta.part_ptr.is_empty()).then_some(meta.part_ptr))
                    .ok_or_else(|| ErrorCode::Internal("ReadDataTransform got wrong meta data"))?;
                self.classify_parts(parts)?;
                continue;
            }

            if self.input.is_finished() {
                self.output.finish();
                return Ok(Event::Finished);
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        self.async_output = self.read_next_remaining_part().await?;
        Ok(())
    }
}

/// See [`ReadDataTransform::block_shortcut`].
enum BlockShortcut {
    /// The block has no row to output.
    Skip,
    /// The output of the block, without reading it.
    Output(DataBlock),
    /// The block is read, with its condition cache entry, if any.
    Read(Option<CachedCondition>),
}
