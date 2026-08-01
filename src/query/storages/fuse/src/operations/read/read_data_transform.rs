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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_base::JoinHandle;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeScanFilters;
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

use super::parquet_data_source::ParquetDataSource;
use super::read_block_context::ReadBlockContext;
use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;

struct AbortOnDrop<T> {
    task: JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    fn new(task: JoinHandle<T>) -> Self {
        Self { task }
    }
}

impl<T> Unpin for AbortOnDrop<T> {}

impl<T> Future for AbortOnDrop<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.task).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct ReadDataTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    pending_output: VecDeque<DataBlock>,
    remaining_parts: Vec<PartInfoPtr>,
    async_output: Option<DataBlock>,

    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    runtime_scan_filters: RuntimeScanFilters,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let runtime_scan_filters = ctx.get_runtime_scan_filters(scan_id);
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            pending_output: VecDeque::new(),
            remaining_parts: Vec::new(),
            async_output: None,
            func_ctx,
            block_reader,
            read_block_context,
            table_schema,
            scan_id,
            context: ctx,
            runtime_scan_filters,
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
            threshold,
            filter_exprs,
        ))
    }

    fn classify_parts(&mut self, parts: Vec<PartInfoPtr>) -> Result<()> {
        for part in parts {
            let part_info = FuseBlockPartInfo::from_part(&part)?;
            let columns_stat = part_info.columns_stat.as_ref();

            if self.runtime_scan_filters.should_prune(columns_stat) {
                continue;
            }

            let Some(groups) = self.read_block_context.granule_groups_if_subset(&part)? else {
                self.remaining_parts.push(part);
                continue;
            };

            self.pending_output
                .push_back(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                    vec![part],
                    vec![ParquetDataSource::Granule(groups)],
                )));
        }
        Ok(())
    }

    async fn read_remaining_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let mut parts_to_read = Vec::with_capacity(parts.len());
        let mut sources = Vec::with_capacity(parts.len());
        let mut full_reads = Vec::new();
        let expr_runtime_pruner = self.create_runtime_pruners()?;

        for part in parts {
            {
                let part_info = FuseBlockPartInfo::from_part(&part)?;
                let columns_stat = part_info.columns_stat.as_ref();

                if self.runtime_scan_filters.should_prune(columns_stat) {
                    continue;
                }
            }

            if expr_runtime_pruner.prune(&part).await? {
                continue;
            }

            let index = parts_to_read.len();
            let groups = self.read_block_context.granule_groups(&part, None)?;

            parts_to_read.push(part.clone());
            sources.push(groups.map(ParquetDataSource::Granule));

            if sources[index].is_none() {
                let filters = self.runtime_scan_filters.clone();
                let read_block_context = self.read_block_context.clone();

                full_reads.push(async move {
                    let read_part = part.clone();
                    let task = databend_common_base::runtime::spawn(async move {
                        read_block_context.read_full_data(read_part).await
                    });
                    let mut read = AbortOnDrop::new(task);

                    if filters.is_empty() {
                        let source = read.await.map_err(|error| {
                            ErrorCode::TokioError(format!("block read task failed: {error}"))
                        })??;
                        return Ok::<_, ErrorCode>((index, Some(source)));
                    }

                    loop {
                        // Subscribe before checking to avoid missing an update.
                        let rechecks = filters.recheck_notified();
                        // `select_all` panics on empty input.
                        debug_assert!(!rechecks.is_empty());
                        let part_info = FuseBlockPartInfo::from_part(&part)?;
                        if filters.should_prune(part_info.columns_stat.as_ref()) {
                            return Ok::<_, ErrorCode>((index, None));
                        }

                        tokio::select! {
                            result = &mut read => {
                                let source = result.map_err(|error| {
                                    ErrorCode::TokioError(format!("block read task failed: {error}"))
                                })??;
                                return Ok::<_, ErrorCode>((index, Some(source)));
                            }
                            _ = futures::future::select_all(rechecks) => {}
                        }
                    }
                });
            }
        }

        let completed_reads = futures::future::try_join_all(full_reads).await?;
        for (index, source) in completed_reads {
            sources[index] = source;
        }

        let mut retained_parts = Vec::with_capacity(parts_to_read.len());
        let mut retained_sources = Vec::with_capacity(sources.len());
        for (part, source) in parts_to_read.into_iter().zip(sources) {
            if let Some(source) = source {
                retained_parts.push(part);
                retained_sources.push(source);
            }
        }

        let meta = DataSourceWithMeta::create(retained_parts, retained_sources);
        Ok(DataBlock::empty_with_meta(meta))
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
        let parts = std::mem::take(&mut self.remaining_parts);
        self.async_output = Some(self.read_remaining_parts(parts).await?);
        Ok(())
    }
}
