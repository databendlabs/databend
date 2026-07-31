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

use databend_common_catalog::plan::PartInfoPtr;
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
        })))
    }

    fn create_runtime_pruners(&self) -> Result<ExprRuntimePruner> {
        let read_settings = self.read_block_context.read_settings();
        let inlist_bloom_prune_threshold =
            self.context
                .get_settings()
                .get_inlist_runtime_bloom_prune_threshold()? as usize;
        let runtime_filters = self.context.get_runtime_filters(self.scan_id);

        Ok(ExprRuntimePruner::new(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            self.block_reader.operator(),
            read_settings,
            inlist_bloom_prune_threshold,
            runtime_filters
                .iter()
                .flat_map(RuntimeFilterExpr::from_entry)
                .collect(),
        ))
    }

    fn classify_parts(&mut self, parts: Vec<PartInfoPtr>) -> Result<()> {
        let runtime_top_n_filters = self.context.get_runtime_top_n_filters(self.scan_id);
        for part in parts {
            if FuseBlockPartInfo::from_part(&part)?
                .should_prune_by_runtime_top_n(&runtime_top_n_filters)
            {
                continue;
            }
            if let Some(groups) = self.read_block_context.granule_groups_if_subset(&part)? {
                let source = ParquetDataSource::Granule(groups);
                let meta = DataSourceWithMeta::create(vec![part], vec![source]);
                self.pending_output
                    .push_back(DataBlock::empty_with_meta(meta));
            } else {
                self.remaining_parts.push(part);
            }
        }
        Ok(())
    }

    async fn read_remaining_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let mut parts_to_read = Vec::with_capacity(parts.len());
        let mut sources = Vec::with_capacity(parts.len());
        let mut full_reads = Vec::new();
        let expr_runtime_pruner = self.create_runtime_pruners()?;
        let runtime_top_n_filters = self.context.get_runtime_top_n_filters(self.scan_id);

        for part in parts {
            // The boundary may have tightened since this partition was first
            // classified, so check it again immediately before scheduling I/O.
            if FuseBlockPartInfo::from_part(&part)?
                .should_prune_by_runtime_top_n(&runtime_top_n_filters)
                || expr_runtime_pruner.prune(&part).await?
            {
                continue;
            }

            let index = parts_to_read.len();
            let groups = self.read_block_context.granule_groups(&part, None)?;
            parts_to_read.push(part.clone());
            sources.push(groups.map(ParquetDataSource::Granule));
            if sources[index].is_none() {
                let read_block_context = self.read_block_context.clone();
                full_reads.push(async move {
                    let source = databend_common_base::runtime::spawn(async move {
                        read_block_context.read_full_data(part).await
                    })
                    .await
                    .unwrap()?;
                    Result::<_>::Ok((index, source))
                });
            }
        }

        for (index, source) in futures::future::try_join_all(full_reads).await? {
            sources[index] = Some(source);
        }

        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            parts_to_read,
            sources
                .into_iter()
                .map(|source| source.expect("every retained part has a data source"))
                .collect(),
        )))
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
