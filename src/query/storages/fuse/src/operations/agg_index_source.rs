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
use std::sync::Arc;

use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::UncompressedBuffer;
use crate::operations::read::fuse_source::fill_internal_column_meta;
use crate::FusePartInfo;

pub struct AggIndexSource {
    output: Arc<OutputPort>,
    parts: Vec<PartInfoPtr>,
    block_reader: Arc<BlockReader>,
    data_block: Option<DataBlock>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
    read_settings: ReadSettings,
    finished: bool,
}

impl AggIndexSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        parts: Vec<PartInfoPtr>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        let buffer_size = ctx.get_settings().get_parquet_uncompressed_buffer_size()? as usize;
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        Ok(ProcessorPtr::create(Box::new(AggIndexSource {
            output,
            parts,
            block_reader,
            data_block: None,
            uncompressed_buffer: UncompressedBuffer::new(buffer_size),
            read_settings,
            finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for AggIndexSource {
    fn name(&self) -> String {
        "AggBlockSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.data_block.take() {
            self.output.push_data(Ok(data_block));
        }

        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(part) = self.parts.pop() {
            let fuse_part = FusePartInfo::from_part(&part)?;

            let read_result = self
                .block_reader
                .read_columns_data_by_merge_io(
                    &self.read_settings,
                    &fuse_part.location,
                    &fuse_part.columns_meta,
                )
                .await?;

            let columns_chunks = read_result.columns_chunks()?;

            let data_block = self.block_reader.deserialize_parquet_chunks_with_buffer(
                &fuse_part.location,
                fuse_part.nums_rows,
                &fuse_part.compression,
                &fuse_part.columns_meta,
                columns_chunks,
                Some(self.uncompressed_buffer.clone()),
            )?;

            if self.block_reader.query_internal_columns() {
                let data_block = fill_internal_column_meta(data_block, fuse_part, None)?;
                self.data_block = Some(data_block);
            } else {
                self.data_block = Some(data_block);
            };
        } else {
            self.finished = true;
        }
        Ok(())
    }
}
