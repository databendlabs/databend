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
use std::time::Instant;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use super::fuse_source::fill_internal_column_meta;
use super::DataSource;
use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::UncompressedBuffer;
use crate::metrics::metrics_inc_remote_io_deserialize_milliseconds;
use crate::operations::read::parquet_data_source::DataSourceMeta;

pub struct DeserializeDataTransform {
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data_blocks: VecDeque<DataBlock>,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<DataSource>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
    block_size: usize,

    index_reader: Arc<Option<AggIndexReader>>,
}

unsafe impl Send for DeserializeDataTransform {}

impl DeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let buffer_size = settings.get_parquet_uncompressed_buffer_size()? as usize;
        let block_size = settings.get_max_block_size()? as usize;
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            scan_progress,
            block_reader,
            input,
            output,
            output_data_blocks: VecDeque::new(),
            parts: vec![],
            chunks: vec![],
            uncompressed_buffer: UncompressedBuffer::new(buffer_size),
            block_size,
            index_reader,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for DeserializeDataTransform {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            self.uncompressed_buffer.clear();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if !self.output_data_blocks.is_empty() {
            let data_block = self.output_data_blocks.pop_front().unwrap();
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(source_meta) = DataSourceMeta::downcast_from(source_meta) {
                    self.parts = source_meta.part;
                    self.chunks = source_meta.data;
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            self.uncompressed_buffer.clear();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, read_res)) = part.zip(chunks) {
            match read_res {
                DataSource::AggIndex(data) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let data_block = agg_index_reader.deserialize(&data)?;
                    if data_block.num_rows() > self.block_size {
                        let (sub_blocks, remain_block) = data_block.split_by_rows(self.block_size);
                        self.output_data_blocks.extend(sub_blocks);
                        if let Some(remain) = remain_block {
                            self.output_data_blocks.push_back(remain);
                        }
                    } else {
                        self.output_data_blocks.push_back(data_block);
                    }
                }
                DataSource::Normal(data) => {
                    let start = Instant::now();
                    let columns_chunks = data.columns_chunks()?;
                    let part = FusePartInfo::from_part(&part)?;

                    let data_block = self.block_reader.deserialize_parquet_chunks_with_buffer(
                        &part.location,
                        part.nums_rows,
                        &part.compression,
                        &part.columns_meta,
                        columns_chunks,
                        Some(self.uncompressed_buffer.clone()),
                    )?;

                    // Perf.
                    {
                        metrics_inc_remote_io_deserialize_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }

                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
                    // `FillInternalColumnProcessor` will generate internal columns using `BlockMetaIndex` in next pipeline.
                    let data_block = if self.block_reader.query_internal_columns() {
                        fill_internal_column_meta(data_block, part, None)?
                    } else {
                        data_block
                    };
                    if data_block.num_rows() > self.block_size {
                        let (sub_blocks, remain_block) = data_block.split_by_rows(self.block_size);
                        self.output_data_blocks.extend(sub_blocks);
                        if let Some(remain) = remain_block {
                            self.output_data_blocks.push_back(remain);
                        }
                    } else {
                        self.output_data_blocks.push_back(data_block);
                    }
                }
            }
        }

        Ok(())
    }
}
