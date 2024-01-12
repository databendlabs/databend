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
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_formats::output_format::OutputFormat;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use opendal::Operator;

use super::block_batch::BlockBatch;
use crate::append::output::DataSummary;
use crate::append::path::unload_path;
use crate::append::UnloadOutput;

pub struct ParquetFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    table_info: StageTableInfo,
    output_format: Box<dyn OutputFormat>,

    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,
    input_data: Option<DataBlock>,
    file_to_write: Option<(Vec<u8>, DataSummary)>,

    data_accessor: Operator,

    uuid: String,
    group_id: usize,
    batch_id: usize,
}

impl ParquetFileWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table_info: StageTableInfo,
        output_format: Box<dyn OutputFormat>,
        data_accessor: Operator,
        uuid: String,
        group_id: usize,
    ) -> Result<ProcessorPtr> {
        let unload_output =
            UnloadOutput::create(table_info.stage_info.copy_options.detailed_output);
        Ok(ProcessorPtr::create(Box::new(ParquetFileWriter {
            input,
            output,
            table_info,
            output_format,
            unload_output,
            unload_output_blocks: None,
            input_data: None,
            file_to_write: None,
            data_accessor,
            uuid,
            group_id,
            batch_id: 0,
        })))
    }
}

#[async_trait]
impl Processor for ParquetFileWriter {
    fn name(&self) -> String {
        "ParquetFileSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if self.file_to_write.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if self.input_data.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if self.unload_output.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            if self.unload_output_blocks.is_none() {
                self.unload_output_blocks = Some(self.unload_output.to_block_partial().into());
            }
            if self.output.can_push() {
                if let Some(block) = self.unload_output_blocks.as_mut().unwrap().pop_front() {
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                } else {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            } else {
                Ok(Event::NeedConsume)
            }
        } else if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        let block = self.input_data.take().unwrap();
        let block_meta = block.get_owned_meta().unwrap();
        let blocks = BlockBatch::downcast_from(block_meta).unwrap();
        let mut input_bytes = 0;
        let mut row_counts = 0;
        for b in blocks.blocks {
            input_bytes += b.memory_size();
            row_counts += b.num_rows();
            self.output_format.serialize_block(&b)?;
        }
        let data = self.output_format.finalize()?;
        let output_bytes = data.len();
        self.file_to_write = Some((data, DataSummary {
            row_counts,
            input_bytes,
            output_bytes,
        }));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(self.file_to_write.is_some());
        let path = unload_path(
            &self.table_info,
            &self.uuid,
            self.group_id,
            self.batch_id,
            None,
        );
        let (data, summary) = mem::take(&mut self.file_to_write).unwrap();
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
