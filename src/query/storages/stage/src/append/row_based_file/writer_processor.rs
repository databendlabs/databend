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
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::CompressCodec;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use super::buffers::FileOutputBuffers;
use crate::append::UnloadOutput;
use crate::append::output::DataSummary;
use crate::append::path::unload_path;

pub struct RowBasedFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    info: CopyIntoLocationInfo,

    // always blocks for a whole file if not empty
    input_data: Option<DataBlock>,
    // always the data for a whole file if not empty
    file_to_write: Option<(Vec<u8>, DataSummary, Option<Arc<str>>)>,

    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    data_accessor: Operator,
    prefix: Vec<u8>,

    query_id: String,
    group_id: usize,
    batch_id: usize,

    compression: Option<CompressAlgorithm>,
    suffix: String,
}

impl RowBasedFileWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        data_accessor: Operator,
        prefix: Vec<u8>,
        query_id: String,
        group_id: usize,
        compression: Option<CompressAlgorithm>,
        suffix: &str,
    ) -> Result<ProcessorPtr> {
        let unload_output = UnloadOutput::create(info.options.detailed_output);
        Ok(ProcessorPtr::create(Box::new(RowBasedFileWriter {
            info,
            input,
            input_data: None,
            data_accessor,
            prefix,
            query_id,
            group_id,
            batch_id: 0,
            file_to_write: None,
            compression,
            suffix: suffix.to_string(),
            output,
            unload_output,
            unload_output_blocks: None,
        })))
    }
}

#[async_trait]
impl Processor for RowBasedFileWriter {
    fn name(&self) -> String {
        "RowBasedFileWriter".to_string()
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
        let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
        let partition = buffers.partition.clone();
        let size = buffers
            .buffers
            .iter()
            .map(|b| b.buffer.len())
            .sum::<usize>();
        let row_counts = buffers.buffers.iter().map(|b| b.row_counts).sum::<usize>();
        let mut output = Vec::with_capacity(self.prefix.len() + size);
        output.extend_from_slice(self.prefix.as_slice());
        for b in buffers.buffers {
            output.extend_from_slice(b.buffer.as_slice());
        }
        let input_bytes = output.len();
        if let Some(compression) = self.compression {
            output = if compression == CompressAlgorithm::Zip {
                let name = format!("unload_{}{}", self.batch_id, self.suffix);
                CompressCodec::compress_all_zip(&output, name.as_str())?
            } else {
                CompressCodec::from(compression).compress_all(&output)?
            };
        }
        let output_bytes = output.len();
        let summary = DataSummary {
            row_counts,
            input_bytes,
            output_bytes,
        };
        self.file_to_write = Some((output, summary, partition));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let (data, summary, partition) = mem::take(&mut self.file_to_write).unwrap();
        let path = unload_path(
            &self.info,
            &self.query_id,
            self.group_id,
            self.batch_id,
            self.compression,
            partition.as_deref(),
        );
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
