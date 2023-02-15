//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// read -> 然后read bitmap，filter之后，获取最新的，合并之后，更新mark。

use std::{sync::Arc, any::Any};

use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::{port::{InputPort, OutputPort}, Processor, processor::Event};

use crate::{MergeIOReadResult, io::{BlockReader, UncompressedBuffer}, operations::read::DataSourceMeta};

pub struct ParquetDeleteTransform {
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<MergeIOReadResult>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
}

#[async_trait::async_trait]
impl Processor for ParquetDeleteTransform {
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

        if let Some(data_block) = self.output_data.take() {
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
            if let Some(mut source_meta) = data_block.take_meta() {
                if let Some(source_meta) = source_meta.as_mut_any().downcast_mut::<DataSourceMeta>()
                {
                    self.parts = source_meta.part.clone();
                    self.chunks = std::mem::take(&mut source_meta.data);
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
        Ok(())
    }
}
