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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use opendal::Reader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use super::parquet_reader::ParquetRSReader;
use crate::ParquetPart;

pub struct ParquetSource {
    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    reader: Arc<ParquetRSReader>,
    stream: Option<ParquetRecordBatchStream<Reader>>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            ctx,
            output,
            reader,
            stream: None,
            generated_data: None,
            is_finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetRSSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.generated_data.take() {
            None => Ok(Event::Async),
            Some(data_block) => {
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            if let Some(block) = self.reader.read_block(&mut stream).await? {
                self.generated_data = Some(block);
                self.stream = Some(stream);
            }
            // else:
            // If `read_block` returns `None`, it means the stream is finished.
            // And we should try to build another stream (in next event loop).
        } else if let Some(part) = self.ctx.get_partition() {
            match ParquetPart::from_part(&part)? {
                ParquetPart::ParquetRSFile(file) => {
                    let stream = self
                        .reader
                        .prepare_data_stream(self.ctx.clone(), &file.location)
                        .await?;
                    self.stream = Some(stream);
                }
                _ => unreachable!(),
            }
        } else {
            self.is_finished = true;
        }

        Ok(())
    }
}
