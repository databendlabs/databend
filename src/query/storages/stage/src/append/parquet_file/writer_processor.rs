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
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use opendal::Operator;

use super::block_batch::BlockBatch;
use crate::append::path::unload_path;

pub struct ParquetFileWriter {
    input: Arc<InputPort>,
    table_info: StageTableInfo,
    output_format: Box<dyn OutputFormat>,

    input_data: Option<DataBlock>,
    output_data: Vec<u8>,

    data_accessor: Operator,

    uuid: String,
    group_id: usize,
    batch_id: usize,
}

impl ParquetFileWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        table_info: StageTableInfo,
        output_format: Box<dyn OutputFormat>,
        data_accessor: Operator,
        uuid: String,
        group_id: usize,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(ParquetFileWriter {
            input,
            table_info,
            output_format,
            input_data: None,
            output_data: vec![],
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
        if !self.output_data.is_empty() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if self.input_data.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            self.input.set_not_need_data();
            Ok(Event::Finished)
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
        for b in blocks.blocks {
            self.output_format.serialize_block(&b)?;
        }
        self.output_data = self.output_format.finalize()?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(!self.output_data.is_empty());
        let path = unload_path(
            &self.table_info,
            &self.uuid,
            self.group_id,
            self.batch_id,
            None,
        );
        let data = mem::take(&mut self.output_data);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
