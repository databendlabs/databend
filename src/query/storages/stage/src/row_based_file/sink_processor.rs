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
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::CompressCodec;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use opendal::Operator;

use crate::row_based_file::buffers::FileOutputBuffers;
use crate::stage_table::unload_path;

pub struct RowBasedFileSink {
    input: Arc<InputPort>,
    table_info: StageTableInfo,

    // always blocks for a whole file if not empty
    input_data: Option<DataBlock>,
    // always the data for a whole file if not empty
    output_data: Vec<u8>,

    data_accessor: Operator,
    prefix: Vec<u8>,

    uuid: String,
    group_id: usize,
    batch_id: usize,

    compression: Option<CompressAlgorithm>,
}

impl RowBasedFileSink {
    pub fn try_create(
        input: Arc<InputPort>,
        table_info: StageTableInfo,
        data_accessor: Operator,
        prefix: Vec<u8>,
        uuid: String,
        group_id: usize,
        compression: Option<CompressAlgorithm>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(RowBasedFileSink {
            table_info,
            input,
            input_data: None,
            data_accessor,
            prefix,
            uuid,
            group_id,
            batch_id: 0,
            output_data: vec![],
            compression,
        })))
    }
}

#[async_trait]
impl Processor for RowBasedFileSink {
    fn name(&self) -> String {
        "StageSink".to_string()
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
        let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
        let size = buffers.buffers.iter().map(|b| b.len()).sum::<usize>();
        let mut output = Vec::with_capacity(self.prefix.len() + size);
        output.extend_from_slice(self.prefix.as_slice());
        for b in buffers.buffers {
            output.extend_from_slice(b.as_slice());
        }
        if let Some(compression) = self.compression {
            output = CompressCodec::from(compression).compress_all(&output)?;
        }
        self.output_data = output;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let path = unload_path(
            &self.table_info,
            &self.uuid,
            self.group_id,
            self.batch_id,
            self.compression,
        );
        let data = mem::take(&mut self.output_data);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
