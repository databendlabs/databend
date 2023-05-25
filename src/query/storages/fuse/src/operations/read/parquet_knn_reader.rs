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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;
use crate::FuseTable;

pub struct ReadParquetKnnSource {
    is_finish: bool,
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    block_reader: Arc<BlockReader>,
    limit: usize,
    target: Vec<f32>,
}

impl ReadParquetKnnSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: Arc<dyn Table>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        limit: usize,
        target: Vec<f32>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Self {
            is_finish: false,
            ctx,
            table,
            output,
            output_data: None,
            block_reader,
            limit,
            target,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for ReadParquetKnnSource {
    fn name(&self) -> String {
        String::from("ReadParquetKnnSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finish {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.output_data.take() {
            self.output.push_data(Ok(data));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if self.is_finish {
            return Ok(());
        }
        let table = FuseTable::try_from_table(self.table.as_ref())?;
        self.output_data = table
            .read_by_vector_index(
                self.block_reader.clone(),
                self.ctx.clone(),
                self.limit,
                &self.target,
            )
            .await?;
        self.is_finish = true;
        Ok(())
    }
}
