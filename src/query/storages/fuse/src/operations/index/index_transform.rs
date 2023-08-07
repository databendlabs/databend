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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use storages_common_table_meta::meta::BlockMeta;

pub struct RefreshAggIndexTransform {
    ctx: Arc<dyn TableContext>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    data_block: Option<DataBlock>,
}

impl RefreshAggIndexTransform {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(RefreshAggIndexTransform {
            ctx,
            input,
            output,
            data_block: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for RefreshAggIndexTransform {
    fn name(&self) -> String {
        "RefreshAggIndexTransform".to_string()
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
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        let input_meta = input_data.take_meta().unwrap();
        let block_meta = BlockMeta::downcast_ref_from(&input_meta)
            .ok_or(ErrorCode::Internal("No commit meta. It's a bug"))?
            .clone();
        dbg!(block_meta.location);

        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        Ok(())
    }
}
