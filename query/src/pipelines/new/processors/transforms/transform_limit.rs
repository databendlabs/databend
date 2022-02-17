// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

pub struct TransformLimit;

impl TransformLimit {
    pub fn try_create(
        limit: Option<usize>,
        offset: usize,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        match (limit, offset) {
            (None, 0) => Err(ErrorCode::LogicalError("It's a bug")),
            (Some(_), 0) => OnlyLimitTransform::create(input, output, limit, offset),
            (None, _) => OnlyOffsetTransform::create(input, output, limit, offset),
            (Some(_), _) => OffsetAndLimitTransform::create(input, output, limit, offset),
        }
    }
}

const ONLY_LIMIT: usize = 0;
const ONLY_OFFSET: usize = 1;
const OFFSET_AND_LIMIT: usize = 2;

type OnlyLimitTransform = TransformLimitImpl<ONLY_LIMIT>;
type OnlyOffsetTransform = TransformLimitImpl<ONLY_OFFSET>;
type OffsetAndLimitTransform = TransformLimitImpl<OFFSET_AND_LIMIT>;

struct TransformLimitImpl<const MODE: usize> {
    take_remaining: usize,
    skip_remaining: usize,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data_block: Option<DataBlock>,
    output_data_block: Option<DataBlock>,
}

impl<const MODE: usize> TransformLimitImpl<MODE>
where Self: Processor
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            input_data_block: None,
            output_data_block: None,
            skip_remaining: offset,
            take_remaining: limit.unwrap_or(0),
        })))
    }

    pub fn take_rows(&mut self, data_block: DataBlock) -> DataBlock {
        let rows = data_block.num_rows();
        debug_assert_ne!(self.take_remaining, 0);

        if self.take_remaining >= rows {
            self.take_remaining -= rows;
            return data_block;
        }

        let remaining = self.take_remaining;
        self.take_remaining = 0;
        data_block.slice(0, remaining)
    }

    pub fn skip_rows(&mut self, data_block: DataBlock) -> Option<DataBlock> {
        let rows = data_block.num_rows();

        if self.skip_remaining >= rows {
            self.skip_remaining -= rows;
            return None;
        }

        let remaining = self.skip_remaining;
        self.skip_remaining = 0;
        Some(data_block.slice(remaining, rows - remaining))
    }
}

#[async_trait::async_trait]
impl<const MODE: usize> Processor for TransformLimitImpl<MODE> {
    fn name(&self) -> &'static str {
        match MODE {
            ONLY_LIMIT => "LimitTransform",
            ONLY_OFFSET => "OffsetTransform",
            OFFSET_AND_LIMIT => "OffsetAndLimitTransform",
            _ => unreachable!(),
        }
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_block.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.skip_remaining == 0 && self.take_remaining == 0 {
            if MODE == ONLY_LIMIT || MODE == OFFSET_AND_LIMIT {
                self.input.finish();
                self.output.finish();
                return Ok(Event::Finished);
            }

            if MODE == ONLY_OFFSET {
                if self.input.is_finished() {
                    self.output.finish();
                    return Ok(Event::Finished);
                }

                if !self.input.has_data() {
                    self.input.set_need_data();
                    return Ok(Event::NeedData);
                }

                self.output.push_data(self.input.pull_data().unwrap());
                return Ok(Event::NeedConsume);
            }
        }

        if self.input_data_block.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.input_data_block = Some(self.input.pull_data().unwrap()?);
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data_block.take() {
            self.output_data_block = match MODE {
                ONLY_OFFSET => self.skip_rows(data_block),
                ONLY_LIMIT => Some(self.take_rows(data_block)),
                OFFSET_AND_LIMIT if self.skip_remaining != 0 => self.skip_rows(data_block),
                OFFSET_AND_LIMIT => Some(self.take_rows(data_block)),
                _ => unreachable!(),
            }
        }

        Ok(())
    }
}
