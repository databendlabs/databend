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

use databend_common_exception::Result;

use crate::processors::Event;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;

pub struct DuplicateProcessor {
    input: Arc<InputPort>,
    output1: Arc<OutputPort>,
    output2: Arc<OutputPort>,

    /// Whether two outputs should finish together.
    force_finish_together: bool,
}

/// This processor duplicate the input data to two outputs.
impl DuplicateProcessor {
    pub fn create(
        input: Arc<InputPort>,
        output1: Arc<OutputPort>,
        output2: Arc<OutputPort>,
        force_finish_together: bool,
    ) -> Self {
        DuplicateProcessor {
            input,
            output1,
            output2,
            force_finish_together,
        }
    }
}

#[async_trait::async_trait]
impl Processor for DuplicateProcessor {
    fn name(&self) -> String {
        "Duplicate".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let is_finished1 = self.output1.is_finished();
        let is_finished2 = self.output2.is_finished();
        let one_finished = is_finished1 || is_finished2;
        let all_finished = is_finished1 && is_finished2;

        let can_push1 = self.output1.can_push();
        let can_push2 = self.output2.can_push();

        if all_finished || (self.force_finish_together && one_finished) {
            self.input.finish();
            self.output1.finish();
            self.output2.finish();
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            self.output1.finish();
            self.output2.finish();
            return Ok(Event::Finished);
        }

        if (!is_finished1 && !can_push1) || (!is_finished2 && !can_push2) {
            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        if self.input.has_data() {
            let block = self.input.pull_data().unwrap();
            if !is_finished1 {
                self.output1.push_data(block.clone());
            }
            if !is_finished2 {
                self.output2.push_data(block);
            }
            return Ok(Event::NeedConsume);
        }

        Ok(Event::NeedData)
    }
}
