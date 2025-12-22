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

use crate::core::port::InputPort;
use crate::core::port::OutputPort;
use crate::core::processor::Event;
use crate::core::processor::Processor;

pub struct DuplicateProcessor {
    input: Arc<InputPort>,
    outputs: Vec<Arc<OutputPort>>,

    /// Whether all outputs should finish together.
    force_finish_together: bool,
}

/// This processor duplicate the input data to multiple outputs.
impl DuplicateProcessor {
    pub fn create(
        input: Arc<InputPort>,
        outputs: Vec<Arc<OutputPort>>,
        force_finish_together: bool,
    ) -> Self {
        DuplicateProcessor {
            input,
            outputs,
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
        let all_finished = self.outputs.iter().all(|x| x.is_finished());

        if all_finished
            || (self.force_finish_together && self.outputs.iter().any(|x| x.is_finished()))
        {
            self.input.finish();
            self.outputs.iter_mut().for_each(|x| x.finish());
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            self.outputs.iter_mut().for_each(|x| x.finish());
            return Ok(Event::Finished);
        }

        if self
            .outputs
            .iter()
            .any(|x| !x.is_finished() && !x.can_push())
        {
            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        if self.input.has_data() {
            let block = self.input.pull_data().ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(
                    "Failed to pull data from input port when data is available",
                )
            })?;
            for output in self.outputs.iter() {
                if !output.is_finished() {
                    output.push_data(block.clone());
                }
            }
            return Ok(Event::NeedConsume);
        }

        Ok(Event::NeedData)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use crate::basic::duplicate_processor::DuplicateProcessor;
    use crate::core::Event;
    use crate::core::InputPort;
    use crate::core::OutputPort;
    use crate::core::Processor;
    use crate::core::port::connect;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_duplicate_output_finish() -> databend_common_exception::Result<()> {
        {
            let input = InputPort::create();
            let output1 = OutputPort::create();
            let output2 = OutputPort::create();
            let mut processor = DuplicateProcessor::create(
                input.clone(),
                vec![output1.clone(), output2.clone()],
                false,
            );

            let upstream_output = OutputPort::create();
            let downstream_input1 = InputPort::create();
            let downstream_input2 = InputPort::create();

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input1, &output1);
                connect(&downstream_input2, &output2);
            }

            downstream_input1.set_need_data();
            downstream_input2.set_need_data();
            downstream_input1.finish();

            assert!(matches!(processor.event()?, Event::NeedData));

            downstream_input2.finish();
            assert!(matches!(processor.event()?, Event::Finished));
            assert!(input.is_finished());
        }

        {
            let input = InputPort::create();
            let output1 = OutputPort::create();
            let output2 = OutputPort::create();
            let mut processor = DuplicateProcessor::create(
                input.clone(),
                vec![output1.clone(), output2.clone()],
                true,
            );

            let upstream_output = OutputPort::create();
            let downstream_input1 = InputPort::create();
            let downstream_input2 = InputPort::create();

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input1, &output1);
                connect(&downstream_input2, &output2);
            }

            downstream_input1.finish();
            assert!(matches!(processor.event()?, Event::Finished));
            assert!(input.is_finished());
        }

        // One output finished, one output no finished and can push.
        {
            let input = InputPort::create();
            let output1 = OutputPort::create();
            let output2 = OutputPort::create();
            let mut processor = DuplicateProcessor::create(
                input.clone(),
                vec![output1.clone(), output2.clone()],
                false,
            );

            let upstream_output = OutputPort::create();
            let downstream_input1 = InputPort::create();
            let downstream_input2 = InputPort::create();

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input1, &output1);
                connect(&downstream_input2, &output2);
            }

            downstream_input1.finish();
            downstream_input2.set_need_data();
            assert!(matches!(processor.event()?, Event::NeedData));
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_duplicate_processor() -> databend_common_exception::Result<()> {
        let input = InputPort::create();
        let output1 = OutputPort::create();
        let output2 = OutputPort::create();
        let mut processor =
            DuplicateProcessor::create(input.clone(), vec![output1.clone(), output2.clone()], true);

        let upstream_output = OutputPort::create();
        let downstream_input1 = InputPort::create();
        let downstream_input2 = InputPort::create();

        unsafe {
            connect(&input, &upstream_output);
            connect(&downstream_input1, &output1);
            connect(&downstream_input2, &output2);
        }

        downstream_input1.set_need_data();
        downstream_input2.set_need_data();

        let col = Int32Type::from_data(vec![1, 2, 3]);
        let block = DataBlock::new_from_columns(vec![col.clone()]);
        upstream_output.push_data(Ok(block));
        assert!(matches!(processor.event()?, Event::NeedConsume));

        let out1 = downstream_input1.pull_data().unwrap()?;
        let out2 = downstream_input2.pull_data().unwrap()?;

        assert!(out1.columns()[0].as_column().unwrap().eq(&col));
        assert!(out2.columns()[0].as_column().unwrap().eq(&col));

        Ok(())
    }
}
