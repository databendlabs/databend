// Copyright 2023 Datafuse Labs.
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
//

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub use crate::operations::merge_into::mutator::replace_into_mutator::ReplaceIntoMutator;

// use std::marker::PhantomData;
// pub struct TypedPort<P, T> {
//    port: P,
//    _t: PhantomData<T>,
//}
// pub type TypedInputPort<T> = TypedPort<Arc<InputPort>, T>;
// pub type TypedOutputPort<T> = TypedPort<Arc<OutputPort>, T>;

/// - append data to table, by just sending data to the CompactTransform (CompactTrans)
/// - if any rows (might) need to be deleted, output the deletion operation log to down stream (deletion_log_output_port)
pub struct ReplaceIntoProcessor {
    replace_into_mutator: ReplaceIntoMutator,
    input_port: Arc<InputPort>,
    input_data: Option<DataBlock>,

    output_port_merge_into_action: Arc<OutputPort>,
    output_port_append_data: Arc<OutputPort>,
    output_data_merge_into_action: Option<DataBlock>,
    output_data_append: Option<DataBlock>,
}

impl ReplaceIntoProcessor {
    pub fn try_create() -> Result<Self> {
        todo!()
    }

    pub fn get_input_port(&self) -> &Arc<InputPort> {
        &self.input_port
    }

    pub fn get_merge_into_action_output_port(&self) -> &Arc<OutputPort> {
        &self.output_port_merge_into_action
    }

    pub fn get_append_data_output_port(&self) -> &Arc<OutputPort> {
        &self.output_port_append_data
    }

    pub fn try_output_port(port: &OutputPort, data: &mut Option<DataBlock>) -> OutputState {
        if port.is_finished() {
            return OutputState::AllSent;
        };

        return if port.can_push() {
            if let Some(data_block) = data.take() {
                port.push_data(Ok(data_block));
                OutputState::AllSent
            } else {
                OutputState::PartiallySent
            }
        } else {
            if data.is_some() {
                OutputState::PartiallySent
            } else {
                OutputState::AllSent
            }
        };
    }
    pub fn try_outputs(&mut self) -> OutputState {
        if !self.output_port_append_data.can_push()
            || !self.output_port_merge_into_action.can_push()
        {
            return OutputState::PartiallySent;
        }

        let data_output =
            Self::try_output_port(&self.output_port_append_data, &mut self.output_data_append);
        let action_output = Self::try_output_port(
            &self.output_port_merge_into_action,
            &mut self.output_data_merge_into_action,
        );

        match (data_output, action_output) {
            (OutputState::PartiallySent, _) => OutputState::PartiallySent,
            (_, OutputState::PartiallySent) => OutputState::PartiallySent,
            (OutputState::AllSent, OutputState::AllSent) => OutputState::AllSent,
        }
    }

    pub fn pending_output(&self) -> bool {
        self.output_data_append.is_some() || self.output_data_merge_into_action.is_some()
    }

    pub fn finish_outputs(&self) {
        self.output_port_append_data.finish();
        self.output_port_merge_into_action.finish();
    }

    pub fn all_outputs_finished(&self) -> bool {
        self.output_port_append_data.is_finished()
            && self.output_port_merge_into_action.is_finished()
    }
}

pub enum OutputState {
    // something need to be sent, but outputs are not available, all partially sent
    PartiallySent,
    // something need to be sent, and all sent
    AllSent,
}

#[async_trait::async_trait]
impl Processor for ReplaceIntoProcessor {
    fn name(&self) -> String {
        "ReplaceIntoTransform".to_owned()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
    fn event(&mut self) -> Result<Event> {
        if self.all_outputs_finished() {
            self.input_port.finish();
            return Ok(Event::Finished);
        }

        // TODO not sure about this
        if self.pending_output() {
            return match self.try_outputs() {
                OutputState::PartiallySent => {
                    self.input_port.set_not_need_data();
                    Ok(Event::NeedConsume)
                }
                OutputState::AllSent => Ok(Event::NeedConsume),
            };
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input_port.is_finished() {
            self.finish_outputs();
            return Ok(Event::Finished);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let merge_into_action = self.replace_into_mutator.process_input_block(&data_block)?;
            self.output_data_merge_into_action =
                Some(DataBlock::empty_with_meta(Box::new(merge_into_action)));
            self.output_data_append = Some(data_block);
            return Ok(());
        }

        Ok(())
    }
}
