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

//! Zip matching branch contributions with bounded per-block backpressure.

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use crate::operations::table_index::meta::BuiltIndexes;

/// Zip one result from each branch before accepting another block. Together with
/// Duplicate's backpressure this bounds both shared inputs and pending artifacts.
pub struct JoinIndexes {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    pending: Option<DataBlock>,
}

impl JoinIndexes {
    pub fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>) -> Self {
        Self {
            inputs,
            output,
            pending: None,
        }
    }
}

#[async_trait::async_trait]
impl Processor for JoinIndexes {
    fn name(&self) -> String {
        "JoinRefreshIndexes".into()
    }
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            for input in &self.inputs {
                input.set_not_need_data();
            }
            return Ok(Event::NeedConsume);
        }
        if let Some(block) = self.pending.take() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }
        let available = self.inputs.iter().filter(|input| input.has_data()).count();
        if available == self.inputs.len() {
            return Ok(Event::Sync);
        }
        let finished = self
            .inputs
            .iter()
            .filter(|input| input.is_finished() && !input.has_data())
            .count();
        if finished == self.inputs.len() {
            self.output.finish();
            return Ok(Event::Finished);
        }
        if finished != 0 && available != 0 {
            return Err(ErrorCode::Internal(
                "Refresh index streams have different lengths",
            ));
        }
        for input in &self.inputs {
            if input.has_data() {
                input.set_not_need_data();
            } else if !input.is_finished() {
                input.set_need_data();
            }
        }
        Ok(Event::NeedData)
    }
    fn process(&mut self) -> Result<()> {
        let mut merged: Option<BuiltIndexes> = None;
        for input in &self.inputs {
            let block = input
                .pull_data()
                .ok_or_else(|| ErrorCode::Internal("Missing refresh index output"))??;
            let result = block
                .get_owned_meta()
                .and_then(BuiltIndexes::downcast_from)
                .ok_or_else(|| ErrorCode::Internal("Missing built refresh indexes"))?;
            if let Some(merged) = &mut merged {
                if !Arc::ptr_eq(&merged.plan, &result.plan) {
                    return Err(ErrorCode::Internal("Refresh index streams are not aligned"));
                }
                merged.artifacts.extend(result.artifacts);
            } else {
                merged = Some(result);
            }
        }
        self.pending = Some(DataBlock::empty_with_meta(Box::new(
            merged.ok_or_else(|| ErrorCode::Internal("No refresh index inputs"))?,
        )));
        Ok(())
    }
}
