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
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::pipelines::processors::transforms::new_hash_join::join::Join;

enum Stage {
    Build,
    BuildFinal,
    Probe,
    ProbeFinal,
}

pub struct TransformHashJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,

    joined_port: Arc<OutputPort>,

    stage: Stage,
    join: Box<dyn Join>,
}

impl TransformHashJoin {
    pub fn create(
        build_port: Arc<InputPort>,
        probe_port: Arc<InputPort>,
        joined_port: Arc<OutputPort>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(TransformHashJoin {
            build_port,
            probe_port,
            joined_port,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoin {
    fn name(&self) -> String {
        String::from("TransformHashJoin")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.joined_port.is_finished() {
            self.build_port.finish();
            self.probe_port.finish();
            return Ok(Event::Finished);
        }

        if !self.joined_port.can_push() {
            match self.stage {
                Stage::Build => self.build_port.set_not_need_data(),
                Stage::Probe => self.probe_port.set_not_need_data(),
                Stage::BuildFinal | Stage::ProbeFinal => (),
            }

            return Ok(Event::NeedConsume);
        }

        if !self.build_port.is_finished() {
            // build stage
        }
    }

    fn process(&mut self) -> Result<()> {
        todo!()
    }

    async fn async_process(&mut self) -> Result<()> {
        todo!()
    }
}
