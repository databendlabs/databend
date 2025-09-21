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

use databend_common_pipeline_core::processors::{Event, InputPort, Processor};
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_exception::Result;

pub struct TransformHashJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,

    joined_port: Arc<OutputPort>,
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

impl Processor for TransformHashJoin {
    fn name(&self) -> String {
        String::from("TransformHashJoin")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }
}
