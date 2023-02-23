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

use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

// one to many, broadcast, some kind of Shuffle?
pub struct BroadcastProcessor {}

impl BroadcastProcessor {
    pub fn get_input_port() -> Option<Arc<InputPort>> {
        todo!()
    }

    pub fn get_output_port(index: usize) -> Option<Arc<OutputPort>> {
        todo!()
    }
}

impl Processor for BroadcastProcessor {
    fn name(&self) -> String {
        todo!()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        todo!()
    }

    fn event(&mut self) -> common_exception::Result<Event> {
        todo!()
    }
}
