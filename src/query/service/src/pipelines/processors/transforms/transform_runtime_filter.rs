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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_pipeline_sinks::Sink;
use common_pipeline_sinks::Sinker;

use crate::pipelines::processors::transforms::runtime_filter::RuntimeFilterConnector;

// The sinker will contains runtime filter source
// It will be represented as `ChannelFilter<RuntimeFilterId, XorFilter>
pub struct SinkRuntimeFilterSource {
    connector: Arc<dyn RuntimeFilterConnector>,
}

impl SinkRuntimeFilterSource {
    pub fn new(connector: Arc<dyn RuntimeFilterConnector>) -> Self {
        Self { connector }
    }
}

impl Sink for SinkRuntimeFilterSource {
    const NAME: &'static str = "GenerateRuntimeFilter";

    fn on_start(&mut self) -> common_exception::Result<()> {
        todo!()
    }

    fn on_finish(&mut self) -> common_exception::Result<()> {
        todo!()
    }

    fn interrupt(&self) {
        todo!()
    }

    fn consume(&mut self, data_block: DataBlock) -> common_exception::Result<()> {
        todo!()
    }
}

pub struct TransformRuntimeFilter {
    input_data: Option<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    connector: Arc<dyn RuntimeFilterConnector>,
}

impl TransformRuntimeFilter {
    pub fn new(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        connector: Arc<dyn RuntimeFilterConnector>,
    ) -> Box<dyn Processor> {
        Box::new(TransformRuntimeFilter {
            input_data: None,
            output_data_blocks: Default::default(),
            input_port,
            output_port,
            connector,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilter {
    fn name(&self) -> String {
        todo!()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        todo!()
    }

    fn event(&mut self) -> common_exception::Result<Event> {
        todo!()
    }

    fn interrupt(&self) {
        todo!()
    }

    fn process(&mut self) -> common_exception::Result<()> {
        todo!()
    }

    async fn async_process(&mut self) -> common_exception::Result<()> {
        todo!()
    }
}
