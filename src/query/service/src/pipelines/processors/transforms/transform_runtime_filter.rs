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
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_pipeline_sinks::Sink;

use crate::pipelines::processors::transforms::runtime_filter::RuntimeFilterConnector;

// The sinker will contains runtime filter source
// It will be represented as `ChannelFilter<RuntimeFilterId, XorFilter>
pub struct SinkRuntimeFilterSource {
    connector: Arc<dyn RuntimeFilterConnector>,
}

impl SinkRuntimeFilterSource {
    pub fn new(connector: Arc<dyn RuntimeFilterConnector>) -> Self {
        connector.attach();
        Self { connector }
    }
}

impl Sink for SinkRuntimeFilterSource {
    const NAME: &'static str = "GenerateRuntimeFilter";

    fn on_finish(&mut self) -> Result<()> {
        self.connector.detach()
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.connector.collect(&data_block)
    }
}

enum RuntimeFilterStep {
    // Collect runtime filter
    Collect,
    // Consume runtime filter
    Consume,
}

pub struct TransformRuntimeFilter {
    input_data: Option<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    pub(crate) connector: Arc<dyn RuntimeFilterConnector>,
    step: RuntimeFilterStep,
}

impl TransformRuntimeFilter {
    pub fn create(
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
            step: RuntimeFilterStep::Collect,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilter {
    fn name(&self) -> String {
        "TransformRuntimeFilter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            RuntimeFilterStep::Collect => Ok(Event::Async),
            RuntimeFilterStep::Consume => {
                if self.output_port.is_finished() {
                    self.input_port.finish();
                    return Ok(Event::Finished);
                }
                if !self.output_port.can_push() {
                    self.input_port.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self.output_data_blocks.pop_front().unwrap();
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.has_data() {
                    let data = self.input_port.pull_data().unwrap()?;
                    self.input_data = Some(data);
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    self.output_port.finish();
                    return Ok(Event::Finished);
                }

                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            RuntimeFilterStep::Collect => Ok(()),
            RuntimeFilterStep::Consume => {
                if let Some(data) = self.input_data.take() {
                    let data = data.convert_to_full();
                    self.output_data_blocks
                        .extend(self.connector.consume(&data)?);
                }
                Ok(())
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let RuntimeFilterStep::Collect = &self.step {
            self.connector.wait_finish().await?;
            self.step = RuntimeFilterStep::Consume;
        }
        Ok(())
    }
}
