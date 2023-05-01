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

use common_expression::DataBlock;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::TransformDummy;

use crate::api::rpc::exchange::serde::exchange_deserializer::ExchangeDeserializeMeta;
use crate::api::rpc::flight_client::FlightReceiver;
use crate::api::DataPacket;

pub struct ExchangeSourceReader {
    finished: bool,
    output: Arc<OutputPort>,
    output_data: Vec<DataPacket>,
    flight_receiver: FlightReceiver,
}

impl ExchangeSourceReader {
    pub fn create(output: Arc<OutputPort>, flight_receiver: FlightReceiver) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(ExchangeSourceReader {
            output,
            flight_receiver,
            finished: false,
            output_data: vec![],
        }))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeSourceReader {
    fn name(&self) -> String {
        String::from("ExchangeSourceReader")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> common_exception::Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            if !self.finished {
                self.finished = true;
                self.flight_receiver.close();
            }

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if !self.output_data.is_empty() {
            let packets = std::mem::take(&mut self.output_data);
            let exchange_source_meta = ExchangeDeserializeMeta::create(packets);
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(exchange_source_meta)));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> common_exception::Result<()> {
        if self.output_data.is_empty() {
            let mut dictionaries = Vec::new();

            while let Some(output_data) = self.flight_receiver.recv().await? {
                if !matches!(&output_data, DataPacket::Dictionary(_)) {
                    dictionaries.push(output_data);
                    self.output_data = dictionaries;
                    return Ok(());
                }

                dictionaries.push(output_data);
            }

            assert!(dictionaries.is_empty());
        }

        if !self.finished {
            self.finished = true;
            self.flight_receiver.close();
        }

        Ok(())
    }
}

pub fn via_reader(prefix_size: usize, pipeline: &mut Pipeline, receivers: Vec<FlightReceiver>) {
    let mut items = Vec::with_capacity(prefix_size + receivers.len());

    for _index in 0..prefix_size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    for flight_exchange in receivers {
        let output = OutputPort::create();
        items.push(PipeItem::create(
            ExchangeSourceReader::create(output.clone(), flight_exchange),
            vec![],
            vec![output],
        ));
    }

    pipeline.add_pipe(Pipe::create(prefix_size, items.len(), items));
}

pub fn create_reader_item(flight_receiver: FlightReceiver) -> PipeItem {
    let output = OutputPort::create();
    PipeItem::create(
        ExchangeSourceReader::create(output.clone(), flight_receiver),
        vec![],
        vec![output],
    )
}
