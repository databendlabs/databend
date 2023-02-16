//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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

use crate::api::rpc::exchange::serde::exchange_deserializer::ExchangeDeserializeMeta;
use crate::api::rpc::flight_client::FlightExchangeRef;
use crate::api::DataPacket;
use crate::pipelines::processors::TransformDummy;

pub struct ExchangeSourceReader {
    finished: bool,
    initialized: bool,
    output: Arc<OutputPort>,
    output_data: Option<DataPacket>,
    flight_exchange: FlightExchangeRef,
}

impl ExchangeSourceReader {
    pub fn create(output: Arc<OutputPort>, flight_exchange: FlightExchangeRef) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(ExchangeSourceReader {
            output,
            flight_exchange,
            finished: false,
            output_data: None,
            initialized: false,
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
                return Ok(Event::Async);
            }

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_packet) = self.output_data.take() {
            let exchange_source_meta = ExchangeDeserializeMeta::create(data_packet);
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(exchange_source_meta)));
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> common_exception::Result<()> {
        if !self.initialized {
            self.initialized = true;
            self.flight_exchange.close_output().await;
        }

        if self.output_data.is_none() {
            if let Some(output_data) = self.flight_exchange.recv().await? {
                self.output_data = Some(output_data);
                return Ok(());
            }
        }

        if !self.finished {
            self.finished = true;
            self.flight_exchange.close_input().await;
        }

        Ok(())
    }
}

pub fn via_reader(prefix_size: usize, exchanges: Vec<FlightExchangeRef>, pipeline: &mut Pipeline) {
    let mut items = Vec::with_capacity(prefix_size + exchanges.len());

    for _index in 0..prefix_size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    for flight_exchange in exchanges {
        let output = OutputPort::create();
        items.push(PipeItem::create(
            ExchangeSourceReader::create(output.clone(), flight_exchange),
            vec![],
            vec![output],
        ));
    }

    pipeline.add_pipe(Pipe::create(prefix_size, items.len(), items));
}

pub fn create_reader_item(exchange: FlightExchangeRef) -> PipeItem {
    let output = OutputPort::create();
    PipeItem::create(
        ExchangeSourceReader::create(output.clone(), exchange),
        vec![],
        vec![output],
    )
}
