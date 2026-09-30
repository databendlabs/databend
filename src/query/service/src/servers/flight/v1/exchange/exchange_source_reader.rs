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
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::servers::flight::FlightReceiver;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::packets::DataPacket;

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

    fn close(&mut self) {
        if !std::mem::replace(&mut self.finished, true) {
            self.flight_receiver.close();
        }
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

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            self.close();
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

    // Blocking on `recv` is pointless once downstream finished; `event()` closes the receiver.
    fn cancel_async_on_outputs_finished(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
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

            // assert!(dictionaries.is_empty());
        }

        self.close();
        Ok(())
    }
}

pub fn create_reader_item(flight_receiver: FlightReceiver) -> PipeItem {
    let output = OutputPort::create();
    PipeItem::create(
        ExchangeSourceReader::create(output.clone(), flight_receiver),
        vec![],
        vec![output],
    )
}
