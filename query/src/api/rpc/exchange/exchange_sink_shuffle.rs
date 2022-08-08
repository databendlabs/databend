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
use std::sync::Arc;

use async_channel::TrySendError;
use common_arrow::arrow::io::flight::serialize_batch;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::api::rpc::exchange::exchange_params::{ExchangeParams, SerializeParams};
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::packets::FragmentData;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

struct OutputData {
    pub has_serialized_data: bool,
    pub serialized_blocks: Vec<Option<DataPacket>>,
}

pub struct ExchangePublisherSink {
    ctx: Arc<QueryContext>,

    serialize_params: SerializeParams,
    shuffle_exchange_params: ShuffleExchangeParams,

    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<OutputData>,
    flight_exchanges: Vec<FlightExchange>,
}

impl ExchangePublisherSink {
    pub fn try_create(ctx: Arc<QueryContext>, input: Arc<InputPort>, params: &ShuffleExchangeParams) -> Result<ProcessorPtr> {
        let exchange_params = ExchangeParams::ShuffleExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

        Ok(ProcessorPtr::create(Box::new(
            ExchangePublisherSink {
                ctx,
                input,
                flight_exchanges,
                input_data: None,
                output_data: None,
                shuffle_exchange_params: params.clone(),
                serialize_params: params.create_serialize_params()?,
            }
        )))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangePublisherSink {
    fn name(&self) -> &'static str {
        "ExchangeShuffleSink"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            // No more data to sent.
            for flight_exchange in &self.flight_exchanges {
                flight_exchange.close_response();
            }

            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let scatter = &self.shuffle_exchange_params.shuffle_scatter;

            let scatted_blocks = scatter.execute(&data_block, 0)?;
            let mut output_data = OutputData {
                has_serialized_data: false,
                serialized_blocks: vec![],
            };

            for data_block in scatted_blocks.into_iter() {
                if data_block.is_empty() {
                    output_data.serialized_blocks.push(None);
                    continue;
                }

                let chunks = data_block.try_into()?;
                let options = &self.serialize_params.options;
                let ipc_fields = &self.serialize_params.ipc_fields;
                let (dicts, values) = serialize_batch(&chunks, ipc_fields, options)?;

                if !dicts.is_empty() {
                    return Err(ErrorCode::UnImplement(
                        "DatabendQuery does not implement dicts.",
                    ));
                }

                output_data.has_serialized_data = true;
                let data = FragmentData::create(values);
                output_data.serialized_blocks.push(Some(DataPacket::FragmentData(data)));
            }

            if output_data.has_serialized_data {
                self.output_data = Some(output_data);
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut output_data) = self.output_data.take() {
            for index in 0..output_data.serialized_blocks.len() {
                if let Some(output_packet) = output_data.serialized_blocks[index].take() {
                    let flight_exchange = &self.flight_exchanges[index];

                    if flight_exchange.send(output_packet).await.is_err() {
                        return Err(ErrorCode::TokioError(
                            "Cannot send flight data to endpoint, because sender is closed.",
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}
