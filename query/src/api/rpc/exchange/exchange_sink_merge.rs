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

use crate::api::rpc::exchange::exchange_params::{ExchangeParams, MergeExchangeParams};
use crate::api::rpc::exchange::exchange_params::SerializeParams;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::packets::FragmentData;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct ExchangeMergeSink {
    ctx: Arc<QueryContext>,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataPacket>,
    serialize_params: SerializeParams,
    exchange_params: MergeExchangeParams,
    flight_exchange: FlightExchange,
}

impl ExchangeMergeSink {
    pub fn try_create(ctx: Arc<QueryContext>, input: Arc<InputPort>, exchange_params: &MergeExchangeParams) -> Result<ProcessorPtr> {
        let params = ExchangeParams::MergeExchange(exchange_params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let mut flight_exchange = exchange_manager.get_flight_exchanges(&params)?;
        assert_eq!(flight_exchange.len(), 1);

        Ok(ProcessorPtr::create(Box::new(ExchangeMergeSink {
            ctx,
            input,
            input_data: None,
            output_data: None,
            exchange_params: exchange_params.clone(),
            flight_exchange: flight_exchange.remove(0),
            serialize_params: exchange_params.create_serialize_params()?,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeMergeSink {
    fn name(&self) -> &'static str {
        "ExchangeMergeSink"
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
            self.flight_exchange.close_response();
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
            if data_block.is_empty() {
                return Ok(());
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

            // FlightData
            let data = FragmentData::create(values);
            self.output_data = Some(DataPacket::FragmentData(data));
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(output_data) = self.output_data.take() {
            if self.flight_exchange.send(output_data).await.is_err() {
                return Err(ErrorCode::TokioError(
                    "Cannot send flight data to endpoint, because sender is closed.",
                ));
            }
        }

        Ok(())
    }
}
