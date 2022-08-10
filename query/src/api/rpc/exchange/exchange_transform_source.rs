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

use async_channel::Receiver;
use async_channel::TryRecvError;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::ipc::write::default_ipc_fields;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_arrow::arrow_format::flight::data::FlightData;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use common_exception::{ErrorCode, Result};
use crate::api::{DataPacket, FragmentData};
use crate::api::rpc::exchange::exchange_params::{ExchangeParams, MergeExchangeParams};
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::{PrecommitBlock, ProgressInfo};
use crate::sessions::QueryContext;

pub struct ExchangeSourceTransform {
    finished: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    remote_flight_data: Option<DataPacket>,
    flight_exchanges: Vec<FlightExchange>,
    exchange_params: MergeExchangeParams,
}

impl ExchangeSourceTransform {
    pub fn try_create(
        ctx: &QueryContext,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &MergeExchangeParams,
    ) -> Result<ProcessorPtr> {
        let exchange_params = ExchangeParams::MergeExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

        Ok(ProcessorPtr::create(Box::new(ExchangeSourceTransform {
            finished: false,
            input,
            output,
            flight_exchanges,
            output_data: None,
            remote_flight_data: None,
            exchange_params: params.clone(),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeSourceTransform {
    fn name(&self) -> &'static str {
        "ExchangeSourceTransform"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.remote_flight_data.is_some() {
            return Ok(Event::Sync);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            if self.finished {
                self.output.finish();
                return Ok(Event::Finished);
            }

            return Ok(Event::Async);
        }

        for flight_exchange in &self.flight_exchanges {
            if let Some(remote_flight_data) = flight_exchange.try_recv()? {
                self.remote_flight_data = Some(remote_flight_data);
                return Ok(Event::Sync);
            }
        }

        if self.input.has_data() {
            self.output.push_data(Ok(self.input.pull_data().unwrap()?));
            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        // Processing data received from other nodes
        if let Some(remote_data) = self.remote_flight_data.take() {
            return match remote_data {
                DataPacket::ErrorCode(v) => self.on_recv_error(v),
                DataPacket::Progress(v) => self.on_recv_progress(v),
                DataPacket::FragmentData(v) => self.on_recv_data(v),
                DataPacket::PrecommitBlock(v) => self.on_recv_precommit(v),
                DataPacket::FinishQuery => self.on_finish(),
            };
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        // async recv if input is finished.
        // TODO: use future::future::select_all to parallel await
        for flight_exchange in &self.flight_exchanges {
            if let Some(data_packet) = flight_exchange.recv().await? {
                self.remote_flight_data = Some(data_packet);
                return Ok(());
            }
        }

        self.finished = true;
        Ok(())
    }
}

impl ExchangeSourceTransform {
    fn on_recv_error(&mut self, cause: ErrorCode) -> Result<()> {
        Err(cause)
    }

    fn on_recv_data(&mut self, fragment_data: FragmentData) -> Result<()> {
        let schema = &self.exchange_params.schema;
        let arrow_schema = Arc::new(schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        let batch = deserialize_batch(
            &fragment_data.data,
            &arrow_schema.fields,
            &ipc_schema,
            &Default::default(),
        )?;

        self.output_data = Some(DataBlock::from_chunk(schema, &batch)?);

        Ok(())
    }

    fn on_recv_progress(&mut self, progress: ProgressInfo) -> Result<()> {
        unimplemented!()
    }

    fn on_recv_precommit(&mut self, fragment_data: PrecommitBlock) -> Result<()> {
        unimplemented!()
    }

    fn on_finish(&mut self) -> Result<()> {
        unimplemented!()
    }
}
