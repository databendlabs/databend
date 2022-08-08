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
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::ipc::write::default_ipc_fields;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_arrow::arrow_format::flight::data::FlightData;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;

use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use common_exception::Result;

pub struct ExchangeTransformSink {
    output: Arc<OutputPort>,
    rx: Receiver<Result<FlightData>>,
    schema: DataSchemaRef,
    remote_flight_data: Option<FlightData>,
    remote_data_block: Option<DataBlock>,
}

impl ExchangeTransformSink {
    pub fn try_create(
        output: Arc<OutputPort>,
        rx: Receiver<Result<FlightData>>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(ExchangeTransformSink {
            rx,
            output,
            schema,
            remote_flight_data: None,
            remote_data_block: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeTransformSink {
    fn name(&self) -> &'static str {
        "ExchangeTransformSink"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.remote_data_block.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.remote_flight_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.rx.is_closed() && self.rx.is_empty() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(flight_data) = self.remote_flight_data.take() {
            let arrow_schema = Arc::new(self.schema.to_arrow());
            let ipc_fields = default_ipc_fields(&arrow_schema.fields);
            let ipc_schema = IpcSchema {
                fields: ipc_fields,
                is_little_endian: true,
            };

            let batch = deserialize_batch(
                &flight_data,
                &arrow_schema.fields,
                &ipc_schema,
                &Default::default(),
            )?;

            self.remote_data_block = Some(DataBlock::from_chunk(&self.schema, &batch)?);
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Ok(flight_data) = self.rx.recv().await {
            self.remote_flight_data = Some(flight_data?);
        }

        Ok(())
    }
}
