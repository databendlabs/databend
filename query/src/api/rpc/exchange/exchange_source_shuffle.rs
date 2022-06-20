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

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

pub struct ExchangeShuffleSource {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    rx: Receiver<common_exception::Result<FlightData>>,
    schema: DataSchemaRef,
    remote_data_block: Option<DataBlock>,
    remote_flight_data: Option<FlightData>,
}

impl ExchangeShuffleSource {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        rx: Receiver<common_exception::Result<FlightData>>,
        schema: DataSchemaRef,
    ) -> common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(ExchangeShuffleSource {
            rx,
            input,
            output,
            schema,
            remote_data_block: None,
            remote_flight_data: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeShuffleSource {
    fn name(&self) -> &'static str {
        "ViaExchangeSubscriber"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> common_exception::Result<Event> {
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

        if let Some(data_block) = self.remote_data_block.take() {
            // println!("receive remote data block {:?}", data_block);
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            return match self.rx.try_recv() {
                Err(TryRecvError::Empty) => Ok(Event::Async),
                Err(TryRecvError::Closed) => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                Ok(flight_data) => {
                    self.remote_flight_data = Some(flight_data?);
                    Ok(Event::Sync)
                }
            };
        } else if let Ok(flight_data) = self.rx.try_recv() {
            self.remote_flight_data = Some(flight_data?);
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.output.push_data(self.input.pull_data().unwrap());
            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> common_exception::Result<()> {
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

    async fn async_process(&mut self) -> common_exception::Result<()> {
        if let Ok(flight_data) = self.rx.recv().await {
            self.remote_flight_data = Some(flight_data?);
        }

        Ok(())
    }
}
