use std::mem::replace;
use std::sync::Arc;
use poem::web::Multipart;
use common_base::tokio::io::AsyncReadExt;
use common_base::tokio::sync::mpsc::{Receiver, Sender};
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::{ErrorCode, Result};
use common_io::prelude::FormatSettings;
use crate::format::{FormatFactory, InputFormat, InputState};
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};


pub struct MultipartFormat;

impl MultipartFormat {
    pub fn input_sources(name: &str, mut multipart: Multipart, schema: DataSchemaRef, settings: FormatSettings, ports: Vec<Arc<OutputPort>>) -> Result<Vec<ProcessorPtr>> {
        let input_format = FormatFactory::instance().get_input(name, schema, settings)?;

        if ports.len() != 1 || input_format.support_parallel() {
            return Err(ErrorCode::UnImplement("Unimplemented parallel input format."));
        }

        let (tx, rx) = common_base::tokio::sync::mpsc::channel(2);

        common_base::tokio::spawn(async move {
            while let Ok(Some(field)) = multipart.next_field().await {
                let mut buf = vec![0; 2048];
                let mut async_reader = field.into_async_read();
                match async_reader.read(&mut buf[..]).await {
                    Ok(0) => { break; }
                    Ok(read_size) => {
                        if read_size != buf.len() {
                            buf = buf[0..read_size].to_vec();
                        }

                        if let Err(cause) = tx.send(Ok(buf)).await {
                            common_tracing::tracing::warn!("Multipart channel disconnect. {}", cause);
                        }
                    }
                    Err(cause) => {
                        if let Err(cause) = tx.send(Err(ErrorCode::BadBytes(format!("Read part to field bytes error, cause {:?}", cause)))).await {
                            common_tracing::tracing::warn!("Multipart channel disconnect. {}", cause);
                        }
                    }
                }
            }
        });

        Ok(vec![SequentialInputFormatSource::create(ports[0].clone(), input_format, rx)?])
    }
}

enum State {
    NeedReceiveData,
    ReceivedData(Vec<u8>),
    NeedDeserialize,
}

pub struct SequentialInputFormatSource {
    state: State,
    finished: bool,
    output: Arc<OutputPort>,
    data_block: Vec<DataBlock>,
    input_state: Box<dyn InputState>,
    input_format: Box<dyn InputFormat>,
    data_receiver: Receiver<Result<Vec<u8>>>,
}

impl SequentialInputFormatSource {
    pub fn create(output: Arc<OutputPort>, input_format: Box<dyn InputFormat>, data_receiver: Receiver<Result<Vec<u8>>>) -> Result<ProcessorPtr> {
        let input_state = input_format.create_state();
        Ok(ProcessorPtr::create(Box::new(SequentialInputFormatSource {
            output,
            input_state,
            input_format,
            data_receiver,
            finished: false,
            state: State::NeedReceiveData,
            data_block: vec![],
        })))
    }
}

#[async_trait::async_trait]
impl Processor for SequentialInputFormatSource {
    fn name(&self) -> &'static str {
        "SequentialInputFormatSource"
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.data_block.pop() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        match &self.state {
            State::NeedReceiveData => Ok(Event::Async),
            State::ReceivedData(_data) => Ok(Event::Sync),
            State::NeedDeserialize => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> Result<()> {
        match replace(&mut self.state, State::NeedReceiveData) {
            State::ReceivedData(data) => {
                let mut data_slice: &[u8] = &data;

                while !data_slice.is_empty() {
                    let len = data_slice.len();
                    let read_size = self.input_format.read_buf(data_slice, &mut self.input_state)?;

                    data_slice = &data_slice[read_size..];

                    if read_size < len {
                        self.data_block.push(self.input_format.deserialize_data(&mut self.input_state)?);
                    }
                }
            }
            State::NeedDeserialize => {
                self.data_block.push(self.input_format.deserialize_data(&mut self.input_state)?);
            }
            _ => {
                return Err(ErrorCode::LogicalError("State failure in Multipart format."));
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let State::NeedReceiveData = std::mem::replace(&mut self.state, State::NeedReceiveData) {
            if let Some(receive_res) = self.data_receiver.recv().await {
                self.state = State::ReceivedData(receive_res?);
                return Ok(());
            }
        }

        self.finished = true;
        self.state = State::NeedDeserialize;
        Ok(())
    }
}
