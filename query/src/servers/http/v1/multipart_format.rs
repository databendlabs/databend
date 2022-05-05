use std::mem::replace;
use std::sync::Arc;
use poem::web::Multipart;
use sqlparser::ast::ShowCreateObject::Event;
use common_base::tokio::io::AsyncReadExt;
use common_base::tokio::sync::mpsc::{Receiver, Sender};
use common_datablocks::DataBlock;
use common_exception::{ErrorCode, Result};
use crate::format::{FormatFactory, InputFormat, InputState};
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};


pub struct MultipartFormat;

impl MultipartFormat {
    pub fn input_sources(name: &str, mut multipart: Multipart, ports: Vec<Arc<OutputPort>>) -> Result<Vec<ProcessorPtr>> {
        let input_format = FormatFactory::instance().get_input(name)?;

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
                        if let Err(cause) = tx.send(Err(ErrorCode::BadBytes(format!("Read part to field bytes error, cause {:?}", cause)))) {
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
    data_block: Option<DataBlock>,
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
            data_block: None,
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

        if let Some(data_block) = self.data_block.take() {
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
        if let State::ReceivedData(data) = replace(&mut self.state, State::NeedReceiveData) {
            let read_size = self.input_format.read_buf(&data, &mut self.input_state)?;

            if read_size < data.len() {
                self.state = State::NeedDeserialize;
            }
        }

        if let State::NeedDeserialize = replace(&mut self.state, State::NeedReceiveData) {
            self.data_block = Some(self.input_format.deserialize_data(&mut self.input_state)?);
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
