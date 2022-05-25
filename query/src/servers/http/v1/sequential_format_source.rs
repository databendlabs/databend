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

use std::mem::replace;
use std::sync::Arc;

use common_base::base::tokio::io::AsyncReadExt;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use poem::web::Multipart;

use crate::formats::InputFormat;
use crate::formats::InputState;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;
use crate::servers::http::v1::multipart_format::MultipartWorker;

pub struct SequentialMultipartWorker {
    multipart: Multipart,
    tx: Option<Sender<Result<Vec<u8>>>>,
}

impl SequentialMultipartWorker {
    pub fn create(multipart: Multipart, tx: Sender<Result<Vec<u8>>>) -> SequentialMultipartWorker {
        SequentialMultipartWorker {
            multipart,
            tx: Some(tx),
        }
    }
}

#[async_trait::async_trait]
impl MultipartWorker for SequentialMultipartWorker {
    async fn work(&mut self) {
        if let Some(tx) = self.tx.take() {
            'outer: loop {
                match self.multipart.next_field().await {
                    Err(cause) => {
                        if let Err(cause) = tx
                            .send(Err(ErrorCode::BadBytes(format!(
                                "Parse multipart error, cause {:?}",
                                cause
                            ))))
                            .await
                        {
                            common_tracing::tracing::warn!(
                                "Multipart channel disconnect. {}",
                                cause
                            );

                            break 'outer;
                        }
                    }
                    Ok(None) => {
                        break 'outer;
                    }
                    Ok(Some(field)) => {
                        let filename = field.file_name().unwrap_or("Unknown file name").to_string();

                        if let Err(cause) = tx.send(Ok(vec![])).await {
                            common_tracing::tracing::warn!(
                                "Multipart channel disconnect. {}, filename '{}'",
                                cause,
                                filename
                            );

                            break 'outer;
                        }

                        let mut async_reader = field.into_async_read();

                        'read: loop {
                            // 1048576 from clickhouse DBMS_DEFAULT_BUFFER_SIZE
                            let mut buf = vec![0; 1048576];
                            let read_res = async_reader.read(&mut buf[..]).await;

                            match read_res {
                                Ok(0) => {
                                    break 'read;
                                }
                                Ok(sz) => {
                                    if sz != buf.len() {
                                        buf.truncate(sz);
                                    }

                                    if let Err(cause) = tx.send(Ok(buf)).await {
                                        common_tracing::tracing::warn!(
                                            "Multipart channel disconnect. {}, filename: '{}'",
                                            cause,
                                            filename
                                        );

                                        break 'outer;
                                    }
                                }
                                Err(cause) => {
                                    if let Err(cause) = tx
                                        .send(Err(ErrorCode::BadBytes(format!(
                                            "Read part to field bytes error, cause {:?}, filename: '{}'",
                                            cause,
                                            filename
                                        ))))
                                        .await
                                    {
                                        common_tracing::tracing::warn!(
                                            "Multipart channel disconnect. {}, filename: '{}'",
                                            cause,
                                            filename
                                        );
                                        break 'outer;
                                    }

                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
        }
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
    skipped_header: bool,
    output: Arc<OutputPort>,
    data_block: Vec<DataBlock>,
    scan_progress: Arc<Progress>,
    input_state: Box<dyn InputState>,
    input_format: Box<dyn InputFormat>,
    data_receiver: Receiver<common_exception::Result<Vec<u8>>>,
}

impl SequentialInputFormatSource {
    pub fn create(
        output: Arc<OutputPort>,
        input_format: Box<dyn InputFormat>,
        data_receiver: Receiver<Result<Vec<u8>>>,
        scan_progress: Arc<Progress>,
    ) -> Result<ProcessorPtr> {
        let input_state = input_format.create_state();
        Ok(ProcessorPtr::create(Box::new(
            SequentialInputFormatSource {
                output,
                input_state,
                input_format,
                data_receiver,
                scan_progress,
                finished: false,
                state: State::NeedReceiveData,
                data_block: vec![],
                skipped_header: false,
            },
        )))
    }
}

#[async_trait::async_trait]
impl Processor for SequentialInputFormatSource {
    fn name(&self) -> &'static str {
        "SequentialInputFormatSource"
    }

    fn event(&mut self) -> common_exception::Result<Event> {
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

        if self.finished && !matches!(&self.state, State::NeedDeserialize) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        match &self.state {
            State::NeedReceiveData => Ok(Event::Async),
            State::ReceivedData(_data) => Ok(Event::Sync),
            State::NeedDeserialize => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> common_exception::Result<()> {
        let mut progress_values = ProgressValues::default();
        match replace(&mut self.state, State::NeedReceiveData) {
            State::ReceivedData(data) => {
                let mut data_slice: &[u8] = &data;
                progress_values.bytes += data.len();

                if !self.skipped_header {
                    let len = data_slice.len();
                    let skip_size = self
                        .input_format
                        .skip_header(data_slice, &mut self.input_state)?;

                    data_slice = &data_slice[skip_size..];

                    if skip_size < len {
                        self.skipped_header = true;
                        self.input_state = self.input_format.create_state();
                    }
                }

                while !data_slice.is_empty() {
                    let len = data_slice.len();
                    let read_size = self
                        .input_format
                        .read_buf(data_slice, &mut self.input_state)?;

                    data_slice = &data_slice[read_size..];

                    if read_size < len {
                        let state = &mut self.input_state;
                        let mut blocks = self.input_format.deserialize_data(state)?;

                        self.data_block.reserve(blocks.len());
                        while let Some(block) = blocks.pop() {
                            progress_values.rows += block.num_rows();
                            self.data_block.push(block);
                        }
                    }
                }
            }
            State::NeedDeserialize => {
                let state = &mut self.input_state;
                let mut blocks = self.input_format.deserialize_data(state)?;

                self.data_block.reserve(blocks.len());
                while let Some(block) = blocks.pop() {
                    progress_values.rows += block.num_rows();
                    self.data_block.push(block);
                }
            }
            _ => {
                return Err(ErrorCode::LogicalError(
                    "State failure in Multipart format.",
                ));
            }
        }

        self.scan_progress.incr(&progress_values);
        Ok(())
    }

    async fn async_process(&mut self) -> common_exception::Result<()> {
        if let State::NeedReceiveData = replace(&mut self.state, State::NeedReceiveData) {
            if let Some(receive_res) = self.data_receiver.recv().await {
                let receive_bytes = receive_res?;

                if !receive_bytes.is_empty() {
                    self.state = State::ReceivedData(receive_bytes);
                } else {
                    self.skipped_header = false;
                    self.state = State::NeedDeserialize;
                }

                return Ok(());
            }
        }

        self.finished = true;
        self.state = State::NeedDeserialize;
        Ok(())
    }
}
