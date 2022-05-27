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

use async_channel::Receiver;
use async_channel::Sender;
use common_base::base::tokio::io::AsyncReadExt;
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

pub struct ParallelMultipartWorker {
    multipart: Multipart,
    input_format: Box<dyn InputFormat>,
    tx: Option<Sender<Result<Box<dyn InputState>>>>,
}

impl ParallelMultipartWorker {
    pub fn create(
        multipart: Multipart,
        tx: Sender<Result<Box<dyn InputState>>>,
        input_format: Box<dyn InputFormat>,
    ) -> ParallelMultipartWorker {
        ParallelMultipartWorker {
            multipart,
            input_format,
            tx: Some(tx),
        }
    }

    async fn send(
        tx: &Sender<Result<Box<dyn InputState>>>,
        data: Result<Box<dyn InputState>>,
    ) -> bool {
        if let Err(cause) = tx.send(data).await {
            common_tracing::tracing::warn!("Multipart channel disconnect. {}", cause);
            return false;
        }

        true
    }
}

#[async_trait::async_trait]
impl MultipartWorker for ParallelMultipartWorker {
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
                        let mut skipped_header = false;
                        let filename = field.file_name().unwrap_or("Unknown file name").to_string();

                        let mut buf = vec![0; 1048576];
                        let mut state = self.input_format.create_state();
                        let mut async_reader = field.into_async_read();

                        'read: loop {
                            let read_res = async_reader.read(&mut buf[..]).await;

                            match read_res {
                                Ok(0) => {
                                    break 'read;
                                }
                                Ok(sz) => {
                                    let mut buf_slice: &[u8] = match sz == buf.len() {
                                        true => &buf,
                                        false => &buf[..sz],
                                    };

                                    if !skipped_header {
                                        let skip_size = match self
                                            .input_format
                                            .skip_header(buf_slice, &mut state)
                                        {
                                            Ok(skip_size) => skip_size,
                                            Err(cause) => {
                                                Self::send(&tx, Err(cause)).await;
                                                break 'outer;
                                            }
                                        };

                                        buf_slice = &buf_slice[skip_size..];

                                        if skip_size < buf_slice.len() {
                                            skipped_header = true;
                                            state = self.input_format.create_state();
                                        }
                                    }

                                    if !skipped_header {
                                        continue 'read;
                                    }

                                    while !buf_slice.is_empty() {
                                        let read_size =
                                            match self.input_format.read_buf(buf_slice, &mut state)
                                            {
                                                Ok(read_size) => read_size,
                                                Err(cause) => {
                                                    Self::send(&tx, Err(cause)).await;
                                                    break 'outer;
                                                }
                                            };

                                        if read_size < buf_slice.len() {
                                            let new_state = self.input_format.create_state();
                                            let prepared_state = replace(&mut state, new_state);

                                            if !Self::send(&tx, Ok(prepared_state)).await {
                                                break 'outer;
                                            }
                                        }

                                        buf_slice = &buf_slice[read_size..];
                                    }
                                }
                                Err(cause) => {
                                    if let Err(_cause) = tx
                                        .send(Err(ErrorCode::BadBytes(format!(
                                            "Read part to field bytes error, cause {:?}, filename: '{}'",
                                            cause,
                                            filename
                                        ))))
                                        .await
                                    {
                                        // common_tracing::tracing::warn!(
                                        //     "Multipart channel disconnect. {}, filename: '{}'",
                                        //     cause,
                                        //     filename
                                        // );
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
    Finished,
    NeedReceiveData,
    ReceivedData(Box<dyn InputState>),
}

pub struct ParallelInputFormatSource {
    state: State,
    output: Arc<OutputPort>,
    data_block: Vec<DataBlock>,
    scan_progress: Arc<Progress>,
    input_format: Box<dyn InputFormat>,
    data_receiver: Receiver<Result<Box<dyn InputState>>>,
}

impl ParallelInputFormatSource {
    pub fn create(
        output: Arc<OutputPort>,
        scan_progress: Arc<Progress>,
        input_format: Box<dyn InputFormat>,
        data_receiver: Receiver<Result<Box<dyn InputState>>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(ParallelInputFormatSource {
            output,
            input_format,
            data_receiver,
            scan_progress,
            data_block: vec![],
            state: State::NeedReceiveData,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParallelInputFormatSource {
    fn name(&self) -> &'static str {
        "ParallelInputFormatSource"
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

        if matches!(&self.state, State::Finished) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        match &self.state {
            State::Finished => Ok(Event::Finished),
            State::NeedReceiveData => Ok(Event::Async),
            State::ReceivedData(_data) => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> Result<()> {
        let mut progress_values = ProgressValues::default();
        if let State::ReceivedData(mut state) = replace(&mut self.state, State::NeedReceiveData) {
            let mut blocks = self.input_format.deserialize_data(&mut state)?;

            self.data_block.reserve(blocks.len());
            while let Some(block) = blocks.pop() {
                progress_values.rows += block.num_rows();
                progress_values.bytes += block.memory_size();
                self.data_block.push(block);
            }
        }

        self.scan_progress.incr(&progress_values);
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let State::NeedReceiveData = replace(&mut self.state, State::NeedReceiveData) {
            if let Ok(receive_res) = self.data_receiver.recv().await {
                self.state = State::ReceivedData(receive_res?);
                return Ok(());
            }
        }

        self.state = State::Finished;
        Ok(())
    }
}
