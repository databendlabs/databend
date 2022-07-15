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

use std::collections::VecDeque;
use std::mem::replace;
use std::sync::Arc;

use common_base::base::ProgressValues;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::InputFormat;
use common_formats::InputState;
use futures::AsyncRead;
use futures::AsyncReadExt;
use opendal::io_util::CompressAlgorithm;
use opendal::io_util::DecompressDecoder;
use opendal::io_util::DecompressState;

#[derive(Copy, Clone)]
pub enum State {
    NeedData,
    ReceivedData(usize),
    NeedFlush,
    Finished,
}

// reader -> input_buf -> decoder -> format_state -> splits
pub struct FileSplitter {
    input_format: Arc<dyn InputFormat>,

    state: State,

    reader: Box<dyn AsyncRead + Send + Unpin>,
    input_buf: Vec<u8>,

    decoder: Option<DecompressDecoder>,
    decompress_buf: Vec<u8>,

    format_state: Box<dyn InputState>,
    skipped_header: bool,
}

impl FileSplitter {
    pub fn create<R: AsyncRead + Unpin + Send + 'static>(
        reader: R,
        input_format: Arc<dyn InputFormat>,
        compress_algorithm: Option<CompressAlgorithm>,
    ) -> FileSplitter {
        let decoder = compress_algorithm.map(DecompressDecoder::new);
        FileSplitter {
            state: State::NeedData,
            skipped_header: false,
            reader: Box::new(reader),
            input_buf: vec![0; 1024 * 1024],
            decompress_buf: vec![0; 1024 * 1024],
            decoder,
            input_format: input_format.clone(),
            format_state: input_format.create_state(),
        }
    }

    pub fn state(&self) -> State {
        self.state
    }

    fn split(&mut self, size: usize, output_splits: &mut VecDeque<Vec<u8>>) -> Result<()> {
        //let data = &self.input_buf[..size];
        if self.decoder.is_some() {
            self.dec_and_split(size, output_splits)
        } else {
            Self::split_simple(
                &self.input_buf[..size],
                &self.input_format,
                output_splits,
                &mut self.format_state,
                &mut self.skipped_header,
            )
        }
    }

    fn split_simple(
        data: &[u8],
        input_format: &Arc<dyn InputFormat>,
        output_splits: &mut VecDeque<Vec<u8>>,
        format_state: &mut Box<dyn InputState>,
        skipped_header: &mut bool,
    ) -> Result<()> {
        let mut data_slice = data;

        if !*skipped_header {
            let len = data_slice.len();
            let skip_size = input_format.skip_header(data_slice, format_state)?;
            if skip_size < len {
                *skipped_header = true;
                *format_state = input_format.create_state();
            }
            data_slice = &data_slice[skip_size..];
        }

        while !data_slice.is_empty() {
            let len = data_slice.len();
            let read_size = input_format.read_buf(data_slice, format_state)?;

            data_slice = &data_slice[read_size..];

            if read_size < len {
                let buf = input_format.take_buf(format_state);
                output_splits.push_back(buf);
            }
        }
        Ok(())
    }

    fn dec_and_split(&mut self, size: usize, output_splits: &mut VecDeque<Vec<u8>>) -> Result<()> {
        let data = &self.input_buf[..size];
        let decoder = self.decoder.as_mut().unwrap();
        let mut amt = 0;

        loop {
            match decoder.state() {
                DecompressState::Reading => {
                    // If all data has been consumed, we should break with existing data directly.
                    if amt == data.len() {
                        break;
                    }

                    let read = decoder.fill(&data[amt..]);
                    amt += read;
                }
                DecompressState::Decoding => {
                    let written = decoder.decode(&mut self.decompress_buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    Self::split_simple(
                        &self.decompress_buf[..written],
                        &self.input_format,
                        output_splits,
                        &mut self.format_state,
                        &mut self.skipped_header,
                    )?;
                }
                DecompressState::Flushing => {
                    let written = decoder.finish(&mut self.decompress_buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    Self::split_simple(
                        &self.decompress_buf[..written],
                        &self.input_format,
                        output_splits,
                        &mut self.format_state,
                        &mut self.skipped_header,
                    )?;
                }
                DecompressState::Done => break,
            }
        }
        Ok(())
    }

    pub fn flush(&mut self, output_splits: &mut VecDeque<Vec<u8>>) {
        let state = &mut self.format_state;
        let buf = self.input_format.take_buf(state);
        if !buf.is_empty() {
            output_splits.push_back(buf);
        }
    }

    async fn fill(&mut self) -> Result<()> {
        let n_read = self.reader.read(&mut *self.input_buf).await?;
        if n_read > 0 {
            self.state = State::ReceivedData(n_read);
        } else {
            self.state = State::NeedFlush;
        }
        Ok(())
    }

    pub fn process(
        &mut self,
        output_splits: &mut VecDeque<Vec<u8>>,
        progress_values: &mut ProgressValues,
    ) -> Result<()> {
        match replace(&mut self.state, State::NeedData) {
            State::ReceivedData(size) => {
                self.split(size, output_splits)?;
                progress_values.bytes += size
            }
            State::NeedFlush => {
                self.flush(output_splits);
                self.state = State::Finished;
            }
            _ => return self.wrong_state(),
        }
        Ok(())
    }

    pub(crate) async fn async_process(&mut self) -> Result<()> {
        if let State::NeedData = replace(&mut self.state, State::NeedData) {
            self.fill().await
        } else {
            self.wrong_state()
        }
    }

    fn wrong_state(&self) -> Result<()> {
        Err(ErrorCode::LogicalError("State failure in FileSplitter."))
    }
}
