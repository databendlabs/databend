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
use common_io::prelude::FormatSettings;
use futures::AsyncRead;
use futures_util::AsyncReadExt;
use opendal::io_util::CompressAlgorithm;
use opendal::io_util::DecompressDecoder;
use opendal::io_util::DecompressState;

#[derive(Copy, Clone)]
pub enum FileSplitterState {
    NeedData,
    ReceivedData(usize),
    NeedFlush,
    Finished,
}

// reader -> input_buf -> decoder -> format_state -> splits
pub struct FileSplitter {
    input_format: Arc<dyn InputFormat>,

    state: FileSplitterState,

    reader: Box<dyn AsyncRead + Send + Unpin>,
    input_buf: Vec<u8>,

    decoder: Option<DecompressDecoder>,
    decompress_buf: Vec<u8>,

    format_state: Box<dyn InputState>,
    rows_to_skip: u64,
}

impl FileSplitter {
    pub fn create<R: AsyncRead + Unpin + Send + 'static>(
        reader: R,
        input_format: Arc<dyn InputFormat>,
        format_settings: FormatSettings,
        compress_algorithm: Option<CompressAlgorithm>,
    ) -> FileSplitter {
        let decoder = compress_algorithm.map(DecompressDecoder::new);
        FileSplitter {
            state: FileSplitterState::NeedData,
            rows_to_skip: format_settings.skip_header,
            reader: Box::new(reader),
            input_buf: vec![0; 1024 * 1024],
            decompress_buf: vec![0; 1024 * 1024],
            decoder,
            input_format: input_format.clone(),
            format_state: input_format.create_state(),
        }
    }

    pub fn state(&self) -> FileSplitterState {
        self.state
    }

    fn split(&mut self, size: usize, output_splits: &mut VecDeque<Vec<u8>>) -> Result<()> {
        if self.decoder.is_some() {
            self.dec_and_split(size, output_splits)
        } else {
            Self::split_simple(
                &self.input_buf[..size],
                &self.input_format,
                output_splits,
                &mut self.format_state,
                &mut self.rows_to_skip,
            )
        }
    }

    fn split_simple(
        data: &[u8],
        input_format: &Arc<dyn InputFormat>,
        output_splits: &mut VecDeque<Vec<u8>>,
        format_state: &mut Box<dyn InputState>,
        rows_to_skip: &mut u64,
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut data_slice = data;

        if *rows_to_skip > 0 {
            let len = data_slice.len();
            let mut skip_size = 0;
            while *rows_to_skip > 0 {
                skip_size += input_format.skip_header(data_slice, format_state, 1)?;
                *rows_to_skip -= 1;
            }
            if skip_size < len {
                *format_state = input_format.create_state();
                data_slice = &data_slice[skip_size..];
            } else {
                return Ok(());
            }
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
                        &mut self.rows_to_skip,
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
                        &mut self.rows_to_skip,
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

    /// Fill will try its best to fill the `input_buf`
    ///
    /// Either the input_buf is full or the reader has been consumed.
    async fn fill(&mut self) -> Result<()> {
        let mut buf = &mut self.input_buf[0..];
        let mut n = 0;

        while !buf.is_empty() {
            let read = self.reader.read(buf).await?;
            if read == 0 {
                break;
            }

            n += read;
            buf = &mut self.input_buf[n..]
        }

        self.state = if n > 0 {
            FileSplitterState::ReceivedData(n)
        } else {
            FileSplitterState::NeedFlush
        };
        Ok(())
    }

    pub fn process(
        &mut self,
        output_splits: &mut VecDeque<Vec<u8>>,
        progress_values: &mut ProgressValues,
    ) -> Result<()> {
        match replace(&mut self.state, FileSplitterState::NeedData) {
            FileSplitterState::ReceivedData(size) => {
                self.split(size, output_splits)?;
                progress_values.bytes += size
            }
            FileSplitterState::NeedFlush => {
                self.flush(output_splits);
                self.state = FileSplitterState::Finished;
            }
            _ => return self.wrong_state(),
        }
        Ok(())
    }

    pub async fn async_process(&mut self) -> Result<()> {
        if let FileSplitterState::NeedData = replace(&mut self.state, FileSplitterState::NeedData) {
            self.fill().await
        } else {
            self.wrong_state()
        }
    }

    fn wrong_state(&self) -> Result<()> {
        Err(ErrorCode::LogicalError("State failure in FileSplitter."))
    }
}
