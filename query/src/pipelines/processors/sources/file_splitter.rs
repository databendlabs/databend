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
use common_io::prelude::FileSplit;
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

trait Splitter {
    fn split(&mut self, data: &[u8], output_splits: &mut VecDeque<FileSplit>) -> Result<()>;
    fn flush(&mut self, output_splits: &mut VecDeque<FileSplit>);
}

// reader -> input_buf -> decoder -> format_state -> splits
pub struct FileSplitterCore {
    path: Option<String>,
    #[allow(unused)]
    rows: usize,
    offset: usize,
    start_row: usize,
    start_offset: usize,
    rows_to_skip: u64,

    input_format: Arc<dyn InputFormat>,
    format_state: Box<dyn InputState>,
}

pub struct FileSplitterCompressed {
    core: FileSplitterCore,
    decoder: DecompressDecoder,
    decompress_buf: Vec<u8>,
}

pub struct FileSplitter {
    inner: Splitters,
    state: FileSplitterState,
    input_buf: Vec<u8>,
    reader: Box<dyn AsyncRead + Send + Unpin>,
}

enum Splitters {
    Simple(FileSplitterCore),
    Compressed(Box<FileSplitterCompressed>),
}

impl Splitter for Splitters {
    fn split(&mut self, data: &[u8], output_splits: &mut VecDeque<FileSplit>) -> Result<()> {
        match self {
            Splitters::Simple(s) => s.split(data, output_splits),
            Splitters::Compressed(s) => s.split(data, output_splits),
        }
    }

    fn flush(&mut self, output_splits: &mut VecDeque<FileSplit>) {
        match self {
            Splitters::Simple(s) => s.flush(output_splits),
            Splitters::Compressed(s) => s.flush(output_splits),
        }
    }
}

impl Splitter for FileSplitterCore {
    fn split(&mut self, data: &[u8], output_splits: &mut VecDeque<FileSplit>) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut data_slice = data;
        if self.rows_to_skip > 0 {
            let skip_size = self.input_format.skip_header(
                data_slice,
                &mut self.format_state,
                self.rows_to_skip as usize,
            )?;
            let skip_rows = self.input_format.read_row_num(&mut self.format_state)?;

            if skip_rows == self.rows_to_skip as usize {
                self.rows_to_skip = 0;
                self.input_format.take_buf(&mut self.format_state);
                self.format_state = self.input_format.create_state();
                self.start_offset += skip_size;
                self.offset += skip_size;
                self.start_offset = self.offset;
                data_slice = &data_slice[skip_size..];
                self.start_row = skip_rows;
            } else {
                self.offset += data_slice.len();
                return Ok(());
            }
        }

        while !data_slice.is_empty() {
            let (read_size, is_full) = self
                .input_format
                .read_buf(data_slice, &mut self.format_state)?;
            self.offset += read_size;
            data_slice = &data_slice[read_size..];

            if is_full {
                let buf = self.input_format.take_buf(&mut self.format_state);
                let rows = self.input_format.read_row_num(&mut self.format_state)?;
                self.format_state = self.input_format.create_state();
                let split = FileSplit {
                    path: self.path.clone(),
                    start_offset: self.start_offset,
                    start_row: self.start_row,
                    buf,
                };
                self.start_row += rows;
                self.start_offset = self.offset;
                output_splits.push_back(split);
            }
        }
        Ok(())
    }

    fn flush(&mut self, output_splits: &mut VecDeque<FileSplit>) {
        let state = &mut self.format_state;
        let buf = self.input_format.take_buf(state);
        if !buf.is_empty() {
            let split = FileSplit {
                path: self.path.clone(),
                start_offset: self.start_offset,
                start_row: self.start_row,
                buf,
            };
            output_splits.push_back(split);
        }
    }
}

impl Splitter for FileSplitterCompressed {
    fn split(&mut self, data: &[u8], output_splits: &mut VecDeque<FileSplit>) -> Result<()> {
        let decoder = &mut self.decoder;
        let decompress_buf = &mut self.decompress_buf;
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
                    let written = decoder.decode(decompress_buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    self.core.split(&decompress_buf[..written], output_splits)?;
                }
                DecompressState::Flushing => {
                    let written = decoder.finish(decompress_buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    self.core.split(&decompress_buf[..written], output_splits)?;
                }
                DecompressState::Done => break,
            }
        }
        Ok(())
    }

    fn flush(&mut self, output_splits: &mut VecDeque<FileSplit>) {
        self.core.flush(output_splits)
    }
}

impl FileSplitter {
    pub fn create<R: AsyncRead + Unpin + Send + 'static>(
        reader: R,
        path: Option<String>,
        input_format: Arc<dyn InputFormat>,
        format_settings: FormatSettings,
        compress_algorithm: Option<CompressAlgorithm>,
    ) -> FileSplitter {
        let core = FileSplitterCore {
            path,
            rows: 0,
            offset: 0,
            start_row: 0,
            start_offset: 0,
            rows_to_skip: format_settings.skip_header,
            input_format: input_format.clone(),
            format_state: input_format.create_state(),
        };
        let inner = match compress_algorithm {
            None => Splitters::Simple(core),
            Some(alg) => Splitters::Compressed(Box::new(FileSplitterCompressed {
                core,
                decoder: DecompressDecoder::new(alg),
                decompress_buf: vec![0; format_settings.decompress_buffer_size],
            })),
        };
        FileSplitter {
            state: FileSplitterState::NeedData,
            inner,
            input_buf: vec![0; format_settings.input_buffer_size],
            reader: Box::new(reader),
        }
    }

    pub fn state(&self) -> FileSplitterState {
        self.state
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
        output_splits: &mut VecDeque<FileSplit>,
        progress_values: &mut ProgressValues,
    ) -> Result<()> {
        match replace(&mut self.state, FileSplitterState::NeedData) {
            FileSplitterState::ReceivedData(size) => {
                self.inner.split(&self.input_buf[0..size], output_splits)?;
                progress_values.bytes += size
            }
            FileSplitterState::NeedFlush => {
                self.inner.flush(output_splits);
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
