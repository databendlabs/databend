//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use common_base::base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::StageFileFormatType;
use common_pipeline_core::Pipeline;
use opendal::io_util::DecompressDecoder;
use opendal::io_util::DecompressState;
use opendal::Object;

use super::InputFormat;
use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::impls::input_format_csv::CsvReaderState;
use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_format::FileInfo;
use crate::processors::sources::input_formats::input_format::InputData;
use crate::processors::sources::input_formats::input_format::SplitInfo;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_pipeline::StreamingReadBatch;

pub trait InputFormatTextBase: Sized + Send + Sync + 'static {
    fn format_type() -> StageFileFormatType;

    fn is_splittable() -> bool {
        false
    }

    fn default_record_delimiter() -> RecordDelimiter {
        RecordDelimiter::Crlf
    }

    fn default_field_delimiter() -> u8;

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()>;

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>>;
}

pub struct InputFormatText<T: InputFormatTextBase> {
    phantom: PhantomData<T>,
}

impl<T: InputFormatTextBase> InputFormatText<T> {
    pub fn create() -> Self {
        Self {
            phantom: Default::default(),
        }
    }
}

pub struct InputFormatTextPipe<T> {
    phantom: PhantomData<T>,
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> InputFormatPipe for InputFormatTextPipe<T> {
    type ReadBatch = Vec<u8>;
    type RowBatch = RowBatch;
    type AligningState = AligningState<T>;
    type BlockBuilder = BlockBuilder<T>;
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> InputFormat for InputFormatText<T> {
    fn default_record_delimiter(&self) -> RecordDelimiter {
        T::default_record_delimiter()
    }

    fn default_field_delimiter(&self) -> u8 {
        T::default_field_delimiter()
    }

    async fn read_file_meta(
        &self,
        _obj: &Object,
        _size: usize,
    ) -> Result<Option<Arc<dyn InputData>>> {
        Ok(None)
    }

    async fn read_split_meta(
        &self,
        _obj: &Object,
        _split_info: &SplitInfo,
    ) -> Result<Option<Box<dyn InputData>>> {
        Ok(None)
    }

    fn split_files(&self, file_infos: Vec<FileInfo>, split_size: usize) -> Vec<SplitInfo> {
        let mut splits = vec![];
        for f in file_infos {
            if f.compress_alg.is_none() || !T::is_splittable() {
                splits.push(SplitInfo::from_file_info(f))
            } else {
                splits.append(&mut f.split_by_size(split_size))
            }
        }
        splits
    }

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        tracing::info!("exe text");
        InputFormatTextPipe::<T>::execute_copy_with_aligner(ctx, pipeline)
    }

    fn exec_stream(
        &self,
        ctx: Arc<InputContext>,
        pipeline: &mut Pipeline,
        input: Receiver<StreamingReadBatch>,
    ) -> Result<()> {
        InputFormatTextPipe::<T>::execute_stream(ctx, pipeline, input)
    }
}

#[derive(Default)]
pub struct RowBatch {
    pub data: Vec<u8>,
    pub row_ends: Vec<usize>,
    pub field_ends: Vec<usize>,

    // for error info
    pub path: String,
    pub offset: usize,
    pub start_row: Option<usize>,
}

pub struct AligningState<T> {
    pub path: String,
    pub record_delimiter_end: u8,
    pub field_delimiter: u8,
    pub rows: usize,
    pub offset: usize,
    pub rows_to_skip: usize,
    pub tail_of_last_batch: Vec<u8>,
    pub num_fields: usize,
    pub decoder: Option<DecompressDecoder>,
    pub csv_reader: Option<CsvReaderState>,
    phantom: PhantomData<T>,
}

impl<T: InputFormatTextBase> AligningState<T> {
    pub fn align_by_record_delimiter(&mut self, buf_in: &[u8]) -> Vec<RowBatch> {
        let record_delimiter_end = self.record_delimiter_end;
        let mut buf = buf_in;
        if self.rows_to_skip > 0 {
            let mut i = 0;
            for b in buf.iter() {
                if *b == record_delimiter_end {
                    self.rows_to_skip -= 1;
                    if self.rows_to_skip == 0 {
                        break;
                    }
                }
                i += 1;
            }
            if self.rows_to_skip > 0 {
                self.tail_of_last_batch = vec![];
                return vec![];
            } else {
                buf = &buf[i + 1..];
            }
        }
        if buf.is_empty() {
            return vec![];
        }

        let mut output = RowBatch::default();
        let rows = &mut output.row_ends;
        for (i, b) in buf.iter().enumerate() {
            if *b == b'\n' {
                rows.push(i)
            }
        }
        let last = rows[rows.len() - 1];
        if rows.is_empty() {
            self.tail_of_last_batch.extend_from_slice(buf);
            vec![]
        } else {
            output.data = mem::take(&mut self.tail_of_last_batch);
            output.data.extend_from_slice(&buf[0..last + 1]);
            let size = output.data.len();
            output.path = self.path.to_string();
            self.offset += size;
            tracing::debug!(
                "align {} bytes to {} rows: {} .. {}",
                size,
                rows.len(),
                rows[0],
                last
            );
            vec![output]
        }
    }

    fn flush(&mut self) -> Vec<RowBatch> {
        if self.tail_of_last_batch.is_empty() {
            vec![]
        } else {
            // last row
            let data = mem::take(&mut self.tail_of_last_batch);
            let end = data.len();
            let row_batch = RowBatch {
                data,
                row_ends: vec![end],
                field_ends: vec![],
                path: self.path.to_string(),
                offset: self.offset,
                start_row: None,
            };
            vec![row_batch]
        }
    }
}

impl<T: InputFormatTextBase> AligningStateTrait for AligningState<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn try_create(ctx: &Arc<InputContext>, split_info: &SplitInfo) -> Result<Self> {
        let rows_to_skip = if split_info.seq_infile == 0 {
            ctx.rows_to_skip
        } else {
            0
        };
        let path = split_info.file_info.path.clone();

        let decoder = ctx.get_compression_alg(&path)?.map(DecompressDecoder::new);
        let csv_reader = if T::format_type() == StageFileFormatType::Csv {
            Some(CsvReaderState::create(ctx))
        } else {
            None
        };

        Ok(AligningState::<T> {
            path,
            decoder,
            rows_to_skip,
            csv_reader,
            tail_of_last_batch: vec![],
            rows: 0,
            num_fields: ctx.schema.num_fields(),
            offset: split_info.offset,
            record_delimiter_end: ctx.record_delimiter.end(),
            field_delimiter: ctx.field_delimiter,
            phantom: Default::default(),
        })
    }

    fn align(&mut self, read_batch: Option<Vec<u8>>) -> Result<Vec<RowBatch>> {
        let row_batches = if let Some(data) = read_batch {
            let buf = if let Some(decoder) = self.decoder.as_mut() {
                decompress(decoder, &data)?
            } else {
                data
            };
            T::align(self, &buf)?
        } else {
            if let Some(decoder) = &self.decoder {
                assert_eq!(decoder.state(), DecompressState::Done)
            }
            self.flush()
        };
        Ok(row_batches)
    }
}

pub struct BlockBuilder<T> {
    pub ctx: Arc<InputContext>,
    pub mutable_columns: Vec<TypeDeserializerImpl>,
    pub num_rows: usize,
    phantom: PhantomData<T>,
}

impl<T: InputFormatTextBase> BlockBuilder<T> {
    fn flush(&mut self) -> Result<Vec<DataBlock>> {
        let mut columns = Vec::with_capacity(self.mutable_columns.len());
        for deserializer in &mut self.mutable_columns {
            columns.push(deserializer.finish_to_column());
        }
        self.mutable_columns = self
            .ctx
            .schema
            .create_deserializers(self.ctx.rows_per_block);
        self.num_rows = 0;

        Ok(vec![DataBlock::create(self.ctx.schema.clone(), columns)])
    }
}

impl<T: InputFormatTextBase> BlockBuilderTrait for BlockBuilder<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn create(ctx: Arc<InputContext>) -> Self {
        let columns = ctx.schema.create_deserializers(ctx.rows_per_block);
        BlockBuilder {
            ctx,
            mutable_columns: columns,
            num_rows: 0,
            phantom: Default::default(),
        }
    }

    fn deserialize(&mut self, batch: Option<RowBatch>) -> Result<Vec<DataBlock>> {
        if let Some(b) = batch {
            self.num_rows += b.row_ends.len();
            T::deserialize(self, b)?;
            if self.num_rows >= self.ctx.rows_per_block {
                self.flush()
            } else {
                Ok(vec![])
            }
        } else {
            self.flush()
        }
    }
}

fn decompress(decoder: &mut DecompressDecoder, compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decompress_bufs = vec![];
    let mut amt = 0;
    loop {
        match decoder.state() {
            DecompressState::Reading => {
                if amt == compressed.len() {
                    break;
                }
                let read = decoder.fill(&compressed[amt..]);
                amt += read;
            }
            DecompressState::Decoding => {
                let mut decompress_buf = vec![0u8; 4096];
                let written = decoder.decode(&mut decompress_buf[..]).map_err(|e| {
                    ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                })?;
                decompress_buf.truncate(written);
                decompress_bufs.push(decompress_buf);
            }
            DecompressState::Flushing => {
                let mut decompress_buf = vec![0u8; 4096];
                let written = decoder.finish(&mut decompress_buf).map_err(|e| {
                    ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                })?;
                decompress_buf.truncate(written);
                decompress_bufs.push(decompress_buf);
            }
            DecompressState::Done => break,
        }
    }
    Ok(decompress_bufs.concat())
}
