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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FieldDecoder;
use common_formats::FileFormatOptionsExt;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use opendal::raw::DecompressDecoder;
use opendal::raw::DecompressState;
use opendal::Operator;

use super::InputFormat;
use crate::processors::sources::input_formats::beyond_end_reader::BeyondEndReader;
use crate::processors::sources::input_formats::impls::input_format_csv::CsvReaderState;
use crate::processors::sources::input_formats::impls::input_format_xml::XmlReaderState;
use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_pipeline::RowBatchTrait;
use crate::processors::sources::input_formats::input_split::split_by_size;
use crate::processors::sources::input_formats::input_split::FileInfo;
use crate::processors::sources::input_formats::input_split::SplitInfo;

pub trait InputFormatTextBase: Sized + Send + Sync + 'static {
    fn format_type() -> StageFileFormatType;

    fn is_splittable() -> bool {
        false
    }

    fn create_field_decoder(options: &FileFormatOptionsExt) -> Arc<dyn FieldDecoder>;

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()>;

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>>;

    fn align_flush(state: &mut AligningState<Self>) -> Result<Vec<RowBatch>> {
        if state.tail_of_last_batch.is_empty() {
            Ok(vec![])
        } else {
            // last row
            let data = mem::take(&mut state.tail_of_last_batch);
            let end = data.len();
            let row_batch = RowBatch {
                data,
                row_ends: vec![end],
                field_ends: vec![],
                path: state.path.to_string(),
                batch_id: state.batch_id,
                offset: state.offset,
                start_row: Some(state.rows),
            };
            tracing::debug!(
                "align flush batch {}, bytes = {}, start_row = {}",
                row_batch.batch_id,
                state.tail_of_last_batch.len(),
                state.rows
            );
            Ok(vec![row_batch])
        }
    }
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
    type SplitMeta = ();
    type ReadBatch = Vec<u8>;
    type RowBatch = RowBatch;
    type AligningState = AligningState<T>;
    type BlockBuilder = BlockBuilder<T>;
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> InputFormat for InputFormatText<T> {
    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        InputFormatTextPipe::<T>::execute_copy_with_aligner(ctx, pipeline)
    }

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        InputFormatTextPipe::<T>::execute_stream(ctx, pipeline)
    }

    async fn infer_schema(&self, _path: &str, _op: &Operator) -> Result<DataSchemaRef> {
        Err(ErrorCode::Unimplemented(
            "infer_schema is not implemented for this format yet.",
        ))
    }

    async fn get_splits(
        &self,
        files: &[String],
        stage_info: &UserStageInfo,
        op: &Operator,
        _settings: &Arc<Settings>,
    ) -> Result<Vec<Arc<SplitInfo>>> {
        let mut infos = vec![];
        for path in files {
            let obj = op.object(path);
            let size = obj.metadata().await?.content_length() as usize;
            let compress_alg = InputContext::get_compression_alg_copy(
                stage_info.file_format_options.compression,
                path,
            )?;
            let split_size = stage_info.copy_options.split_size;
            if compress_alg.is_none() && T::is_splittable() && split_size > 0 {
                let split_offsets = split_by_size(size, split_size);
                let num_file_splits = split_offsets.len();
                tracing::debug!(
                    "split file {} of size {} to {} {} bytes splits",
                    path,
                    size,
                    num_file_splits,
                    split_size
                );
                let file = Arc::new(FileInfo {
                    path: path.clone(),
                    size,
                    num_splits: split_offsets.len(),
                    compress_alg,
                });
                for (i, (offset, size)) in split_offsets.into_iter().enumerate() {
                    infos.push(Arc::new(SplitInfo {
                        file: file.clone(),
                        seq_in_file: i,
                        offset,
                        size,
                        num_file_splits,
                        format_info: None,
                    }));
                }
            } else {
                let file = Arc::new(FileInfo {
                    path: path.clone(),
                    size, // dummy
                    num_splits: 1,
                    compress_alg,
                });
                infos.push(Arc::new(SplitInfo {
                    file,
                    seq_in_file: 0,
                    offset: 0,
                    size, // dummy
                    num_file_splits: 1,
                    format_info: None,
                }));
            }
        }
        Ok(infos)
    }
}

#[derive(Default)]
pub struct RowBatch {
    pub data: Vec<u8>,
    pub row_ends: Vec<usize>,
    pub field_ends: Vec<usize>,

    // for error info
    pub path: String,
    pub batch_id: usize,
    pub offset: usize,
    pub start_row: Option<usize>,
}

impl RowBatchTrait for RowBatch {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn rows(&self) -> usize {
        self.row_ends.len()
    }
}

pub struct AligningState<T> {
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    pub path: String,
    pub record_delimiter_end: u8,
    pub field_delimiter: u8,
    pub batch_id: usize,
    pub rows: usize,
    pub offset: usize,
    pub rows_to_skip: usize,
    pub tail_of_last_batch: Vec<u8>,
    pub num_fields: usize,
    pub decoder: Option<DecompressDecoder>,
    pub csv_reader: Option<CsvReaderState>,
    pub xml_reader: Option<XmlReaderState>,
    phantom: PhantomData<T>,
}

impl<T: InputFormatTextBase> AligningState<T> {
    pub fn align_by_record_delimiter(&mut self, buf_in: &[u8]) -> Vec<RowBatch> {
        let record_delimiter_end = self.record_delimiter_end;
        let size_last_remain = self.tail_of_last_batch.len();
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
                rows.push(i + 1 + size_last_remain)
            }
        }
        if rows.is_empty() {
            self.tail_of_last_batch.extend_from_slice(buf);
            vec![]
        } else {
            let batch_end = rows[rows.len() - 1] - size_last_remain;
            output.data = mem::take(&mut self.tail_of_last_batch);
            output.data.extend_from_slice(&buf[..batch_end]);
            self.tail_of_last_batch.extend_from_slice(&buf[batch_end..]);
            let size = output.data.len();
            output.path = self.path.to_string();
            output.start_row = Some(self.rows);
            output.offset = self.offset;
            output.batch_id = self.batch_id;
            self.offset += size;
            self.rows += rows.len();
            self.batch_id += 1;
            tracing::debug!(
                "align batch {}, {} + {} + {} bytes to {} rows",
                output.batch_id,
                size_last_remain,
                batch_end,
                self.tail_of_last_batch.len(),
                rows.len(),
            );
            vec![output]
        }
    }
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> AligningStateTrait for AligningState<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn try_create(ctx: &Arc<InputContext>, split_info: &Arc<SplitInfo>) -> Result<Self> {
        let rows_to_skip = if split_info.seq_in_file == 0 {
            ctx.rows_to_skip
        } else {
            (T::is_splittable() && split_info.num_file_splits > 1) as usize
        };
        let path = split_info.file.path.clone();

        let decoder = ctx.get_compression_alg(&path)?.map(DecompressDecoder::new);
        let csv_reader = if T::format_type() == StageFileFormatType::Csv {
            Some(CsvReaderState::create(ctx))
        } else {
            None
        };

        let xml_reader = if T::format_type() == StageFileFormatType::Xml {
            Some(XmlReaderState::create(ctx))
        } else {
            None
        };

        Ok(AligningState::<T> {
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            path,
            decoder,
            rows_to_skip,
            csv_reader,
            xml_reader,
            tail_of_last_batch: vec![],
            rows: 0,
            batch_id: 0,
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
                let state = decoder.state();
                if !matches!(state, DecompressState::Done | DecompressState::Reading) {
                    tracing::warn!("decompressor end with state {:?}", state)
                }
            }
            T::align_flush(self)?
        };
        Ok(row_batches)
    }

    fn read_beyond_end(&self) -> Option<BeyondEndReader> {
        Some(BeyondEndReader {
            ctx: self.ctx.clone(),
            split_info: self.split_info.clone(),
            path: self.path.clone(),
            record_delimiter_end: self.record_delimiter_end,
        })
    }
}

pub struct BlockBuilder<T> {
    pub field_decoder: Arc<dyn FieldDecoder>,
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
            .create_deserializers(self.ctx.block_compact_thresholds.min_rows_per_block);
        self.num_rows = 0;

        Ok(vec![DataBlock::create(self.ctx.schema.clone(), columns)])
    }

    fn memory_size(&self) -> usize {
        self.mutable_columns.iter().map(|x| x.memory_size()).sum()
    }
}

impl<T: InputFormatTextBase> BlockBuilderTrait for BlockBuilder<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn create(ctx: Arc<InputContext>) -> Self {
        let columns = ctx
            .schema
            .create_deserializers(ctx.block_compact_thresholds.min_rows_per_block);
        let field_decoder = T::create_field_decoder(&ctx.format_options);
        BlockBuilder {
            ctx,
            mutable_columns: columns,
            num_rows: 0,
            phantom: Default::default(),
            field_decoder,
        }
    }

    fn deserialize(&mut self, batch: Option<RowBatch>) -> Result<Vec<DataBlock>> {
        if let Some(b) = batch {
            self.num_rows += b.row_ends.len();
            T::deserialize(self, b)?;
            let mem = self.memory_size();
            tracing::debug!(
                "block builder added new batch: row {} size {}",
                self.num_rows,
                mem
            );
            if self.num_rows >= self.ctx.block_compact_thresholds.min_rows_per_block
                || mem > self.ctx.block_compact_thresholds.max_bytes_per_block
            {
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
