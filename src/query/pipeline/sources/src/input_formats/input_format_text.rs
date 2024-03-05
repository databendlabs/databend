// Copyright 2021 Datafuse Labs
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

use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use databend_common_compress::DecompressDecoder;
use databend_common_compress::DecompressState;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_formats::FieldDecoder;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::StageInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::Settings;
use databend_common_storage::FileStatus;
use databend_common_storage::StageFileInfo;
use log::debug;
use log::info;
use opendal::Operator;

use crate::input_formats::input_pipeline::AligningStateTrait;
use crate::input_formats::input_pipeline::BlockBuilderTrait;
use crate::input_formats::input_pipeline::InputFormatPipe;
use crate::input_formats::input_pipeline::RowBatchTrait;
use crate::input_formats::input_split::FileInfo;
use crate::input_formats::split_by_size;
use crate::input_formats::BeyondEndReader;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormat;
use crate::input_formats::SplitInfo;

pub trait AligningStateTextBased: Sync + Sized + Send {
    fn is_splittable() -> bool {
        false
    }

    fn align(&mut self, buf: &[u8]) -> Result<Vec<RowBatch>>;

    fn align_flush(&mut self) -> Result<Vec<RowBatch>>;

    fn read_beyond_end(&self) -> Option<BeyondEndReader> {
        None
    }
}

pub struct AligningStateCommon {
    pub batch_id: usize,
    pub rows: usize,
    pub offset: usize,
    pub rows_to_skip: usize,
}

impl AligningStateCommon {
    pub fn create(split_info: &Arc<SplitInfo>, is_splittable: bool, skip_header: usize) -> Self {
        let rows_to_skip = if split_info.seq_in_file == 0 {
            skip_header
        } else {
            (is_splittable && split_info.num_file_splits > 1) as usize
        };
        Self {
            batch_id: 0,
            rows: 0,
            offset: 0,
            rows_to_skip,
        }
    }
}

pub struct AligningStateRowDelimiter {
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    record_delimiter_end: u8,
    record_delimiter_escaped: bool,

    common: AligningStateCommon,
    tail_of_last_batch: Vec<u8>,

    in_escape: bool,
}

impl AligningStateRowDelimiter {
    pub fn try_create(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
        record_delimiter_end: u8,
        record_delimiter_escaped: bool,
        headers: usize,
    ) -> Result<Self> {
        Ok(Self {
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            record_delimiter_end,
            record_delimiter_escaped,
            common: AligningStateCommon::create(split_info, true, headers),
            tail_of_last_batch: vec![],
            in_escape: false,
        })
    }

    fn align_splittable(&mut self, buf_in: &[u8]) -> Result<Vec<RowBatch>> {
        let record_delimiter_end = self.record_delimiter_end;
        let size_last_remain = self.tail_of_last_batch.len();
        let mut buf = buf_in;
        if self.common.rows_to_skip > 0 {
            let mut i = 0;
            for b in buf.iter() {
                if *b == record_delimiter_end {
                    self.common.rows_to_skip -= 1;
                    if self.common.rows_to_skip == 0 {
                        break;
                    }
                }
                i += 1;
            }
            if self.common.rows_to_skip > 0 {
                self.tail_of_last_batch = vec![];
                return Ok(vec![]);
            } else {
                buf = &buf[i + 1..];
            }
        }
        if buf.is_empty() {
            return Ok(vec![]);
        }

        let mut output = RowBatch {
            data: vec![],
            row_ends: vec![],
            field_ends: vec![],
            num_fields: vec![],
            split_info: self.split_info.clone(),
            batch_id: self.common.batch_id,
            start_offset_in_split: self.common.offset,
            start_row_in_split: self.common.rows,
            start_row_of_split: self.split_info.start_row_text(),
        };
        let rows = &mut output.row_ends;
        for (i, b) in buf.iter().enumerate() {
            if *b == record_delimiter_end {
                rows.push(i + 1 + size_last_remain)
            }
        }
        if rows.is_empty() {
            self.tail_of_last_batch.extend_from_slice(buf);
            Ok(vec![])
        } else {
            let batch_end = rows[rows.len() - 1] - size_last_remain;
            output.data = mem::take(&mut self.tail_of_last_batch);
            output.data.extend_from_slice(&buf[..batch_end]);
            self.tail_of_last_batch.extend_from_slice(&buf[batch_end..]);
            let size = output.data.len();
            self.common.offset += size;
            self.common.rows += rows.len();
            self.common.batch_id += 1;
            debug!(
                "align batch {}, {} + {} + {} bytes to {} rows",
                output.batch_id,
                size_last_remain,
                batch_end,
                self.tail_of_last_batch.len(),
                rows.len(),
            );
            Ok(vec![output])
        }
    }

    fn align_non_splittable(&mut self, buf_in: &[u8]) -> Result<Vec<RowBatch>> {
        let record_delimiter_end = self.record_delimiter_end;
        let size_last_remain = self.tail_of_last_batch.len();
        let mut buf = buf_in;
        if self.common.rows_to_skip > 0 {
            let mut i = 0;
            for b in buf.iter() {
                if *b == record_delimiter_end {
                    if self.in_escape {
                        self.in_escape = false;
                    } else {
                        self.common.rows_to_skip -= 1;
                        if self.common.rows_to_skip == 0 {
                            break;
                        }
                    }
                } else {
                    // '\\\[delimiter]' -> '\\'
                    self.in_escape = (*b == b'\\') && !self.in_escape
                }
                i += 1;
            }
            if self.common.rows_to_skip > 0 {
                self.tail_of_last_batch = vec![];
                return Ok(vec![]);
            } else {
                buf = &buf[i + 1..];
            }
        }
        if buf.is_empty() {
            return Ok(vec![]);
        }

        let mut data: Vec<u8> = mem::take(&mut self.tail_of_last_batch);
        data.reserve(buf.len() + size_last_remain);
        let mut row_ends = vec![];
        for b in buf.iter() {
            if *b == record_delimiter_end {
                if !self.in_escape {
                    row_ends.push(data.len())
                } else {
                    data.pop();
                    data.push(record_delimiter_end);
                    self.in_escape = false;
                }
            } else {
                data.push(*b);
                // '\\\[delimiter]' -> '\\'
                self.in_escape = (*b == b'\\') && !self.in_escape
            }
        }

        if row_ends.is_empty() {
            self.tail_of_last_batch = data;
            Ok(vec![])
        } else {
            let batch_end = row_ends[row_ends.len() - 1];
            self.tail_of_last_batch = data.split_off(batch_end);
            let len = data.len();
            let num_rows = row_ends.len();
            let output = RowBatch {
                data,
                row_ends,
                field_ends: vec![],
                num_fields: vec![],
                split_info: self.split_info.clone(),
                batch_id: self.common.batch_id,
                start_offset_in_split: self.common.offset,
                start_row_in_split: self.common.rows,
                start_row_of_split: self.split_info.start_row_text(),
            };

            self.common.offset += len;
            self.common.rows += num_rows;
            self.common.batch_id += 1;
            debug!(
                "align batch {}, {} + {} + {} bytes to {} rows",
                output.batch_id,
                size_last_remain,
                batch_end,
                self.tail_of_last_batch.len(),
                num_rows,
            );
            Ok(vec![output])
        }
    }
}

impl AligningStateTextBased for AligningStateRowDelimiter {
    fn align(&mut self, buf_in: &[u8]) -> Result<Vec<RowBatch>> {
        if self.record_delimiter_escaped {
            self.align_splittable(buf_in)
        } else {
            self.align_non_splittable(buf_in)
        }
    }

    fn align_flush(&mut self) -> Result<Vec<RowBatch>> {
        if self.tail_of_last_batch.is_empty() {
            Ok(vec![])
        } else {
            // last row
            let data = mem::take(&mut self.tail_of_last_batch);
            let end = data.len();
            let row_batch = RowBatch {
                data,
                row_ends: vec![end],
                field_ends: vec![],
                num_fields: vec![],
                split_info: self.split_info.clone(),
                batch_id: self.common.batch_id,
                start_offset_in_split: self.common.offset,
                start_row_in_split: self.common.rows,
                start_row_of_split: self.split_info.start_row_text(),
            };
            debug!(
                "align flush batch {}, bytes = {}, start_row = {}",
                row_batch.batch_id,
                self.tail_of_last_batch.len(),
                self.common.rows
            );
            Ok(vec![row_batch])
        }
    }

    fn read_beyond_end(&self) -> Option<BeyondEndReader> {
        if self.record_delimiter_escaped {
            Some(BeyondEndReader {
                ctx: self.ctx.clone(),
                split_info: self.split_info.clone(),
                path: self.split_info.file.path.clone(),
                record_delimiter_end: self.record_delimiter_end,
            })
        } else {
            None
        }
    }
}

pub trait InputFormatTextBase: Sized + Send + Sync + 'static {
    type AligningState: AligningStateTextBased;

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState>;

    fn try_create_block_builder(ctx: &Arc<InputContext>) -> Result<BlockBuilder<Self>> {
        Ok(BlockBuilder::<Self>::create(ctx.clone()))
    }

    fn format_type() -> StageFileFormatType;

    fn is_splittable() -> bool {
        false
    }

    fn create_field_decoder(
        params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder>;

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()>;
}

pub struct InputFormatTextPipe<T> {
    phantom: PhantomData<T>,
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> InputFormatPipe for InputFormatTextPipe<T> {
    type SplitMeta = ();
    type ReadBatch = Vec<u8>;
    type RowBatch = RowBatch;
    type AligningState = AligningStateMaybeCompressed<T>;
    type BlockBuilder = BlockBuilder<T>;

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        AligningStateMaybeCompressed::<T>::try_create(ctx, split_info)
    }

    fn try_create_block_builder(ctx: &Arc<InputContext>) -> Result<Self::BlockBuilder> {
        Ok(BlockBuilder::<T>::create(ctx.clone()))
    }
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> InputFormat for T {
    #[async_backtrace::framed]
    async fn get_splits(
        &self,
        file_infos: Vec<StageFileInfo>,
        stage_info: &StageInfo,
        _op: &Operator,
        _settings: &Arc<Settings>,
    ) -> Result<Vec<Arc<SplitInfo>>> {
        let mut infos = vec![];

        for info in file_infos {
            let size = info.size as usize;
            let path = info.path.clone();

            let compress_alg = InputContext::get_compression_alg_copy(
                stage_info.file_format_params.compression(),
                &path,
            )?;
            let split_size = stage_info.copy_options.split_size;
            if compress_alg.is_none()
                && T::is_splittable()
                && split_size > 0
                && stage_info.copy_options.on_error == OnErrorMode::AbortNum(1)
            {
                let split_offsets = split_by_size(size, split_size);
                let num_file_splits = split_offsets.len();
                info!(
                    "split file {} of size {} to {} {} bytes splits",
                    path, size, num_file_splits, split_size
                );
                let file = Arc::new(FileInfo {
                    path,
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
                    path,
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

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        InputFormatTextPipe::<T>::execute_copy_with_aligner(ctx, pipeline)
    }

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        InputFormatTextPipe::<T>::execute_stream(ctx, pipeline)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RowBatch {
    /// row[i] starts at row_ends[i-1] and ends at row_ends[i]
    /// has num_fields[i] fields
    /// field[j] starts at field_ends[i-1][j] and ends at field_ends[i-1][j]
    pub data: Vec<u8>,
    pub row_ends: Vec<usize>,
    pub field_ends: Vec<usize>,
    pub num_fields: Vec<usize>,

    pub split_info: Arc<SplitInfo>,
    // for error info
    pub batch_id: usize,
    pub start_offset_in_split: usize,
    pub start_row_in_split: usize,
    pub start_row_of_split: Option<usize>,
}

impl RowBatchTrait for RowBatch {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn rows(&self) -> usize {
        self.row_ends.len()
    }
}

#[typetag::serde(name = "row_batch")]
impl BlockMetaInfo for RowBatch {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be cloned.")
    }
}

pub struct AligningStateMaybeCompressed<T: InputFormatTextBase> {
    #[allow(unused)]
    ctx: Arc<InputContext>,
    #[allow(unused)]
    split_info: Arc<SplitInfo>,
    pub decompressor: Option<DecompressDecoder>,
    state: T::AligningState,
}

impl<T: InputFormatTextBase> AligningStateMaybeCompressed<T> {
    fn try_create(ctx: &Arc<InputContext>, split_info: &Arc<SplitInfo>) -> Result<Self> {
        let path = split_info.file.path.clone();
        let decompressor = ctx.get_compression_alg(&path)?.map(DecompressDecoder::new);
        let state = T::try_create_align_state(ctx, split_info)?;

        Ok(Self {
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            decompressor,
            state,
        })
    }
}

#[async_trait::async_trait]
impl<T: InputFormatTextBase> AligningStateTrait for AligningStateMaybeCompressed<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn align(&mut self, read_batch: Option<Vec<u8>>) -> Result<Vec<RowBatch>> {
        let row_batches = if let Some(data) = read_batch {
            let buf = if let Some(decoder) = self.decompressor.as_mut() {
                decoder.decompress_batch(&data)?
            } else {
                data
            };
            self.state.align(&buf)?
        } else {
            if let Some(decoder) = self.decompressor.as_mut() {
                let state = decoder.state();
                if !matches!(state, DecompressState::Done) {
                    let data = decoder.decompress_batch(&[])?;
                    if !data.is_empty() {
                        self.state.align(&data)?;
                    }
                }
                if !matches!(state, DecompressState::Done) {
                    return Err(ErrorCode::BadBytes(format!(
                        "decompressor state is {:?} after decompressing all data",
                        state
                    )));
                }
            }
            self.state.align_flush()?
        };
        Ok(row_batches)
    }

    fn read_beyond_end(&self) -> Option<BeyondEndReader> {
        self.state.read_beyond_end()
    }
}

pub struct BlockBuilder<T> {
    pub field_decoder: Arc<dyn FieldDecoder>,
    pub ctx: Arc<InputContext>,
    pub mutable_columns: Vec<ColumnBuilder>,
    pub num_rows: usize,
    pub projection: Option<Vec<usize>>,
    pub file_status: FileStatus,
    pub ident_case_sensitive: bool,
    phantom: PhantomData<T>,
}

impl<T: InputFormatTextBase> BlockBuilder<T> {
    fn create(ctx: Arc<InputContext>) -> Self {
        let columns = ctx
            .schema
            .fields()
            .iter()
            .map(|f| {
                ColumnBuilder::with_capacity_hint(
                    &f.data_type().into(),
                    // todo(youngsofun): calculate the capacity based on the memory and schema
                    1024,
                    false,
                )
            })
            .collect();
        let field_decoder =
            T::create_field_decoder(&ctx.file_format_params, &ctx.file_format_options_ext);
        let projection = ctx.projection.clone();

        BlockBuilder {
            ident_case_sensitive: ctx.file_format_options_ext.ident_case_sensitive,
            mutable_columns: columns,
            num_rows: 0,
            field_decoder,
            phantom: PhantomData,
            projection,
            file_status: Default::default(),
            ctx,
        }
    }

    fn flush(&mut self) -> Result<Vec<DataBlock>> {
        let columns: Vec<Column> = self
            .mutable_columns
            .iter_mut()
            .map(|col| {
                let empty_builder =
                    ColumnBuilder::with_capacity_hint(&col.data_type(), 1024, false);
                std::mem::replace(col, empty_builder).build()
            })
            .collect();

        let columns = if let Some(projection) = &self.projection {
            columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    if projection.contains(&i) {
                        c
                    } else {
                        Column::String(
                            StringColumnBuilder {
                                need_estimated: false,
                                data: vec![],
                                offsets: vec![0; self.num_rows + 1],
                            }
                            .build(),
                        )
                    }
                })
                .collect::<Vec<_>>()
        } else {
            columns
        };
        self.num_rows = 0;

        if columns.is_empty() || columns[0].len() == 0 {
            Ok(vec![])
        } else {
            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }

    fn memory_size(&self) -> usize {
        self.mutable_columns.iter().map(|x| x.memory_size()).sum()
    }
}

impl<T: InputFormatTextBase> BlockBuilderTrait for BlockBuilder<T> {
    type Pipe = InputFormatTextPipe<T>;

    fn deserialize(&mut self, batch: Option<RowBatch>) -> Result<Vec<DataBlock>> {
        if let Some(b) = batch {
            let file_name = b.split_info.file.path.clone();
            T::deserialize(self, b)?;
            let file_status = mem::take(&mut self.file_status);
            self.ctx
                .table_context
                .add_file_status(&file_name, file_status)?;
            let mem = self.memory_size();
            debug!(
                "chunk builder added new batch: row {} size {}",
                self.num_rows, mem
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
