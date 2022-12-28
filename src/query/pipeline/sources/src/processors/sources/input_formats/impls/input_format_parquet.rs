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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::mem;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::read_columns;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::to_deserializer;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::read_metadata;
use common_arrow::read_columns_async;
use common_datablocks::DataBlock;
use common_datavalues::remove_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use futures::AsyncRead;
use futures::AsyncSeek;
use opendal::Operator;
use serde::Deserializer;
use serde::Serializer;
use similar_asserts::traits::MakeDiff;

use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_pipeline::ReadBatchTrait;
use crate::processors::sources::input_formats::input_pipeline::RowBatchTrait;
use crate::processors::sources::input_formats::input_split::DynData;
use crate::processors::sources::input_formats::input_split::FileInfo;
use crate::processors::sources::input_formats::input_split::SplitInfo;
use crate::processors::sources::input_formats::InputFormat;

pub struct InputFormatParquet;

fn col_offset(meta: &ColumnChunkMetaData) -> i64 {
    meta.data_page_offset()
}

#[async_trait::async_trait]
impl InputFormat for InputFormatParquet {
    async fn get_splits(
        &self,
        files: &[String],
        _stage_info: &UserStageInfo,
        op: &Operator,
        _settings: &Arc<Settings>,
    ) -> Result<Vec<Arc<SplitInfo>>> {
        let mut infos = vec![];
        for path in files {
            let obj = op.object(path);
            let size = obj.metadata().await?.content_length() as usize;
            let mut reader = obj.seekable_reader(..);
            let mut file_meta = read_metadata_async(&mut reader).await?;
            let row_groups = mem::take(&mut file_meta.row_groups);
            let infer_schema = infer_schema(&file_meta)?;
            let fields = Arc::new(infer_schema.fields);
            let read_file_meta = Arc::new(FileMeta { fields });

            let file_info = Arc::new(FileInfo {
                path: path.clone(),
                size,
                num_splits: row_groups.len(),
                compress_alg: None,
            });

            let num_file_splits = row_groups.len();
            for (i, rg) in row_groups.into_iter().enumerate() {
                if !rg.columns().is_empty() {
                    let offset = rg
                        .columns()
                        .iter()
                        .map(col_offset)
                        .min()
                        .expect("must success") as usize;
                    let size = rg.total_byte_size();
                    let meta = Arc::new(SplitMeta {
                        file: read_file_meta.clone(),
                        meta: rg,
                    });
                    let info = Arc::new(SplitInfo {
                        file: file_info.clone(),
                        seq_in_file: i,
                        offset,
                        size,
                        num_file_splits,
                        format_info: Some(meta),
                    });
                    infos.push(info);
                }
            }
        }
        Ok(infos)
    }

    async fn infer_schema(&self, path: &str, op: &Operator) -> Result<DataSchemaRef> {
        let obj = op.object(path);
        let mut reader = obj.seekable_reader(..);
        let file_meta = read_metadata_async(&mut reader).await?;
        let arrow_schema = infer_schema(&file_meta)?;
        Ok(Arc::new(DataSchema::from(arrow_schema)))
    }

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        ParquetFormatPipe::execute_copy_aligned(ctx, pipeline)
    }

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        ParquetFormatPipe::execute_stream(ctx, pipeline)
    }
}

pub struct ParquetFormatPipe;

#[async_trait::async_trait]
impl InputFormatPipe for ParquetFormatPipe {
    type SplitMeta = SplitMeta;
    type ReadBatch = ReadBatch;
    type RowBatch = RowGroupInMemory;
    type AligningState = AligningState;
    type BlockBuilder = ParquetBlockBuilder;

    async fn read_split(
        ctx: Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::RowBatch> {
        let meta = Self::get_split_meta(split_info).expect("must success");
        let op = ctx.source.get_operator()?;
        let obj = op.object(&split_info.file.path);
        let mut reader = obj.seekable_reader(..);
        let input_fields = Arc::new(get_used_fields(&meta.file.fields, &ctx.schema)?);
        RowGroupInMemory::read_async(&mut reader, meta.meta.clone(), input_fields).await
    }
}

pub struct FileMeta {
    // all fields in the parquet file
    pub fields: Arc<Vec<Field>>,
}

#[derive(Clone)]
pub struct SplitMeta {
    pub file: Arc<FileMeta>,
    pub meta: RowGroupMetaData,
}

impl Debug for SplitMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "parquet split meta")
    }
}

impl serde::Serialize for SplitMeta {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!()
    }
}
impl<'a> serde::Deserialize<'a> for SplitMeta {
    fn deserialize<D: Deserializer<'a>>(_deserializer: D) -> Result<Self, D::Error> {
        unimplemented!()
    }
}

#[typetag::serde(name = "parquet_split")]
impl DynData for SplitMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct RowGroupInMemory {
    pub meta: RowGroupMetaData,
    // for input, they are in the order of schema.
    // for select, they are the fields used in query.
    // used both in read and deserialize.
    pub fields_to_read: Arc<Vec<Field>>,
    pub field_meta_indexes: Vec<Vec<usize>>,
    pub field_arrays: Vec<Vec<Vec<u8>>>,
}

impl RowBatchTrait for RowGroupInMemory {
    fn size(&self) -> usize {
        self.meta.compressed_size()
    }

    fn rows(&self) -> usize {
        self.meta.num_rows()
    }
}

impl RowGroupInMemory {
    fn read<R: Read + Seek>(
        reader: &mut R,
        meta: RowGroupMetaData,
        fields: Arc<Vec<Field>>,
    ) -> Result<Self> {
        let field_names = fields.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        let field_meta_indexes = split_column_metas_by_field(meta.columns(), &field_names);
        let mut filed_arrays = vec![];
        for field_name in field_names {
            let meta_data = read_columns(reader, meta.columns(), field_name)?;
            let data = meta_data.into_iter().map(|t| t.1).collect::<Vec<_>>();
            filed_arrays.push(data)
        }
        Ok(Self {
            meta,
            field_meta_indexes,
            field_arrays: filed_arrays,
            fields_to_read: fields,
        })
    }

    async fn read_async<R: AsyncRead + AsyncSeek + Send + Unpin>(
        reader: &mut R,
        meta: RowGroupMetaData,
        fields: Arc<Vec<Field>>,
    ) -> Result<Self> {
        let field_names = fields.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        let field_meta_indexes = split_column_metas_by_field(meta.columns(), &field_names);
        let mut filed_arrays = vec![];
        for field_name in field_names {
            let meta_data = read_columns_async(reader, meta.columns(), field_name).await?;
            let data = meta_data.into_iter().map(|t| t.1).collect::<Vec<_>>();
            filed_arrays.push(data)
        }
        Ok(Self {
            meta,
            field_meta_indexes,
            field_arrays: filed_arrays,
            fields_to_read: fields,
        })
    }

    fn get_arrow_chunk(&mut self) -> Result<Chunk<Box<dyn Array>>> {
        let mut column_chunks = vec![];
        let field_arrays = mem::take(&mut self.field_arrays);
        for (f, datas) in field_arrays.into_iter().enumerate() {
            let meta_iters = self.field_meta_indexes[f]
                .iter()
                .map(|c| &self.meta.columns()[*c]);
            let meta_data = meta_iters.zip(datas.into_iter()).collect::<Vec<_>>();
            let array_iters = to_deserializer(
                meta_data,
                self.fields_to_read[f].clone(),
                self.meta.num_rows(),
                None,
                None,
            )?;
            column_chunks.push(array_iters);
        }

        match RowGroupDeserializer::new(column_chunks, self.meta.num_rows(), None).next() {
            None => Err(ErrorCode::Internal(
                "deserialize from raw group: fail to get a chunk",
            )),
            Some(Ok(chunk)) => Ok(chunk),
            Some(Err(e)) => Err(e.into()),
        }
    }
}

impl Debug for RowGroupInMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowGroupInMemory")
    }
}

#[derive(Debug)]
pub enum ReadBatch {
    Buffer(Vec<u8>),
    #[allow(unused)]
    RowGroup(RowGroupInMemory),
}

impl From<Vec<u8>> for ReadBatch {
    fn from(v: Vec<u8>) -> Self {
        Self::Buffer(v)
    }
}

impl ReadBatchTrait for ReadBatch {
    fn size(&self) -> usize {
        match self {
            ReadBatch::Buffer(v) => v.len(),
            ReadBatch::RowGroup(g) => g.size(),
        }
    }
}

pub struct ParquetBlockBuilder {
    ctx: Arc<InputContext>,
}

impl BlockBuilderTrait for ParquetBlockBuilder {
    type Pipe = ParquetFormatPipe;

    fn create(ctx: Arc<InputContext>) -> Self {
        ParquetBlockBuilder { ctx }
    }

    fn deserialize(&mut self, mut batch: Option<RowGroupInMemory>) -> Result<Vec<DataBlock>> {
        if let Some(rg) = batch.as_mut() {
            let chunk = rg.get_arrow_chunk()?;
            let block = DataBlock::from_chunk(&self.ctx.schema, &chunk)?;

            let block_total_rows = block.num_rows();
            let num_rows_per_block = self.ctx.block_compact_thresholds.max_rows_per_block;
            let blocks: Vec<DataBlock> = (0..block_total_rows)
                .step_by(num_rows_per_block)
                .map(|idx| {
                    if idx + num_rows_per_block < block_total_rows {
                        block.slice(idx, num_rows_per_block)
                    } else {
                        block.slice(idx, block_total_rows - idx)
                    }
                })
                .collect();

            Ok(blocks)
        } else {
            Ok(vec![])
        }
    }
}

pub struct AligningState {
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    buffers: Vec<Vec<u8>>,
}

impl AligningStateTrait for AligningState {
    type Pipe = ParquetFormatPipe;

    fn try_create(ctx: &Arc<InputContext>, split_info: &Arc<SplitInfo>) -> Result<Self> {
        Ok(AligningState {
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            buffers: vec![],
        })
    }

    fn align(&mut self, read_batch: Option<ReadBatch>) -> Result<Vec<RowGroupInMemory>> {
        if let Some(rb) = read_batch {
            if let ReadBatch::Buffer(b) = rb {
                self.buffers.push(b)
            } else {
                return Err(ErrorCode::Internal(
                    "Bug: should not see ReadBatch::RowGroup in align().",
                ));
            };
            Ok(vec![])
        } else {
            let file_in_memory = self.buffers.concat();
            let size = file_in_memory.len();
            tracing::debug!(
                "aligning parquet file {} of {} bytes",
                self.split_info.file.path,
                size,
            );
            let mut cursor = Cursor::new(file_in_memory);
            let file_meta = read_metadata(&mut cursor)?;
            let infer_schema = infer_schema(&file_meta)?;
            let fields = Arc::new(get_used_fields(&infer_schema.fields, &self.ctx.schema)?);
            let mut row_batches = Vec::with_capacity(file_meta.row_groups.len());
            for row_group in file_meta.row_groups.into_iter() {
                row_batches.push(RowGroupInMemory::read(
                    &mut cursor,
                    row_group,
                    fields.clone(),
                )?)
            }
            tracing::info!(
                "align parquet file {} of {} bytes to {} row groups",
                self.split_info.file.path,
                size,
                row_batches.len()
            );
            Ok(row_batches)
        }
    }
}

fn get_used_fields(fields: &Vec<Field>, schema: &DataSchemaRef) -> Result<Vec<Field>> {
    let mut read_fields = Vec::with_capacity(fields.len());
    for f in schema.fields().iter() {
        if let Some(m) = fields
            .iter()
            .filter(|c| c.name.eq_ignore_ascii_case(f.name()))
            .last()
        {
            let tf = DataField::from(m);
            if remove_nullable(tf.data_type()) != remove_nullable(f.data_type()) {
                let pair = (f, m);
                let diff = pair.make_diff("expected_field", "infer_field");
                // TODO(xuanwo): return a more accurate error code here.
                return Err(ErrorCode::Internal(format!(
                    "parquet schema mismatch, differ: {}",
                    diff
                )));
            }

            read_fields.push(m.clone());
        } else {
            // TODO(xuanwo): return a more accurate error code here.
            return Err(ErrorCode::Internal(format!(
                "schema field size mismatch, expected to find column: {}",
                f.name()
            )));
        }
    }
    Ok(read_fields)
}

pub fn split_column_metas_by_field(
    columns: &[ColumnChunkMetaData],
    field_names: &[&str],
) -> Vec<Vec<usize>> {
    let mut r = field_names.iter().map(|_| vec![]).collect::<Vec<_>>();
    let d = field_names
        .iter()
        .enumerate()
        .map(|(i, name)| (name, i))
        .collect::<HashMap<_, _>>();
    columns.iter().enumerate().for_each(|(col_i, x)| {
        if let Some(field_i) = d.get(&x.descriptor().path_in_schema[0].as_str()) {
            r[*field_i].push(col_i);
        }
    });
    r
}
