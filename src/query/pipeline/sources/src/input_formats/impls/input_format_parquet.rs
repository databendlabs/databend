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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::mem;
use std::sync::Arc;

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::chunk::Chunk as ArrowChunk;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::io::parquet::read::read_columns;
use databend_common_arrow::arrow::io::parquet::read::to_deserializer;
use databend_common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use databend_common_arrow::parquet::metadata::ColumnChunkMetaData;
use databend_common_arrow::parquet::metadata::RowGroupMetaData;
use databend_common_arrow::parquet::read::read_metadata;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::Settings;
use databend_common_storage::infer_schema_with_extension;
use databend_common_storage::StageFileInfo;
use log::debug;
use log::info;
use opendal::Operator;
use opendal::Reader;
use serde::Deserializer;
use serde::Serializer;

use crate::input_formats::input_pipeline::AligningStateTrait;
use crate::input_formats::input_pipeline::BlockBuilderTrait;
use crate::input_formats::input_pipeline::InputFormatPipe;
use crate::input_formats::input_pipeline::ReadBatchTrait;
use crate::input_formats::input_pipeline::RowBatchTrait;
use crate::input_formats::input_split::DynData;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormat;
use crate::input_formats::SplitInfo;

pub struct InputFormatParquet;

#[async_trait::async_trait]
impl InputFormat for InputFormatParquet {
    #[async_backtrace::framed]
    async fn get_splits(
        &self,
        _file_infos: Vec<StageFileInfo>,
        _stage_info: &StageInfo,
        _op: &Operator,
        _settings: &Arc<Settings>,
    ) -> Result<Vec<Arc<SplitInfo>>> {
        unimplemented!("InputFormatParquet::get_splits")
    }

    fn exec_copy(&self, _ctx: Arc<InputContext>, _pipeline: &mut Pipeline) -> Result<()> {
        unimplemented!("ParquetFormatPipe::exec_copy")
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
    type AligningState = ParquetAligningState;
    type BlockBuilder = ParquetBlockBuilder;

    #[async_backtrace::framed]
    async fn read_split(
        ctx: Arc<InputContext>,
        split_info: Arc<SplitInfo>,
    ) -> Result<Self::RowBatch> {
        let meta = Self::get_split_meta(&split_info).expect("must success");
        let op = ctx.source.get_operator()?;
        let input_fields = Arc::new(get_used_fields(&meta.file.fields, &ctx.schema)?);

        RowGroupInMemory::read_async(split_info.clone(), op, meta.meta.clone(), input_fields).await
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<ParquetAligningState> {
        Ok(ParquetAligningState {
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            buffers: vec![],
        })
    }

    fn try_create_block_builder(_ctx: &Arc<InputContext>) -> Result<ParquetBlockBuilder> {
        Ok(ParquetBlockBuilder {})
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RowGroupInMemory {
    pub split_info: String,
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

#[typetag::serde(name = "row_batch_parquet")]
impl BlockMetaInfo for RowGroupInMemory {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RowGroupInMemory as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RowGroupInMemory as BlockMetaInfo is not expected to be cloned.")
    }
}

impl RowGroupInMemory {
    fn read<R: Read + Seek>(
        split_info: String,
        reader: &mut R,
        meta: RowGroupMetaData,
        fields: Arc<Vec<Field>>,
    ) -> Result<Self> {
        let field_names = fields.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        let field_meta_indexes = split_column_metas_by_field(meta.columns(), &field_names);
        let mut field_arrays = vec![];
        for field_name in field_names {
            let meta_data = read_columns(reader, meta.columns(), field_name)?;
            let data = meta_data.into_iter().map(|t| t.1).collect::<Vec<_>>();
            field_arrays.push(data)
        }
        Ok(Self {
            split_info,
            meta,
            field_meta_indexes,
            field_arrays,
            fields_to_read: fields,
        })
    }

    #[async_backtrace::framed]
    async fn read_field_async(
        op: Operator,
        path: String,
        col_metas: Vec<ColumnChunkMetaData>,
        index: usize,
    ) -> Result<(usize, Vec<Vec<u8>>)> {
        let mut cols = Vec::with_capacity(col_metas.len());
        let reader = op.reader(&path).await?;
        for meta in &col_metas {
            cols.push(read_single_column_async(reader.clone(), meta).await?)
        }
        Ok((index, cols))
    }

    #[async_backtrace::framed]
    async fn read_async(
        split_info: Arc<SplitInfo>,
        operator: Operator,
        meta: RowGroupMetaData,
        fields: Arc<Vec<Field>>,
    ) -> Result<Self> {
        let field_names = fields.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        let field_meta_indexes = split_column_metas_by_field(meta.columns(), &field_names);
        let mut join_handlers = Vec::with_capacity(field_names.len());
        for (i, field_name) in field_names.iter().enumerate() {
            let col_metas = get_field_columns(meta.columns(), field_name)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let op = operator.clone();
            let path = split_info.file.path.clone();
            join_handlers.push(async move { Self::read_field_async(op, path, col_metas, i).await });
        }

        let mut field_arrays = futures::future::try_join_all(join_handlers).await?;
        field_arrays.sort();
        let field_arrays = field_arrays.into_iter().map(|t| t.1).collect::<Vec<_>>();

        Ok(Self {
            split_info: split_info.to_string(),
            meta,
            field_meta_indexes,
            field_arrays,
            fields_to_read: fields,
        })
    }

    fn get_arrow_chunk(&mut self) -> Result<ArrowChunk<Box<dyn Array>>> {
        let mut column_chunks = vec![];
        let field_arrays = mem::take(&mut self.field_arrays);
        for (f, data) in field_arrays.into_iter().enumerate() {
            let meta_iters = self.field_meta_indexes[f]
                .iter()
                .map(|c| &self.meta.columns()[*c]);
            let meta_data = meta_iters.zip(data.into_iter()).collect::<Vec<_>>();
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
            None => Err(ErrorCode::Internal(format!(
                "no chunk when deserialize row group {}",
                self.split_info
            ))),
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

pub struct ParquetBlockBuilder {}

impl BlockBuilderTrait for ParquetBlockBuilder {
    type Pipe = ParquetFormatPipe;

    fn deserialize(&mut self, mut batch: Option<RowGroupInMemory>) -> Result<Vec<DataBlock>> {
        if let Some(rg) = batch.as_mut() {
            let chunk = rg.get_arrow_chunk()?;

            let fields: Vec<DataField> = rg
                .fields_to_read
                .iter()
                .map(DataField::try_from)
                .collect::<Result<Vec<_>>>()?;

            let input_schema = DataSchema::new(fields);
            let block = DataBlock::from_arrow_chunk(&chunk, &input_schema)?;
            Ok(vec![block])
        } else {
            Ok(vec![])
        }
    }
}

pub struct ParquetAligningState {
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    buffers: Vec<Vec<u8>>,
}

impl AligningStateTrait for ParquetAligningState {
    type Pipe = ParquetFormatPipe;

    fn align(&mut self, read_batch: Option<ReadBatch>) -> Result<Vec<RowGroupInMemory>> {
        let split_info = self.split_info.to_string();
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
            debug!(
                "aligning parquet file {} of {} bytes",
                self.split_info.file.path, size,
            );
            let mut cursor = Cursor::new(file_in_memory);
            let file_meta = read_metadata(&mut cursor)?;
            let infer_schema = infer_schema_with_extension(&file_meta)?;
            let fields = Arc::new(get_used_fields(&infer_schema.fields, &self.ctx.schema)?);
            let mut row_batches = Vec::with_capacity(file_meta.row_groups.len());
            for row_group in file_meta.row_groups.into_iter() {
                row_batches.push(RowGroupInMemory::read(
                    split_info.clone(),
                    &mut cursor,
                    row_group,
                    fields.clone(),
                )?)
            }
            info!(
                "align parquet file {} of {} bytes to {} row groups",
                self.split_info.file.path,
                size,
                row_batches.len()
            );
            Ok(row_batches)
        }
    }
}

fn get_used_fields(fields: &[Field], schema: &TableSchemaRef) -> Result<Vec<Field>> {
    let mut read_fields = Vec::with_capacity(fields.len());
    for f in schema.fields().iter() {
        if let Some(m) = fields
            .iter()
            .filter(|c| c.name.eq_ignore_ascii_case(f.name()))
            .last()
        {
            read_fields.push(m.clone());
        } else {
            return Err(ErrorCode::TableSchemaMismatch(format!(
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

fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Vec<&'a ColumnChunkMetaData> {
    columns
        .iter()
        .filter(|x| x.descriptor().path_in_schema[0] == field_name)
        .collect()
}

#[async_backtrace::framed]
async fn read_single_column_async(reader: Reader, meta: &ColumnChunkMetaData) -> Result<Vec<u8>> {
    let (start, len) = meta.byte_range();
    let buf = reader.read(start..start + len).await?;
    Ok(buf.to_vec())
}
