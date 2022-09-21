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
use common_arrow::arrow::io::parquet::read;
use common_arrow::arrow::io::parquet::read::read_columns;
use common_arrow::arrow::io::parquet::read::to_deserializer;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::read_metadata;
use common_datablocks::DataBlock;
use common_datavalues::remove_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use opendal::Object;
use similar_asserts::traits::MakeDiff;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_format::FileInfo;
use crate::processors::sources::input_formats::input_format::InputData;
use crate::processors::sources::input_formats::input_format::SplitInfo;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::InputFormat;

pub struct InputFormatParquet;

#[async_trait::async_trait]
impl InputFormat for InputFormatParquet {
    fn default_record_delimiter(&self) -> RecordDelimiter {
        RecordDelimiter::Crlf
    }

    fn default_field_delimiter(&self) -> u8 {
        b'_'
    }

    async fn read_file_meta(
        &self,
        _obj: &Object,
        _size: usize,
    ) -> Result<Option<Arc<dyn InputData>>> {
        // todo(youngsofun): execute_copy_aligned
        Ok(None)
    }

    async fn read_split_meta(
        &self,
        _obj: &Object,
        _split_info: &SplitInfo,
    ) -> Result<Option<Box<dyn InputData>>> {
        Ok(None)
    }

    fn split_files(&self, file_infos: Vec<FileInfo>, _split_size: usize) -> Vec<SplitInfo> {
        file_infos
            .into_iter()
            .map(SplitInfo::from_file_info)
            .collect()
    }

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        // todo(youngsofun): execute_copy_aligned
        ParquetFormatPipe::execute_copy_with_aligner(ctx, pipeline)
    }

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        ParquetFormatPipe::execute_stream(ctx, pipeline)
    }
}

pub struct ParquetFormatPipe;

#[async_trait::async_trait]
impl InputFormatPipe for ParquetFormatPipe {
    type ReadBatch = ReadBatch;
    type RowBatch = RowGroupInMemory;
    type AligningState = AligningState;
    type BlockBuilder = ParquetBlockBuilder;
}

pub struct RowGroupInMemory {
    pub meta: RowGroupMetaData,
    pub fields: Arc<Vec<Field>>,
    pub field_meta_indexes: Vec<Vec<usize>>,
    pub field_arrays: Vec<Vec<Vec<u8>>>,
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
            fields,
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
                self.fields[f].clone(),
                self.meta.num_rows() as usize,
                None,
                None,
            )?;
            column_chunks.push(array_iters);
        }
        match RowGroupDeserializer::new(column_chunks, self.meta.num_rows(), None).next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Ok(chunk)) => Ok(chunk),
            Some(Err(e)) => Err(ErrorCode::ParquetError(e.to_string())),
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
            Ok(vec![block])
        } else {
            Ok(vec![])
        }
    }
}

pub struct AligningState {
    ctx: Arc<InputContext>,
    split_info: SplitInfo,
    buffers: Vec<Vec<u8>>,
}

impl AligningStateTrait for AligningState {
    type Pipe = ParquetFormatPipe;

    fn try_create(ctx: &Arc<InputContext>, split_info: &SplitInfo) -> Result<Self> {
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
            };
            Ok(vec![])
        } else {
            let file_in_memory = self.buffers.concat();
            let size = file_in_memory.len();
            tracing::debug!(
                "aligning parquet file {} of {} bytes",
                self.split_info.file_info.path,
                size,
            );
            let mut cursor = Cursor::new(file_in_memory);
            let file_meta =
                read_metadata(&mut cursor).map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
            let read_fields = Arc::new(get_fields(&file_meta, &self.ctx.schema)?);

            let mut row_batches = Vec::with_capacity(file_meta.row_groups.len());
            for row_group in file_meta.row_groups.into_iter() {
                row_batches.push(RowGroupInMemory::read(
                    &mut cursor,
                    row_group,
                    read_fields.clone(),
                )?)
            }
            tracing::info!(
                "align parquet file {} of {} bytes to {} row groups",
                self.split_info.file_info.path,
                size,
                row_batches.len()
            );
            Ok(row_batches)
        }
    }
}

fn get_fields(file_meta: &FileMetaData, schema: &DataSchemaRef) -> Result<Vec<Field>> {
    let infer_schema = read::infer_schema(file_meta)?;
    let mut read_fields = Vec::with_capacity(schema.num_fields());
    for f in schema.fields().iter() {
        if let Some(m) = infer_schema
            .fields
            .iter()
            .filter(|c| c.name.eq_ignore_ascii_case(f.name()))
            .last()
        {
            let tf = DataField::from(m);
            if remove_nullable(tf.data_type()) != remove_nullable(f.data_type()) {
                let pair = (f, m);
                let diff = pair.make_diff("expected_field", "infer_field");
                return Err(ErrorCode::ParquetError(format!(
                    "parquet schema mismatch, differ: {}",
                    diff
                )));
            }

            read_fields.push(m.clone());
        } else {
            return Err(ErrorCode::ParquetError(format!(
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
