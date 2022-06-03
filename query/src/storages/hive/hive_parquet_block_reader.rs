// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageReader;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;

use crate::catalogs::hive::HivePartInfo;

#[derive(Clone)]
pub struct HiveParquetBlockReader {
    operator: Operator,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    parquet_schema_descriptor: SchemaDescriptor,
}

impl HiveParquetBlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Vec<usize>,
    ) -> Result<Arc<HiveParquetBlockReader>> {
        let projected_schema = DataSchemaRef::new(schema.project(projection.clone()));

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;

        Ok(Arc::new(HiveParquetBlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            arrow_schema: Arc::new(arrow_schema),
        }))
    }

    fn to_deserialize(
        column_meta: &ColumnChunkMetaData,
        chunk: Vec<u8>,
        rows: usize,
        column_descriptor: &ColumnDescriptor,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let pages = PageReader::new(
            std::io::Cursor::new(chunk),
            column_meta,
            Arc::new(|_, _| true),
            vec![],
        );

        let primitive_type = &column_descriptor.descriptor.primitive_type;
        let decompressor = BasicDecompressor::new(pages, vec![]);
        Ok(column_iter_to_arrays(
            vec![decompressor],
            vec![primitive_type],
            field,
            rows,
        )?)
    }

    fn get_parquet_column_metadata<'a>(
        row_group: &'a RowGroupMetaData,
        field_name: &str,
    ) -> Result<&'a ColumnChunkMetaData> {
        let column_meta: Vec<&ColumnChunkMetaData> = row_group
            .columns()
            .iter()
            .filter(|x| x.descriptor().path_in_schema[0] == field_name)
            .collect();
        if column_meta.is_empty() {
            return Err(ErrorCode::ParquetError(format!(
                "couldn't find column:{} in parquet file",
                field_name
            )));
        } else if column_meta.len() > 1 {
            return Err(ErrorCode::ParquetError(format!(
                "find multi column:{} in parquet file",
                field_name
            )));
        }
        Ok(column_meta[0])
    }

    async fn read_columns(&self, part: PartInfoPtr) -> Result<(usize, Vec<ArrayIter<'static>>)> {
        let part = HivePartInfo::from_part(&part)?;

        let object = self.operator.object(&part.location);
        let mut reader = object.seekable_reader(0..);
        let meta = read_metadata_async(&mut reader).await?;

        if meta.row_groups.is_empty() {
            return Err(ErrorCode::ParquetError(format!(
                "no rowgroup in parquet file: {}",
                part.location
            )));
        }

        // todo: support more than one rowgroup
        // todo: support predict push down
        let row_group = &meta.row_groups[0];

        //todo : if size of rowgroup is small, there is no need to parallel read
        let num_cols = self.projection.len();
        let mut column_chunk_futs = Vec::with_capacity(num_cols);
        for idx in &self.projection {
            let field = &self.arrow_schema.fields[*idx];
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;
            let (start, len) = column_meta.byte_range();
            let column_reader = self.operator.object(&part.location);
            let fut = async move {
                // NOTE: move chunk inside future so that alloc only
                // happen when future is ready to go.
                let column_chunk = column_reader.range_read(start..start + len).await?;
                Ok::<_, ErrorCode>(column_chunk)
            }
            .instrument(debug_span!("read_col_chunk"));
            column_chunk_futs.push(fut);
        }
        let chunks = futures::stream::iter(column_chunk_futs)
            .buffered(std::cmp::min(10, num_cols))
            .try_collect::<Vec<_>>()
            .await?;

        let rows = row_group.num_rows();
        let mut columns_array_iter = Vec::with_capacity(num_cols);
        for (i, columns_chunk) in chunks.into_iter().enumerate() {
            let idx = self.projection[i];
            let field = self.arrow_schema.fields[idx].clone();
            let column_descriptor = &self.parquet_schema_descriptor.columns()[idx];
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;
            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                columns_chunk,
                rows,
                column_descriptor,
                field,
            )?);
        }

        Ok((rows, columns_array_iter))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self, part: PartInfoPtr) -> Result<DataBlock> {
        let (num_rows, columns_array_iter) = self.read_columns(part).await?;
        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);
        self.try_next_block(&mut deserializer)
    }

    fn try_next_block(&self, deserializer: &mut RowGroupDeserializer) -> Result<DataBlock> {
        match deserializer.next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.projected_schema, &chunk),
        }
    }
}
