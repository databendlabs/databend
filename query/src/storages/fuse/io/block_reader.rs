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
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageIterator;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;
use common_tracing::tracing;
use futures::AsyncReadExt;
use opendal::Operator;

use crate::storages::fuse::fuse_part::ColumnMeta;
use crate::storages::fuse::fuse_part::FusePartInfo;

#[derive(Clone)]
pub struct BlockReader {
    operator: Operator,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    parquet_schema_descriptor: SchemaDescriptor,
}

impl BlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Vec<usize>,
    ) -> Result<Arc<BlockReader>> {
        let projected_schema = DataSchemaRef::new(schema.project(projection.clone()));

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;
        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            arrow_schema: Arc::new(arrow_schema),
        }))
    }

    fn to_deserialize(
        meta: &ColumnMeta,
        chunk: Vec<u8>,
        rows: usize,
        descriptor: &ColumnDescriptor,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let pages = PageIterator::new(
            std::io::Cursor::new(chunk),
            meta.num_values as i64,
            Compression::Lz4,
            descriptor.clone(),
            Arc::new(|_, _| true),
            vec![],
        );

        let descriptor_type = descriptor.type_();
        let decompressor = BasicDecompressor::new(pages, vec![]);
        Ok(column_iter_to_arrays(
            vec![decompressor],
            vec![descriptor_type],
            field,
            rows,
        )?)
    }

    async fn read_columns(self, part: PartInfoPtr) -> Result<(usize, Vec<ArrayIter<'static>>)> {
        let part = FusePartInfo::from_part(&part)?;

        let rows = part.nums_rows;
        // TODO: add prefetch column data.
        let mut columns_array_iter = Vec::with_capacity(0);
        for index in &self.projection {
            let column_meta = &part.columns_meta[index];

            let mut column_reader = self
                .operator
                .object(&part.location)
                .range_reader(column_meta.offset, column_meta.length);

            let mut column_chunk = vec![0; column_meta.length as usize];
            // TODO: check thread name
            column_reader.read_exact(&mut column_chunk).await?;

            let field = self.arrow_schema.fields[*index].clone();
            let column_descriptor = self.parquet_schema_descriptor.column(*index);
            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                column_chunk,
                rows,
                column_descriptor,
                field,
            )?);
        }

        Ok((rows, columns_array_iter))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self, part: PartInfoPtr) -> Result<DataBlock> {
        let this = self.clone();
        let (num_rows, columns_array_iter) = this.read_columns(part).await?;

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);

        match deserializer.next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.projected_schema, &chunk),
        }
    }
}
