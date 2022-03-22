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
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;
use opendal::Reader;

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
        let num_cols = self.projection.len();
        let mut column_chunk_futs = Vec::with_capacity(num_cols);
        let mut col_idx = Vec::with_capacity(num_cols);
        for index in &self.projection {
            let column_meta = &part.columns_meta[index];
            let mut column_reader = self
                .operator
                .object(&part.location)
                .range_reader(column_meta.offset, column_meta.length);
            let fut = async move {
                // NOTE: move chunk inside future so that alloc only
                // happen when future is ready to go.
                let mut column_chunk = vec![0; column_meta.length as usize];
                column_reader.read_exact(&mut column_chunk).await?;
                Ok::<_, ErrorCode>(column_chunk)
            }
            .instrument(debug_span!("read_col_chunk"));
            column_chunk_futs.push(fut);
            col_idx.push(index);
        }

        let chunks = futures::stream::iter(column_chunk_futs)
            .buffered(std::cmp::min(10, num_cols))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?;

        let mut columns_array_iter = Vec::with_capacity(num_cols);
        for (i, column_chunk) in chunks.into_iter().enumerate() {
            let idx = *col_idx[i];
            let field = self.arrow_schema.fields[idx].clone();
            let column_descriptor = self.parquet_schema_descriptor.column(idx);
            let column_meta = &part.columns_meta[&idx];
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

    pub fn deserialize(&self, part: PartInfoPtr, chunks: Vec<Vec<u8>>) -> Result<DataBlock> {
        if self.projection.len() != chunks.len() {
            return Err(ErrorCode::LogicalError(
                "Columns chunk len must be equals projections len.",
            ));
        }

        let part = FusePartInfo::from_part(&part)?;
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let num_rows = part.nums_rows;
        for (index, column_chunk) in chunks.into_iter().enumerate() {
            let index = self.projection[index];
            let field = self.arrow_schema.fields[index].clone();
            let column_descriptor = self.parquet_schema_descriptor.column(index);
            let column_meta = &part.columns_meta[&index];
            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                column_chunk,
                num_rows,
                column_descriptor,
                field,
            )?);
        }

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);

        match deserializer.next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.projected_schema, &chunk),
        }
    }

    pub async fn read_columns_data(&self, part: PartInfoPtr) -> Result<Vec<Vec<u8>>> {
        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.projection.len());

        for index in &self.projection {
            let column_meta = &part.columns_meta[index];

            let column_reader = self
                .operator
                .object(&part.location)
                .range_reader(column_meta.offset, column_meta.length);

            let column_chunk = vec![0; column_meta.length as usize];
            join_handlers.push(Self::read_column(column_reader, column_chunk));
        }

        futures::future::try_join_all(join_handlers).await
    }

    async fn read_column(mut column_reader: Reader, mut chunk: Vec<u8>) -> Result<Vec<u8>> {
        let handler = common_base::tokio::spawn(async move {
            tracing::debug!("read_exact | Begin, {:?}", std::thread::current());
            column_reader.read_exact(&mut chunk).await?;
            tracing::debug!("read_exact | End, {:?}", std::thread::current());
            Result::Ok(chunk)
        });

        match handler.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(cause)) => Err(cause),
            Err(cause) => Err(ErrorCode::TokioError(format!(
                "Cannot join future {:?}",
                cause
            ))),
        }
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
