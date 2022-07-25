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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::compression::Compression as ParquetCompression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Compression;
use common_planners::PartInfoPtr;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::warn;
use common_tracing::tracing::Instrument;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Object;
use opendal::Operator;

use crate::storages::fuse::fuse_part::build_column_leaves;
use crate::storages::fuse::fuse_part::ColumnLeaf;
use crate::storages::fuse::fuse_part::ColumnMeta;
use crate::storages::fuse::fuse_part::FusePartInfo;
use crate::storages::fuse::io::retry;
use crate::storages::fuse::io::retry::Retryable;

#[derive(Clone)]
pub struct BlockReader {
    operator: Operator,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    column_leaves: Vec<ColumnLeaf>,
    parquet_schema_descriptor: SchemaDescriptor,
}

impl BlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Vec<usize>,
    ) -> Result<Arc<BlockReader>> {
        let projected_schema = DataSchemaRef::new(schema.project(&projection));

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;
        let column_leaves = build_column_leaves(&parquet_schema_descriptor);

        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_leaves,
            arrow_schema: Arc::new(arrow_schema),
        }))
    }

    fn to_array_iter(
        metas: Vec<&ColumnMeta>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        column_descriptors: Vec<&ColumnDescriptor>,
        field: Field,
        compression: &Compression,
    ) -> Result<ArrayIter<'static>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: Self::to_parquet_compression(compression),
                    descriptor: column_descriptor.descriptor.clone(),
                };
                let pages = PageReader::new_with_page_meta(
                    std::io::Cursor::new(chunk),
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                );
                BasicDecompressor::new(pages, vec![])
            })
            .collect::<Vec<_>>();

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.descriptor.primitive_type)
            .collect::<Vec<_>>();

        Ok(column_iter_to_arrays(columns, types, field, Some(rows))?)
    }

    // TODO refine these

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_with_block_meta(&self, meta: &BlockMeta) -> Result<DataBlock> {
        let (num_rows, columns_array_iter) = self.read_columns_with_block_meta(meta).await?;
        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);
        self.try_next_block(&mut deserializer)
    }
    // TODO refine these

    pub async fn read_columns_with_block_meta(
        &self,
        meta: &BlockMeta,
    ) -> Result<(usize, Vec<ArrayIter<'static>>)> {
        let num_rows = meta.row_count as usize;
        let num_cols = self.projection.len();
        let mut column_chunk_futs = Vec::with_capacity(num_cols);

        let mut columns_meta: HashMap<usize, ColumnMeta> =
            HashMap::with_capacity(meta.col_metas.len());
        for proj in &self.projection {
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;
            for index in indices {
                let column_meta = &meta.col_metas[&(*index as u32)];

                let column_reader = self.operator.object(&meta.location.0);
                let fut = async move {
                    let column_chunk = column_reader
                        .range_read(column_meta.offset..column_meta.offset + column_meta.len)
                        .await?;
                    Ok::<_, ErrorCode>((*index, column_chunk))
                }
                .instrument(debug_span!("read_col_chunk"));
                column_chunk_futs.push(fut);

                columns_meta.insert(
                    *index,
                    ColumnMeta::create(column_meta.offset, column_meta.len, column_meta.num_values),
                );
            }
        }

        let num_cols = columns_meta.len();
        let chunks = futures::stream::iter(column_chunk_futs)
            .buffered(std::cmp::min(10, num_cols))
            .try_collect::<Vec<_>>()
            .await?;
        let mut chunk_map: HashMap<usize, Vec<u8>> = chunks.into_iter().collect();

        let mut columns_array_iter = Vec::with_capacity(num_cols);
        for proj in &self.projection {
            let field = self.arrow_schema.fields[*proj].clone();
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;

            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            for index in indices {
                let column_meta = &columns_meta[index];
                let column_chunk = chunk_map.remove(index).unwrap();
                let column_descriptor = &self.parquet_schema_descriptor.columns()[*index];
                column_metas.push(column_meta);
                column_chunks.push(column_chunk);
                column_descriptors.push(column_descriptor);
            }
            columns_array_iter.push(Self::to_array_iter(
                column_metas,
                column_chunks,
                num_rows,
                column_descriptors,
                field,
                &meta.compression(),
            )?);
        }

        Ok((num_rows, columns_array_iter))
    }

    async fn read_columns(&self, part: PartInfoPtr) -> Result<(usize, Vec<ArrayIter<'static>>)> {
        let part = FusePartInfo::from_part(&part)?;

        // TODO: add prefetch column data.
        let num_rows = part.nums_rows;
        let num_cols = self.projection.len();
        let mut column_chunk_futs = Vec::with_capacity(num_cols);
        for proj in &self.projection {
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;
            for index in indices {
                let column_meta = &part.columns_meta[index];
                let column_reader = self.operator.object(&part.location);
                let fut = async move {
                    let (idx, column_chunk) = Self::read_column(
                        column_reader,
                        *index,
                        column_meta.offset,
                        column_meta.length,
                    )
                    .await?;
                    Ok::<_, ErrorCode>((idx, column_chunk))
                }
                .instrument(debug_span!("read_col_chunk"));
                column_chunk_futs.push(fut);
            }
        }

        let num_cols = column_chunk_futs.len();
        let chunks = futures::stream::iter(column_chunk_futs)
            .buffered(std::cmp::min(10, num_cols))
            .try_collect::<Vec<_>>()
            .await?;
        let mut chunk_map: HashMap<usize, Vec<u8>> = chunks.into_iter().collect();

        let mut columns_array_iter = Vec::with_capacity(num_cols);
        for proj in &self.projection {
            let field = self.arrow_schema.fields[*proj].clone();
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;

            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            for index in indices {
                let column_meta = &part.columns_meta[index];
                let column_chunk = chunk_map.remove(index).unwrap();
                let column_descriptor = &self.parquet_schema_descriptor.columns()[*index];
                column_metas.push(column_meta);
                column_chunks.push(column_chunk);
                column_descriptors.push(column_descriptor);
            }
            columns_array_iter.push(Self::to_array_iter(
                column_metas,
                column_chunks,
                num_rows,
                column_descriptors,
                field,
                &part.compression,
            )?);
        }

        Ok((num_rows, columns_array_iter))
    }

    pub fn deserialize(
        &self,
        part: PartInfoPtr,
        chunks: Vec<(usize, Vec<u8>)>,
    ) -> Result<DataBlock> {
        let part = FusePartInfo::from_part(&part)?;
        let mut chunk_map: HashMap<usize, Vec<u8>> = chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let num_rows = part.nums_rows;
        for proj in &self.projection {
            let field = self.arrow_schema.fields[*proj].clone();
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;

            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            for index in indices {
                let column_meta = &part.columns_meta[index];
                let column_chunk = chunk_map.remove(index).unwrap();
                let column_descriptor = &self.parquet_schema_descriptor.columns()[*index];
                column_metas.push(column_meta);
                column_chunks.push(column_chunk);
                column_descriptors.push(column_descriptor);
            }
            columns_array_iter.push(Self::to_array_iter(
                column_metas,
                column_chunks,
                num_rows,
                column_descriptors,
                field,
                &part.compression,
            )?);
        }

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);

        self.try_next_block(&mut deserializer)
    }

    pub async fn read_columns_data(&self, part: PartInfoPtr) -> Result<Vec<(usize, Vec<u8>)>> {
        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.projection.len());

        for proj in &self.projection {
            let column_leaf = &self.column_leaves[*proj];
            let indices = &column_leaf.leaf_ids;

            for index in indices {
                let column_meta = &part.columns_meta[index];

                join_handlers.push(Self::read_column(
                    self.operator.object(&part.location),
                    *index,
                    column_meta.offset,
                    column_meta.length,
                ));
            }
        }

        futures::future::try_join_all(join_handlers).await
    }

    pub async fn read_column(
        o: Object,
        index: usize,
        offset: u64,
        length: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let handler = common_base::base::tokio::spawn(async move {
            let op = || async {
                let mut chunk = vec![0; length as usize];
                // Sine error conversion DO matters: retry depends on the conversion
                // to distinguish transient errors from permanent ones.
                // Explict error conversfeat-bloom-switch-to-external-indexion is used here, to make the code easy to be followed
                let mut r = o
                    .range_reader(offset..offset + length)
                    .await
                    .map_err(retry::from_io_error)?;
                r.read_exact(&mut chunk).await?;
                Ok(chunk)
            };

            let notify = |e: std::io::Error, duration| {
                warn!(
                    "transient error encountered while reading column, at duration {:?} : {}",
                    duration, e,
                )
            };

            let chunk = op.retry_with_notify(notify).await?;
            Ok((index, chunk))
        });

        match handler.await {
            Ok(Ok((index, data))) => Ok((index, data)),
            Ok(Err(cause)) => Err(cause),
            Err(cause) => Err(ErrorCode::TokioError(format!(
                "Cannot join future {:?}",
                cause
            ))),
        }
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

    fn to_parquet_compression(meta_compression: &Compression) -> ParquetCompression {
        match meta_compression {
            Compression::Lz4 => ParquetCompression::Lz4,
            Compression::Lz4Raw => ParquetCompression::Lz4Raw,
        }
    }
}
