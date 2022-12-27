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

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::compression::Compression as ParquetCompression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaf;
use common_storage::ColumnLeaves;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ColumnMeta;
use common_storages_table_meta::meta::Compression;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Operator;
use tracing::debug_span;
use tracing::Instrument;

use crate::fuse_part::FusePartInfo;
use crate::io::read::block_reader::MergeIOReadResult;
use crate::io::read::decompressor::BuffedBasicDecompressor;
use crate::io::read::ReadSettings;
use crate::io::BlockReader;
use crate::io::UncompressedBuffer;
use crate::metrics::metrics_inc_remote_io_deserialize_milliseconds;
use crate::metrics::metrics_inc_remote_io_read_bytes;
use crate::metrics::metrics_inc_remote_io_read_parts;
use crate::metrics::metrics_inc_remote_io_seeks;

impl BlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Projection,
    ) -> Result<Arc<BlockReader>> {
        let projected_schema = match projection {
            Projection::Columns(ref indices) => DataSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                DataSchemaRef::new(schema.inner_project(path_indices))
            }
        };

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_leaves,
        }))
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    fn to_array_iter<'a>(
        metas: Vec<&ColumnMeta>,
        chunks: Vec<&'a [u8]>,
        rows: usize,
        column_descriptors: Vec<&ColumnDescriptor>,
        field: Field,
        compression: &Compression,
        uncompressed_buffer: Arc<UncompressedBuffer>,
    ) -> Result<ArrayIter<'a>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: Self::to_parquet_compression(compression)?,
                    descriptor: column_descriptor.descriptor.clone(),
                };
                let pages = PageReader::new_with_page_meta(
                    chunk,
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );

                Ok(BuffedBasicDecompressor::new(
                    pages,
                    uncompressed_buffer.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.descriptor.primitive_type)
            .collect::<Vec<_>>();

        Ok(column_iter_to_arrays(
            columns,
            types,
            field,
            Some(rows),
            rows,
        )?)
    }

    async fn read_cols_by_block_meta(&self, meta: &BlockMeta) -> Result<Vec<(usize, Vec<u8>)>> {
        let num_cols = self.projection.len();
        let mut column_chunk_futs = Vec::with_capacity(num_cols);

        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);
        for (index, _) in indices {
            let column_meta = &meta.col_metas[&(index as u32)];
            let column_reader = self.operator.object(&meta.location.0);
            let fut = async move {
                let column_chunk = column_reader
                    .range_read(column_meta.offset..column_meta.offset + column_meta.len)
                    .await?;
                Ok::<_, ErrorCode>((index, column_chunk))
            }
            .instrument(debug_span!("read_col_chunk"));
            column_chunk_futs.push(fut);
        }

        let num_cols = column_chunk_futs.len();

        futures::stream::iter(column_chunk_futs)
            .buffered(std::cmp::min(10, num_cols))
            .try_collect::<Vec<_>>()
            .await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_with_block_meta(&self, meta: &BlockMeta) -> Result<DataBlock> {
        let chunks = self.read_cols_by_block_meta(meta).await?;

        let num_rows = meta.row_count as usize;
        let columns_meta = meta
            .col_metas
            .iter()
            .map(|(index, meta)| (*index as usize, meta.clone()))
            .collect::<HashMap<_, _>>();

        let columns_chunk = chunks
            .iter()
            .map(|(index, chunk)| (*index, chunk.as_slice()))
            .collect::<Vec<_>>();

        self.deserialize_columns(
            num_rows,
            &meta.compression,
            &columns_meta,
            columns_chunk,
            None,
        )
    }

    pub fn build_block(&self, chunks: Vec<(usize, Box<dyn Array>)>) -> Result<DataBlock> {
        let mut results = Vec::with_capacity(chunks.len());
        let mut chunk_map: HashMap<usize, Box<dyn Array>> = chunks.into_iter().collect();
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        for column in &columns {
            let indices = &column.leaf_ids;

            for index in indices {
                if let Some(array) = chunk_map.remove(index) {
                    results.push(array);
                    break;
                }
            }
        }
        let chunk = Chunk::new(results);
        DataBlock::from_chunk(&self.schema(), &chunk)
    }

    pub fn deserialize_columns(
        &self,
        num_rows: usize,
        compression: &Compression,
        columns_meta: &HashMap<usize, ColumnMeta>,
        columns_chunks: Vec<(usize, &[u8])>,
        uncompressed_buffer: Option<Arc<UncompressedBuffer>>,
    ) -> Result<DataBlock> {
        let chunk_map: HashMap<usize, &[u8]> = columns_chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let columns = self.projection.project_column_leaves(&self.column_leaves)?;

        for column in &columns {
            let field = column.field.clone();
            let indices = &column.leaf_ids;
            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            for index in indices {
                let column_read = <&[u8]>::clone(&chunk_map[index]);
                let column_meta = &columns_meta[index];
                let column_descriptor = &self.parquet_schema_descriptor.columns()[*index];
                column_metas.push(column_meta);
                column_chunks.push(column_read);
                column_descriptors.push(column_descriptor);
            }

            columns_array_iter.push(Self::to_array_iter(
                column_metas,
                column_chunks,
                num_rows,
                column_descriptors,
                field,
                compression,
                uncompressed_buffer
                    .clone()
                    .unwrap_or_else(|| UncompressedBuffer::new(0)),
            )?);
        }

        let mut arrays = Vec::with_capacity(columns_array_iter.len());
        for mut column_array_iter in columns_array_iter.into_iter() {
            let array = column_array_iter.next().unwrap()?;
            arrays.push(array);
            drop(column_array_iter);
        }

        let chunk = Chunk::try_new(arrays)?;
        DataBlock::from_chunk(&self.projected_schema, &chunk)
    }

    pub fn deserialize(
        &self,
        part: PartInfoPtr,
        chunks: Vec<(usize, Vec<u8>)>,
    ) -> Result<DataBlock> {
        let start = Instant::now();
        let reads = chunks
            .iter()
            .map(|(index, chunk)| (*index, chunk.as_slice()))
            .collect::<Vec<_>>();

        let part = FusePartInfo::from_part(&part)?;
        let deserialized_res = self.deserialize_columns(
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            reads,
            None,
        );
        metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        deserialized_res
    }

    pub async fn read_columns_data(
        &self,
        ctx: Arc<dyn TableContext>,
        raw_part: PartInfoPtr,
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        let read_res = self.read_columns_data_by_merge_io(ctx, raw_part).await?;

        Ok(read_res
            .columns_chunks()?
            .into_iter()
            .map(|(column_idx, column_chunk)| (column_idx, column_chunk.to_vec()))
            .collect::<Vec<_>>())
    }

    pub async fn read_columns_data_by_merge_io(
        &self,
        ctx: Arc<dyn TableContext>,
        raw_part: PartInfoPtr,
    ) -> Result<MergeIOReadResult> {
        // Perf
        metrics_inc_remote_io_read_parts(1);

        let part = FusePartInfo::from_part(&raw_part)?;
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);

        let mut ranges = vec![];
        for index in indices.keys() {
            let column_meta = &part.columns_meta[index];
            ranges.push((
                *index,
                column_meta.offset..(column_meta.offset + column_meta.len),
            ));

            // Perf
            metrics_inc_remote_io_seeks(1);
            metrics_inc_remote_io_read_bytes(column_meta.len);
        }

        let object = self.operator.object(&part.location);
        let read_settings = ReadSettings {
            storage_io_min_bytes_for_seek: ctx
                .get_settings()
                .get_storage_io_min_bytes_for_seek()?,
            storage_io_max_page_bytes_for_read: ctx
                .get_settings()
                .get_storage_io_max_page_bytes_for_read()?,
        };

        Self::merge_io_read(&read_settings, object, ranges).await
    }

    pub fn support_blocking_api(&self) -> bool {
        self.operator.metadata().can_blocking()
    }

    pub fn sync_read_columns_data(
        &self,
        ctx: &Arc<dyn TableContext>,
        part: PartInfoPtr,
    ) -> Result<MergeIOReadResult> {
        let part = FusePartInfo::from_part(&part)?;
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);

        let mut ranges = vec![];
        for index in indices.keys() {
            let column_meta = &part.columns_meta[index];
            ranges.push((
                *index,
                column_meta.offset..(column_meta.offset + column_meta.len),
            ));
        }

        let object = self.operator.object(&part.location);
        let read_settings = ReadSettings {
            storage_io_min_bytes_for_seek: ctx
                .get_settings()
                .get_storage_io_min_bytes_for_seek()?,
            storage_io_max_page_bytes_for_read: ctx
                .get_settings()
                .get_storage_io_max_page_bytes_for_read()?,
        };

        Self::sync_merge_io_read(&read_settings, object, ranges)
    }

    fn to_parquet_compression(meta_compression: &Compression) -> Result<ParquetCompression> {
        match meta_compression {
            Compression::Lz4 => {
                let err_msg = r#"Deprecated compression algorithm [Lz4] detected.

                                        The Legacy compression algorithm [Lz4] is no longer supported.
                                        To migrate data from old format, please consider re-create the table,
                                        by using an old compatible version [v0.8.25-nightly … v0.7.12-nightly].

                                        - Bring up the compatible version of databend-query
                                        - re-create the table
                                           Suppose the name of table is T
                                            ~~~
                                            create table tmp_t as select * from T;
                                            drop table T all;
                                            alter table tmp_t rename to T;
                                            ~~~
                                        Please note that the history of table T WILL BE LOST.
                                       "#;
                Err(ErrorCode::StorageOther(err_msg))
            }
            Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
            Compression::Snappy => Ok(ParquetCompression::Snappy),
            Compression::Zstd => Ok(ParquetCompression::Zstd),
            Compression::Gzip => Ok(ParquetCompression::Gzip),
            Compression::None => Ok(ParquetCompression::Uncompressed),
        }
    }

    // Build non duplicate leaf_ids to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(columns: &Vec<&ColumnLeaf>) -> HashMap<usize, Field> {
        let mut indices = HashMap::with_capacity(columns.len());
        for column in columns {
            for index in &column.leaf_ids {
                indices.insert(*index, column.field.clone());
            }
        }
        indices
    }
}
