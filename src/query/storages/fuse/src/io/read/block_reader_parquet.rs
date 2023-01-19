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
use std::time::Instant;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::parquet::compression::Compression as ParquetCompression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Compression;

use crate::fuse_part::FusePartInfo;
use crate::io::read::decompressor::BuffedBasicDecompressor;
use crate::io::read::ReadSettings;
use crate::io::BlockReader;
use crate::io::UncompressedBuffer;
use crate::metrics::metrics_inc_remote_io_deserialize_milliseconds;

impl BlockReader {
    /// Read a parquet file and convert to DataBlock.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_parquet_by_meta(
        &self,
        settings: &ReadSettings,
        meta: &BlockMeta,
    ) -> Result<DataBlock> {
        //  Build columns meta.
        let columns_meta = meta
            .col_metas
            .iter()
            .map(|(index, meta)| (*index as usize, meta.clone()))
            .collect::<HashMap<_, _>>();

        // Get the merged IO read result.
        let read_res = self
            .read_columns_data_by_merge_io(settings, &meta.location.0, &columns_meta)
            .await?;

        // Get the columns chunk.
        let chunks = read_res
            .columns_chunks()?
            .into_iter()
            .map(|(column_idx, column_chunk)| (column_idx, column_chunk.to_vec()))
            .collect::<Vec<_>>();

        let num_rows = meta.row_count as usize;
        let columns_chunk = chunks
            .iter()
            .map(|(index, chunk)| (*index, chunk.as_slice()))
            .collect::<Vec<_>>();

        self.deserialize_parquet_chunks_with_buffer(
            num_rows,
            &meta.compression,
            &columns_meta,
            columns_chunk,
            None,
        )
    }

    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_parquet_chunks(
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
        let deserialized_res = self.deserialize_parquet_chunks_with_buffer(
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            reads,
            None,
        );

        // Perf.
        {
            metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        }

        deserialized_res
    }

    /// Deserialize column chunks data from parquet format to DataBlock with a uncompressed buffer.
    pub fn deserialize_parquet_chunks_with_buffer(
        &self,
        num_rows: usize,
        compression: &Compression,
        columns_meta: &HashMap<usize, ColumnMeta>,
        columns_chunks: Vec<(usize, &[u8])>,
        uncompressed_buffer: Option<Arc<UncompressedBuffer>>,
    ) -> Result<DataBlock> {
        let chunk_map: HashMap<usize, &[u8]> = columns_chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let columns = self.projection.project_column_nodes(&self.column_nodes)?;

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

            columns_array_iter.push(Self::chunks_to_parquet_array_iter(
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
        DataBlock::from_arrow_chunk(&chunk, &self.data_schema())
    }

    fn chunks_to_parquet_array_iter<'a>(
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
                let meta = meta.as_parquet().unwrap();

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
}
