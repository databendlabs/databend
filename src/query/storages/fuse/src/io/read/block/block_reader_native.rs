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

use std::io::BufReader;
use std::ops::Range;
use std::time::Instant;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read::ColumnDescriptor;
use common_arrow::native::read::reader::NativeReader;
use common_arrow::native::read::NativeReadBuf;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Value;
use opendal::Object;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::metrics::metrics_inc_remote_io_read_bytes;
use crate::metrics::metrics_inc_remote_io_read_milliseconds;
use crate::metrics::metrics_inc_remote_io_read_parts;
use crate::metrics::metrics_inc_remote_io_seeks;

// Native storage format

pub trait NativeReaderExt: NativeReadBuf + std::io::Seek + Send + Sync {}
impl<T: NativeReadBuf + std::io::Seek + Send + Sync> NativeReaderExt for T {}

pub type Reader = Box<dyn NativeReaderExt>;

impl BlockReader {
    pub async fn async_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, NativeReader<Reader>)>> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.project_indices.len());

        for (index, column_node) in self.project_column_nodes.iter().enumerate() {
            let leaves: Vec<ColumnDescriptor> = column_node
                .leaf_ids
                .iter()
                .map(|index| self.parquet_schema_descriptor.columns()[*index].clone())
                .collect::<Vec<_>>();
            let metas: Vec<ColumnMeta> = column_node
                .leaf_ids
                .iter()
                .map(|index| part.columns_meta[index].clone())
                .collect::<Vec<_>>();

            join_handlers.push(Self::read_native_columns_data(
                self.operator.object(&part.location),
                index,
                leaves,
                metas,
                &part.range,
                column_node.field.clone(),
            ));

            // Perf
            {
                let total_len = column_node
                    .leaf_ids
                    .iter()
                    .map(|index| {
                        let (_, len) = part.columns_meta[index].offset_length();
                        len
                    })
                    .sum();
                metrics_inc_remote_io_seeks(column_node.leaf_ids.len() as u64);
                metrics_inc_remote_io_read_bytes(total_len);
            }
        }

        let start = Instant::now();
        let res = futures::future::try_join_all(join_handlers).await;

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        res
    }

    pub async fn read_native_columns_data(
        o: Object,
        index: usize,
        leaves: Vec<ColumnDescriptor>,
        metas: Vec<ColumnMeta>,
        range: &Option<Range<usize>>,
        field: ArrowField,
    ) -> Result<(usize, NativeReader<Reader>)> {
        use backon::ExponentialBackoff;
        use backon::Retryable;

        let mut readers = Vec::with_capacity(metas.len());
        let mut scratchs = Vec::with_capacity(metas.len());
        let mut native_metas = Vec::with_capacity(metas.len());
        for meta in metas {
            let (offset, length) = meta.offset_length();
            let mut native_meta = meta.as_native().unwrap().clone();

            if let Some(range) = range {
                native_meta = native_meta.slice(range.start, range.end);
            }
            native_metas.push(native_meta);

            let reader = { || async { o.range_read(offset..offset + length).await } }
                .retry(ExponentialBackoff::default())
                .when(|err| err.is_temporary())
                .await?;

            let reader: Reader = Box::new(std::io::Cursor::new(reader));
            readers.push(reader);
            scratchs.push(Vec::with_capacity(8 * 1024));
        }
        let fuse_reader = NativeReader::new(readers, field, leaves, native_metas, scratchs);
        Ok((index, fuse_reader))
    }

    pub fn sync_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, NativeReader<Reader>)>> {
        let part = FusePartInfo::from_part(&part)?;

        let mut results = Vec::with_capacity(self.project_column_nodes.len());
        for (index, column_node) in self.project_column_nodes.iter().enumerate() {
            let op = self.operator.clone();
            let location = part.location.clone();
            let leaves: Vec<ColumnDescriptor> = column_node
                .leaf_ids
                .iter()
                .map(|index| self.parquet_schema_descriptor.columns()[*index].clone())
                .collect::<Vec<_>>();
            let metas: Vec<ColumnMeta> = column_node
                .leaf_ids
                .iter()
                .map(|index| part.columns_meta[index].clone())
                .collect::<Vec<_>>();

            let result = Self::sync_read_native_column(
                op.object(&location),
                index,
                leaves,
                metas,
                &part.range,
                column_node.field.clone(),
            );
            results.push(result?);
        }

        Ok(results)
    }

    pub fn sync_read_native_column(
        o: Object,
        index: usize,
        leaves: Vec<ColumnDescriptor>,
        metas: Vec<ColumnMeta>,
        range: &Option<Range<usize>>,
        field: ArrowField,
    ) -> Result<(usize, NativeReader<Reader>)> {
        let mut readers = Vec::with_capacity(metas.len());
        let mut scratchs = Vec::with_capacity(metas.len());
        let mut native_metas = Vec::with_capacity(metas.len());
        for meta in metas {
            let mut native_meta = meta.as_native().unwrap().clone();
            if let Some(range) = range {
                native_meta = native_meta.slice(range.start, range.end);
            }
            let (offset, length) = (
                native_meta.offset,
                native_meta.pages.iter().map(|p| p.length).sum::<u64>(),
            );
            native_metas.push(native_meta);

            let reader = o.blocking_range_reader(offset..offset + length)?;
            let reader: Reader = Box::new(BufReader::new(reader));
            readers.push(reader);
            scratchs.push(Vec::with_capacity(8 * 1024));
        }
        let fuse_reader = NativeReader::new(readers, field, leaves, native_metas, scratchs);
        Ok((index, fuse_reader))
    }

    pub fn build_block(&self, chunks: Vec<(usize, Box<dyn Array>)>) -> Result<DataBlock> {
        let mut rows = 0;
        let mut entries = Vec::with_capacity(chunks.len());
        for (index, _column_node) in self.project_column_nodes.iter().enumerate() {
            if let Some(array) = chunks.iter().find(|c| c.0 == index).map(|c| c.1.clone()) {
                let data_type: DataType = self.projected_schema.field(index).data_type().into();
                entries.push(BlockEntry {
                    data_type: data_type.clone(),
                    value: Value::Column(Column::from_arrow(array.as_ref(), &data_type)),
                });
                rows = array.len();
            }
        }
        Ok(DataBlock::new(entries, rows))
    }
}
