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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::BufReader;
use std::ops::Range;
use std::time::Instant;

use common_arrow::arrow::array::Array;
use common_arrow::native::read::reader::NativeReader;
use common_arrow::native::read::NativeReadBuf;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::Value;
use opendal::Operator;
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
    #[async_backtrace::framed]
    pub async fn async_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<BTreeMap<usize, Vec<NativeReader<Reader>>>> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.project_column_nodes.len());

        for (index, column_node) in self.project_column_nodes.iter().enumerate() {
            let metas: Vec<ColumnMeta> = column_node
                .leaf_column_ids
                .iter()
                .filter_map(|column_id| part.columns_meta.get(column_id))
                .cloned()
                .collect::<Vec<_>>();

            join_handlers.push(Self::read_native_columns_data(
                self.operator.clone(),
                &part.location,
                index,
                metas,
                part.range(),
            ));

            // Perf
            {
                let total_len = column_node
                    .leaf_column_ids
                    .iter()
                    .map(|column_id| {
                        if let Some(meta) = part.columns_meta.get(column_id) {
                            let (_, len) = meta.offset_length();
                            len
                        } else {
                            0
                        }
                    })
                    .sum();
                metrics_inc_remote_io_seeks(column_node.leaf_column_ids.len() as u64);
                metrics_inc_remote_io_read_bytes(total_len);
            }
        }
        let start = Instant::now();
        let readers = futures::future::try_join_all(join_handlers).await?;
        let results: BTreeMap<usize, Vec<NativeReader<Reader>>> = readers.into_iter().collect();

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(results)
    }

    #[async_backtrace::framed]
    pub async fn read_native_columns_data(
        op: Operator,
        path: &str,
        index: usize,
        metas: Vec<ColumnMeta>,
        range: Option<&Range<usize>>,
    ) -> Result<(usize, Vec<NativeReader<Reader>>)> {
        let mut native_readers = Vec::with_capacity(metas.len());
        for meta in metas {
            let (offset, length) = meta.offset_length();
            let mut native_meta = meta.as_native().unwrap().clone();
            if let Some(range) = &range {
                native_meta = native_meta.slice(range.start, range.end);
            }

            let reader = op.range_read(path, offset..offset + length).await?;
            let reader: Reader = Box::new(std::io::Cursor::new(reader));

            let native_reader = NativeReader::new(reader, native_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }

        Ok((index, native_readers))
    }

    pub fn sync_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<BTreeMap<usize, Vec<NativeReader<Reader>>>> {
        let part = FusePartInfo::from_part(&part)?;

        let mut results: BTreeMap<usize, Vec<NativeReader<Reader>>> = BTreeMap::new();
        for (index, column_node) in self.project_column_nodes.iter().enumerate() {
            let op = self.operator.clone();
            let metas: Vec<ColumnMeta> = column_node
                .leaf_column_ids
                .iter()
                .filter_map(|column_id| part.columns_meta.get(column_id))
                .cloned()
                .collect::<Vec<_>>();

            let readers =
                Self::sync_read_native_column(op.clone(), &part.location, metas, part.range())?;
            results.insert(index, readers);
        }

        Ok(results)
    }

    pub fn sync_read_native_column(
        op: Operator,
        path: &str,
        metas: Vec<ColumnMeta>,
        range: Option<&Range<usize>>,
    ) -> Result<Vec<NativeReader<Reader>>> {
        let mut native_readers = Vec::with_capacity(metas.len());
        for meta in metas {
            let mut native_meta = meta.as_native().unwrap().clone();
            if let Some(range) = &range {
                native_meta = native_meta.slice(range.start, range.end);
            }
            let (offset, length) = (
                native_meta.offset,
                native_meta.pages.iter().map(|p| p.length).sum::<u64>(),
            );
            let reader = op.blocking().range_reader(path, offset..offset + length)?;
            let reader: Reader = Box::new(BufReader::new(reader));

            let native_reader = NativeReader::new(reader, native_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }

        Ok(native_readers)
    }

    pub fn fill_missing_native_column_values(
        &self,
        data_block: DataBlock,
        parts: &VecDeque<PartInfoPtr>,
    ) -> Result<DataBlock> {
        let part = FusePartInfo::from_part(&parts[0])?;

        let data_block_column_ids: HashSet<ColumnId> = part.columns_meta.keys().cloned().collect();
        let default_vals = self.default_vals.clone();

        DataBlock::create_with_default_value_and_block(
            &self.projected_schema,
            &data_block,
            &data_block_column_ids,
            &default_vals,
        )
    }

    pub fn build_block(
        &self,
        chunks: Vec<(usize, Box<dyn Array>)>,
        default_val_indices: Option<HashSet<usize>>,
    ) -> Result<DataBlock> {
        let mut rows = 0;
        let mut entries = Vec::with_capacity(chunks.len());
        for (index, _) in self.project_column_nodes.iter().enumerate() {
            if let Some(array) = chunks.iter().find(|c| c.0 == index).map(|c| c.1.clone()) {
                let data_type: DataType = self.projected_schema.field(index).data_type().into();
                entries.push(BlockEntry {
                    data_type: data_type.clone(),
                    value: Value::Column(Column::from_arrow(array.as_ref(), &data_type)),
                });
                rows = array.len();
            } else if let Some(ref default_val_indices) = default_val_indices {
                if default_val_indices.contains(&index) {
                    let data_type: DataType = self.projected_schema.field(index).data_type().into();
                    let default_val = &self.default_vals[index];
                    entries.push(BlockEntry {
                        data_type: data_type.clone(),
                        value: Value::Scalar(default_val.to_owned()),
                    });
                }
            }
        }
        Ok(DataBlock::new(entries, rows))
    }
}
