//  Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::DataBlock;

use super::fuse_rows_fetcher::RowsFetcher;
use super::native_data_source::DataChunks;
use super::native_data_source_deserializer::NativeDeserializeDataTransform;
use crate::io::BlockReader;

pub(super) struct NativeRowsFetcher<const BLOCKING_IO: bool> {
    reader: Arc<BlockReader>,
    column_leaves: Vec<Vec<ColumnDescriptor>>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for NativeRowsFetcher<BLOCKING_IO> {
    async fn fetch(
        &self,
        part_map: &HashMap<u64, PartInfoPtr>,
        part_set: HashSet<u64>,
    ) -> Result<(Vec<DataBlock>, HashMap<u64, usize>)> {
        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let chunk = self.reader.sync_read_native_columns_data(part)?;
                chunks.push((part_id, chunk));
            }
        } else {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let chunk = self.reader.async_read_native_columns_data(part).await?;
                chunks.push((part_id, chunk));
            }
        }
        let mut part_idx_map = HashMap::with_capacity(chunks.len());
        let fetched_blocks = chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (part, chunk))| {
                part_idx_map.insert(part, idx);
                self.build_block(chunk)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((fetched_blocks, part_idx_map))
    }

    fn reader(&self) -> &BlockReader {
        &self.reader
    }
}

impl<const BLOCKING_IO: bool> NativeRowsFetcher<BLOCKING_IO> {
    pub fn create(reader: Arc<BlockReader>, column_leaves: Vec<Vec<ColumnDescriptor>>) -> Self {
        Self {
            reader,
            column_leaves,
        }
    }

    fn build_block(&self, mut chunks: DataChunks) -> Result<DataBlock> {
        let mut array_iters = BTreeMap::new();

        for (index, column_node) in self.reader.project_column_nodes.iter().enumerate() {
            let readers = chunks.remove(&index).unwrap();
            if !readers.is_empty() {
                let leaves = self.column_leaves.get(index).unwrap().clone();
                let array_iter =
                    NativeDeserializeDataTransform::build_array_iter(column_node, leaves, readers)?;
                array_iters.insert(index, array_iter);
            }
        }

        let mut arrays = Vec::with_capacity(array_iters.len());
        for (index, array_iter) in array_iters.iter_mut() {
            let array = array_iter.next().unwrap()?;
            arrays.push((*index, array));
        }

        self.reader.build_block(arrays, None)
    }
}
