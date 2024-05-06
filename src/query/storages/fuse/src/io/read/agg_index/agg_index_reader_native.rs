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

use std::sync::Arc;

use databend_common_arrow::arrow::chunk::Chunk;
use databend_common_arrow::native::read as nread;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_table_meta::meta::ColumnMeta;
use log::debug;

use super::AggIndexReader;
use crate::io::BlockReader;
use crate::io::NativeSourceData;
use crate::FuseBlockPartInfo;

impl AggIndexReader {
    pub fn sync_read_native_data(&self, loc: &str) -> Option<NativeSourceData> {
        match self.reader.operator.blocking().reader(loc) {
            Ok(mut reader) => {
                let metadata = nread::reader::read_meta(&mut reader)
                    .inspect_err(|e| {
                        debug!("Read aggregating index `{loc}`'s metadata failed: {e}")
                    })
                    .ok()?;
                let num_rows = metadata[0].pages.iter().map(|p| p.num_values).sum();
                debug_assert!(
                    metadata
                        .iter()
                        .all(|c| c.pages.iter().map(|p| p.num_values).sum::<u64>() == num_rows)
                );
                let columns_meta = metadata
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (i as u32, ColumnMeta::Native(c)))
                    .collect();
                let bloom_index_cols = None;
                let part = FuseBlockPartInfo::create(
                    loc.to_string(),
                    num_rows,
                    columns_meta,
                    None,
                    self.compression.into(),
                    None,
                    None,
                    None,
                    bloom_index_cols,
                );
                let res = self
                    .reader
                    .sync_read_native_columns_data(&part, &None)
                    .inspect_err(|e| debug!("Read aggregating index `{loc}` failed: {e}"))
                    .ok()?;
                Some(res)
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    debug!("Aggregating index `{loc}` not found.")
                } else {
                    debug!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub async fn read_native_data(&self, loc: &str) -> Option<NativeSourceData> {
        match self.reader.operator.reader(loc).await {
            Ok(mut reader) => {
                let metadata = nread::reader::read_meta_async(&mut reader, None)
                    .await
                    .inspect_err(|e| {
                        debug!("Read aggregating index `{loc}`'s metadata failed: {e}")
                    })
                    .ok()?;
                if metadata.is_empty() {
                    debug!("Aggregating index `{loc}` is empty");
                    return None;
                }
                let num_rows = metadata[0].pages.iter().map(|p| p.num_values).sum();
                debug_assert!(
                    metadata
                        .iter()
                        .all(|c| c.pages.iter().map(|p| p.num_values).sum::<u64>() == num_rows)
                );
                let columns_meta = metadata
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (i as u32, ColumnMeta::Native(c)))
                    .collect();
                let bloom_index_cols = None;
                let part = FuseBlockPartInfo::create(
                    loc.to_string(),
                    num_rows,
                    columns_meta,
                    None,
                    self.compression.into(),
                    None,
                    None,
                    None,
                    bloom_index_cols,
                );
                let res = self
                    .reader
                    .async_read_native_columns_data(&part, &self.ctx, &None)
                    .await
                    .inspect_err(|e| debug!("Read aggregating index `{loc}` failed: {e}"))
                    .ok()?;
                Some(res)
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    debug!("Aggregating index `{loc}` not found.")
                } else {
                    debug!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub fn deserialize_native_data(&self, data: &mut NativeSourceData) -> Result<DataBlock> {
        let mut all_columns_arrays = vec![];
        for (index, column_node) in self.reader.project_column_nodes.iter().enumerate() {
            let column_leaves = column_node
                .leaf_indices
                .iter()
                .map(|i| self.reader.parquet_schema_descriptor.columns()[*i].clone())
                .collect::<Vec<_>>();

            let readers = data.remove(&index).unwrap();
            let array_iter = BlockReader::build_array_iter(column_node, column_leaves, readers)?;
            let arrays = array_iter.map(|a| Ok(a?)).collect::<Result<Vec<_>>>()?;
            all_columns_arrays.push(arrays);
        }
        if all_columns_arrays.is_empty() {
            return Ok(DataBlock::empty_with_schema(Arc::new(
                self.reader.data_schema(),
            )));
        }
        debug_assert!(
            all_columns_arrays
                .iter()
                .all(|a| a.len() == all_columns_arrays[0].len())
        );
        let page_num = all_columns_arrays[0].len();
        let mut blocks = Vec::with_capacity(page_num);

        for i in 0..page_num {
            let mut arrays = Vec::with_capacity(all_columns_arrays.len());
            for array in all_columns_arrays.iter() {
                arrays.push(array[i].clone());
            }
            let chunk = Chunk::new(arrays);
            let block = DataBlock::from_arrow_chunk(&chunk, &self.reader.data_schema())?;
            blocks.push(block);
        }
        let block = DataBlock::concat(&blocks)?;
        self.apply_agg_info(block)
    }
}
