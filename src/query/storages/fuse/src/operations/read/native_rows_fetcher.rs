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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::block_id_in_segment;
use common_catalog::plan::compute_row_id_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_storage::ColumnNodes;
use storages_common_table_meta::meta::SegmentInfo;

use super::fuse_rows_fetcher::RowsFetcher;
use super::native_data_source::DataChunks;
use super::native_data_source_deserializer::NativeDeserializeDataTransform;
use crate::io::BlockReader;
use crate::FuseTable;

pub(super) struct NativeRowsFetcher<const BLOCKING_IO: bool> {
    reader: Arc<BlockReader>,
    column_leaves: Vec<Vec<ColumnDescriptor>>,

    // The value contains part info and the page size of the corresponding block file.
    part_map: HashMap<u64, (PartInfoPtr, u64)>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for NativeRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn fetch(&self, row_ids: &[u64]) -> Result<DataBlock> {
        let num_rows: usize = row_ids.len();
        let mut part_set: HashMap<_, HashSet<_>> = HashMap::new();
        let mut row_set = Vec::with_capacity(num_rows);
        for row_id in row_ids {
            let (prefix, idx) = split_row_id(*row_id);
            let page_size = self.part_map[&prefix].1;
            let page_idx = idx / page_size;
            let idx_within_page = idx % page_size;
            part_set
                .entry(prefix)
                .and_modify(|s| {
                    s.insert(page_idx);
                })
                .or_insert(HashSet::from([page_idx]));
            row_set.push((prefix, page_idx, idx_within_page));
        }
        let part_set = part_set
            .into_iter()
            .map(|(k, v)| {
                let mut v = v.into_iter().collect::<Vec<_>>();
                v.sort();
                (k, v)
            })
            .collect();

        let (blocks, idx_map) = self.fetch_blocks(part_set).await?;
        let indices = row_set
            .iter()
            .map(|(prefix, page_idx, idx)| {
                let block_idx = idx_map[&(*prefix, *page_idx)];
                (block_idx, *idx as usize, 1_usize)
            })
            .collect::<Vec<_>>();

        let blocks = blocks.iter().collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }

    fn set_metas(
        &mut self,
        segments: Vec<(u64, Arc<SegmentInfo>)>,
        table_schema: TableSchemaRef,
        projection: &Projection,
    ) {
        let arrow_schema = table_schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&table_schema));

        let size_hint = if let Some((_, segment)) = segments.first() {
            segment.blocks.len()
        } else {
            1
        };
        let mut part_map = HashMap::with_capacity(segments.len() * size_hint);

        for (seg_id, segment) in segments {
            let block_num = segment.blocks.len();
            for (block_idx, block_meta) in segment.blocks.iter().enumerate() {
                let page_size = block_meta.page_size();
                let part_info =
                    FuseTable::projection_part(block_meta, &None, &column_nodes, None, projection);
                let block_id = block_id_in_segment(block_num, block_idx) as u64;
                let prefix = compute_row_id_prefix(seg_id, block_id);
                part_map.insert(prefix, (part_info, page_size));
            }
        }

        self.part_map = part_map;
    }
}

impl<const BLOCKING_IO: bool> NativeRowsFetcher<BLOCKING_IO> {
    pub fn create(reader: Arc<BlockReader>, column_leaves: Vec<Vec<ColumnDescriptor>>) -> Self {
        Self {
            reader,
            column_leaves,
            part_map: HashMap::new(),
        }
    }

    fn build_blocks(&self, mut chunks: DataChunks, needed_pages: &[u64]) -> Result<Vec<DataBlock>> {
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

        let mut blocks = Vec::with_capacity(needed_pages.len());

        for page in needed_pages {
            let mut arrays = Vec::with_capacity(array_iters.len());
            for (index, array_iter) in array_iters.iter_mut() {
                let array = array_iter.nth(*page as usize).unwrap()?;
                arrays.push((*index, array));
            }
            let block = self.reader.build_block(arrays, None)?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    #[allow(clippy::type_complexity)]
    #[async_backtrace::framed]
    async fn fetch_blocks(
        &self,
        part_set: HashMap<u64, Vec<u64>>,
    ) -> Result<(Vec<DataBlock>, HashMap<(u64, u64), usize>)> {
        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for (prefix, needed_pages) in part_set.into_iter() {
                let part = self.part_map[&prefix].0.clone();
                let chunk = self.reader.sync_read_native_columns_data(part)?;
                chunks.push((prefix, chunk, needed_pages));
            }
        } else {
            for (prefix, needed_pages) in part_set.into_iter() {
                let part = self.part_map[&prefix].0.clone();
                let chunk = self.reader.async_read_native_columns_data(part).await?;
                chunks.push((prefix, chunk, needed_pages));
            }
        }
        let num_blocks = chunks
            .iter()
            .map(|(_, _, pages)| pages.len())
            .sum::<usize>();
        let mut idx_map = HashMap::with_capacity(num_blocks);
        let mut blocks = Vec::with_capacity(num_blocks);

        let mut offset = 0_usize;
        for (prefix, chunk, needed_pages) in chunks.into_iter() {
            let fetched_blocks = self.build_blocks(chunk, &needed_pages)?;
            for (block, page) in fetched_blocks.into_iter().zip(needed_pages) {
                idx_map.insert((prefix, page), offset);
                offset += 1;
                blocks.push(block);
            }
        }

        Ok((blocks, idx_map))
    }
}
