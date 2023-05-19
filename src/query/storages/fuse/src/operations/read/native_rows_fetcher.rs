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
use common_catalog::plan::split_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_storage::ColumnNodes;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::TableSnapshot;

use super::fuse_rows_fetcher::RowsFetcher;
use super::native_data_source::DataChunks;
use super::native_data_source_deserializer::NativeDeserializeDataTransform;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::FuseTable;

pub(super) struct NativeRowsFetcher<const BLOCKING_IO: bool> {
    table: Arc<FuseTable>,
    snapshot: Option<Arc<TableSnapshot>>,
    segment_reader: CompactSegmentInfoReader,
    projection: Projection,
    schema: TableSchemaRef,
    reader: Arc<BlockReader>,
    column_leaves: Vec<Vec<ColumnDescriptor>>,

    // The value contains part info and the page size of the corresponding block file.
    part_map: HashMap<u64, (PartInfoPtr, u64)>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for NativeRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock> {
        self.prepare_part_map(row_ids).await?;

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
}

impl<const BLOCKING_IO: bool> NativeRowsFetcher<BLOCKING_IO> {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        column_leaves: Vec<Vec<ColumnDescriptor>>,
    ) -> Self {
        let schema = table.schema();
        let segment_reader =
            MetaReaders::segment_info_reader(table.operator.clone(), schema.clone());

        Self {
            table,
            snapshot: None,
            segment_reader,
            projection,
            schema,
            reader,
            column_leaves,
            part_map: HashMap::new(),
        }
    }

    async fn prepare_part_map(&mut self, row_ids: &[u64]) -> Result<()> {
        let snapshot = self.snapshot.as_ref().unwrap();

        let arrow_schema = self.schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        for row_id in row_ids {
            let (prefix, _) = split_row_id(*row_id);

            if self.part_map.contains_key(&prefix) {
                continue;
            }

            let (segment, block) = split_prefix(prefix);
            let (location, ver) = snapshot.segments[segment as usize].clone();
            let compact_segment_info = self
                .segment_reader
                .read(&LoadParams {
                    ver,
                    location,
                    len_hint: None,
                    put_cache: true,
                })
                .await?;

            let blocks = compact_segment_info.block_metas()?;
            let block_meta = &blocks[block as usize];
            let page_size = block_meta.page_size();
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
            );

            self.part_map.insert(prefix, (part_info, page_size));
        }

        Ok(())
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

        let mut offset = 0;
        for page in needed_pages {
            // Comments in the std lib:
            // Note that all preceding elements, as well as the returned element, will be
            // consumed from the iterator. That means that the preceding elements will be
            // discarded, and also that calling `nth(0)` multiple times on the same iterator
            // will return different elements.
            let pos = *page - offset;
            let mut arrays = Vec::with_capacity(array_iters.len());
            for (index, array_iter) in array_iters.iter_mut() {
                let array = array_iter.nth(pos as usize).unwrap()?;
                arrays.push((*index, array));
            }
            offset = *page + 1;
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
