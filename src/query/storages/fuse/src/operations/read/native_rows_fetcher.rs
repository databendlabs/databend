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

use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use itertools::Itertools;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::NativeSourceData;
use crate::FuseTable;

pub(super) struct NativeRowsFetcher<const BLOCKING_IO: bool> {
    table: Arc<FuseTable>,
    snapshot: Option<Arc<TableSnapshot>>,
    segment_reader: CompactSegmentInfoReader,
    projection: Projection,
    schema: TableSchemaRef,
    reader: Arc<BlockReader>,
    column_leaves: Arc<Vec<Vec<ColumnDescriptor>>>,

    // The value contains part info and the page size of the corresponding block file.
    part_map: HashMap<u64, (PartInfoPtr, u64)>,
    segment_blocks_cache: HashMap<u64, Vec<Arc<BlockMeta>>>,

    // To control the parallelism of fetching blocks.
    max_threads: usize,
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

        // Read blocks in `prefix` order.
        let part_set = part_set
            .into_iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(k, v)| {
                let mut v = v.into_iter().collect::<Vec<_>>();
                v.sort();
                (k, v)
            })
            .collect::<Vec<_>>();
        let mut idx_map = HashMap::with_capacity(part_set.len());
        for (p, pages) in part_set.iter() {
            for page in pages {
                idx_map.insert((*p, *page), idx_map.len());
            }
        }
        // parts_per_thread = num_parts / max_threads
        // remain = num_parts % max_threads
        // task distribution:
        //   Part number of each task   |       Task number
        // ------------------------------------------------------
        //    parts_per_thread + 1      |         remain
        //      parts_per_thread        |   max_threads - remain
        let num_parts = part_set.len();
        let mut tasks = Vec::with_capacity(self.max_threads);
        // Fetch blocks in parallel.
        for i in 0..self.max_threads {
            let begin = num_parts * i / self.max_threads;
            let end = num_parts * (i + 1) / self.max_threads;
            if begin == end {
                continue;
            }
            let parts = part_set[begin..end]
                .iter()
                .map(|(idx, pages)| (self.part_map[idx].0.clone(), pages.clone()))
                .collect::<Vec<_>>();
            tasks.push(Self::fetch_blocks(
                self.reader.clone(),
                parts,
                self.column_leaves.clone(),
            ))
        }

        let num_task = tasks.len();
        let blocks = execute_futures_in_parallel(
            tasks,
            num_task,
            num_task * 2,
            "native rows fetch".to_string(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let indices = row_set
            .iter()
            .map(|(prefix, page_idx, idx)| {
                let block_idx = idx_map[&(*prefix, *page_idx)];
                (block_idx as u32, *idx as u32, 1_usize)
            })
            .collect::<Vec<_>>();

        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }

    fn schema(&self) -> DataSchema {
        self.reader.data_schema()
    }
}

impl<const BLOCKING_IO: bool> NativeRowsFetcher<BLOCKING_IO> {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        column_leaves: Arc<Vec<Vec<ColumnDescriptor>>>,
        max_threads: usize,
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
            segment_blocks_cache: HashMap::new(),
            max_threads,
        }
    }

    async fn prepare_part_map(&mut self, row_ids: &[u64]) -> Result<()> {
        let snapshot = self.snapshot.as_ref().unwrap();

        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        for row_id in row_ids {
            let (prefix, _) = split_row_id(*row_id);

            if self.part_map.contains_key(&prefix) {
                continue;
            }

            let (segment, block) = split_prefix(prefix);

            if let std::collections::hash_map::Entry::Vacant(e) =
                self.segment_blocks_cache.entry(segment)
            {
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
                e.insert(blocks);
            }

            let blocks = self.segment_blocks_cache.get(&segment).unwrap();
            let block_idx = block_idx_in_segment(blocks.len(), block as usize);
            let block_meta = &blocks[block_idx];
            let page_size = block_meta.page_size();

            let bloom_index_cols = None;
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
                bloom_index_cols,
            );
            self.part_map.insert(prefix, (part_info, page_size));
        }

        Ok(())
    }

    fn build_blocks(
        reader: &BlockReader,
        mut chunks: NativeSourceData,
        needed_pages: &[u64],
        column_leaves: &[Vec<ColumnDescriptor>],
    ) -> Result<Vec<DataBlock>> {
        let mut array_iters = BTreeMap::new();

        for (index, column_node) in reader.project_column_nodes.iter().enumerate() {
            let readers = chunks.remove(&index).unwrap();
            if !readers.is_empty() {
                let leaves = column_leaves.get(index).unwrap().clone();
                let array_iter = BlockReader::build_array_iter(column_node, leaves, readers)?;
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
            let block = reader.build_block(&arrays, None)?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    #[allow(clippy::type_complexity)]
    #[async_backtrace::framed]
    async fn fetch_blocks(
        reader: Arc<BlockReader>,
        parts: Vec<(PartInfoPtr, Vec<u64>)>,
        column_leaves: Arc<Vec<Vec<ColumnDescriptor>>>,
    ) -> Result<Vec<DataBlock>> {
        let mut chunks = Vec::with_capacity(parts.len());
        if BLOCKING_IO {
            for (part, _) in parts.iter() {
                let chunk = reader.sync_read_native_columns_data(part, &None)?;
                chunks.push(chunk);
            }
        } else {
            for (part, _) in parts.iter() {
                let chunk = reader
                    .async_read_native_columns_data(part, &reader.ctx, &None)
                    .await?;
                chunks.push(chunk);
            }
        }
        let num_blocks = parts.iter().map(|(_, p)| p.len()).sum::<usize>();
        let mut blocks = Vec::with_capacity(num_blocks);
        for (chunk, (_, needed_pages)) in chunks.into_iter().zip(parts.iter()) {
            let fetched_blocks = Self::build_blocks(&reader, chunk, needed_pages, &column_leaves)?;
            blocks.extend(fetched_blocks);
        }

        Ok(blocks)
    }
}
