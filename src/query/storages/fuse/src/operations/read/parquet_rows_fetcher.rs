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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockRowIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::future;
use itertools::Itertools;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseTable;

pub(super) struct ParquetRowsFetcher<const BLOCKING_IO: bool> {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    segment_reader: CompactSegmentInfoReader,
    projection: Projection,
    schema: TableSchemaRef,

    settings: ReadSettings,
    reader: Arc<BlockReader>,
    part_map: HashMap<u64, PartInfoPtr>,
    segment_blocks_cache: HashMap<u64, Vec<Arc<BlockMeta>>>,

    semaphore: Arc<Semaphore>,
    runtime: Arc<Runtime>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for ParquetRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    fn clear_cache(&mut self) {
        self.part_map.clear();
        self.segment_blocks_cache.clear();
    }

    #[async_backtrace::framed]
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock> {
        self.prepare_part_map(row_ids).await?;

        let num_rows = row_ids.len();
        let mut part_set = HashSet::new();
        let mut row_set = Vec::with_capacity(num_rows);
        let mut block_row_indices = HashMap::new();
        for row_id in row_ids {
            let (prefix, idx) = split_row_id(*row_id);
            part_set.insert(prefix);
            row_set.push((prefix, idx));
            block_row_indices
                .entry(prefix)
                .or_insert(Vec::new())
                .push((0u32, idx as u32, 1usize));
        }

        // Read blocks in `prefix` order.
        let part_set = part_set.into_iter().sorted().collect::<Vec<_>>();
        let mut idx_map = part_set
            .iter()
            .enumerate()
            .map(|(i, p)| (*p, (i, 0)))
            .collect::<HashMap<_, _>>();

        let mut tasks = Vec::with_capacity(part_set.len());
        for part in &part_set {
            tasks.push(Self::fetch_block(
                self.reader.clone(),
                self.part_map[part].clone(),
                self.settings,
                block_row_indices[part].clone(),
            ));
        }

        let tasks = tasks.into_iter().map(|v| {
            |permit| async {
                let r = v.await;
                drop(permit);
                r
            }
        });
        let join_handlers = self
            .runtime
            .try_spawn_batch_with_owned_semaphore(self.semaphore.clone(), tasks)
            .await?;

        let joint = future::try_join_all(join_handlers).await?;
        let blocks = joint.into_iter().collect::<Result<Vec<_>>>()?;
        // Take result rows from blocks.
        let indices = row_set
            .iter()
            .map(|(prefix, _)| {
                let (block_idx, row_idx_in_block) = idx_map.get_mut(prefix).unwrap();
                let row_idx = *row_idx_in_block;
                *row_idx_in_block += 1;
                (*block_idx as u32, row_idx as u32, 1_usize)
            })
            .collect::<Vec<_>>();

        // check if row index is in valid bounds cause we don't ensure rowid is valid
        for (block_idx, row_idx, _) in indices.iter() {
            if *block_idx as usize >= blocks.len()
                || *row_idx as usize >= blocks[*block_idx as usize].num_rows()
            {
                return Err(ErrorCode::Internal(format!(
                    "RowID is invalid, block idx {block_idx}, row idx {row_idx}, blocks len {}, block idx len {:?}",
                    blocks.len(),
                    blocks.get(*block_idx as usize).map(|b| b.num_rows()),
                )));
            }
        }

        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }
}

impl<const BLOCKING_IO: bool> ParquetRowsFetcher<BLOCKING_IO> {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        settings: ReadSettings,
        semaphore: Arc<Semaphore>,
        runtime: Arc<Runtime>,
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
            settings,
            part_map: HashMap::new(),
            segment_blocks_cache: HashMap::new(),
            semaphore,
            runtime,
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
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
            );

            self.part_map.insert(prefix, part_info);
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch_block(
        reader: Arc<BlockReader>,
        part: PartInfoPtr,
        settings: ReadSettings,
        block_row_indices: Vec<BlockRowIndex>,
    ) -> Result<DataBlock> {
        let chunk = if BLOCKING_IO {
            reader.sync_read_columns_data_by_merge_io(&settings, &part, &None)?
        } else {
            let fuse_part = FuseBlockPartInfo::from_part(&part)?;
            reader
                .read_columns_data_by_merge_io(
                    &settings,
                    &fuse_part.location,
                    &fuse_part.columns_meta,
                    &None,
                )
                .await?
        };
        let block = Self::build_block(&reader, &part, chunk)?;
        Ok(DataBlock::take_blocks(
            &[block],
            &block_row_indices,
            block_row_indices.len(),
        ))
    }

    fn build_block(
        reader: &BlockReader,
        part: &PartInfoPtr,
        chunk: BlockReadResult,
    ) -> Result<DataBlock> {
        let columns_chunks = chunk.columns_chunks()?;
        let part = FuseBlockPartInfo::from_part(part)?;
        reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            columns_chunks,
            &part.compression,
            &part.location,
        )
    }
}
