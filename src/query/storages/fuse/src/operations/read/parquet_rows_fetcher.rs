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
use std::collections::hash_map::Entry;
use std::future::Future;
use std::sync::Arc;

use databend_common_base::runtime::spawn;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::compute_row_id_prefix;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::future;
use itertools::Itertools;

use super::fuse_rows_fetcher::RowsFetchMetadata;
use super::fuse_rows_fetcher::RowsFetcher;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;

pub struct RowsFetchMetadataImpl {
    // Average bytes per row
    pub row_bytes: usize,
    // block_bytes after projection
    pub block_bytes: usize,

    pub location: String,
    pub nums_rows: usize,
    pub compression: Compression,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
    pub columns_stat: Option<HashMap<ColumnId, ColumnStatistics>>,
}

impl RowsFetchMetadata for RowsFetchMetadataImpl {
    fn row_bytes(&self) -> usize {
        self.row_bytes
    }
}

pub(super) struct ParquetRowsFetcher {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    projection: Projection,
    schema: TableSchemaRef,
    settings: ReadSettings,

    reader: Arc<BlockReader>,

    segment_reader: CompactSegmentInfoReader,
    block_meta_lru_cache: InMemoryLruCache<RowsFetchMetadataImpl>,
}

#[async_trait::async_trait]
impl RowsFetcher for ParquetRowsFetcher {
    type Metadata = Arc<RowsFetchMetadataImpl>;

    #[async_backtrace::framed]
    async fn initialize(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }
    async fn fetch_metadata(&mut self, block_id: u64) -> Result<Self::Metadata> {
        if let Some(v) = self.block_meta_lru_cache.get(block_id.to_string()) {
            return Ok(v.clone());
        }

        // load metadata
        let (segment, block) = split_prefix(block_id);
        let snapshot = self.snapshot.as_ref().unwrap();

        let (location, ver) = snapshot.segments[segment as usize].clone();
        let segment_load_params = LoadParams {
            ver,
            location,
            len_hint: None,
            put_cache: true,
        };
        let compact_segment_info = self.segment_reader.read(&segment_load_params).await?;

        let blocks = compact_segment_info.block_metas()?;
        let block_idx = block_idx_in_segment(blocks.len(), block as usize);

        static PREFETCH_SIZE: usize = 10;
        let cache_start = block_idx / PREFETCH_SIZE * PREFETCH_SIZE;
        let mut cache_end = block_idx / PREFETCH_SIZE * PREFETCH_SIZE + PREFETCH_SIZE;

        cache_end = std::cmp::min(cache_end, blocks.len());
        let metadata = self.build_metadata(&blocks[cache_start..cache_end])?;

        for (block_index, metadata) in (cache_start..cache_end).zip(metadata.into_iter()) {
            let block_id = block_id_in_segment(blocks.len(), block_index);
            let block_id = compute_row_id_prefix(segment, block_id as u64);
            self.block_meta_lru_cache
                .insert(block_id.to_string(), metadata);
        }

        Ok(self
            .block_meta_lru_cache
            .get(block_id.to_string())
            .clone()
            .unwrap())
    }

    #[async_backtrace::framed]
    async fn fetch(
        &mut self,
        row_ids: &[u64],
        metadata: HashMap<u64, Self::Metadata>,
    ) -> Result<DataBlock> {
        let final_block_index = metadata
            .keys()
            .enumerate()
            .map(|(idx, id)| (*id, idx as u32))
            .collect::<HashMap<_, _>>();

        // take rows from one block
        let mut tasks_indices = HashMap::with_capacity(metadata.len());
        // take rows from blocks
        let mut final_indices = Vec::with_capacity(row_ids.len());

        for row_id in row_ids {
            let (block_id, idx) = split_row_id(*row_id);
            match tasks_indices.entry(block_id) {
                Entry::Occupied(mut v) => {
                    let task_indices: &mut Vec<_> = v.get_mut();

                    let final_index = task_indices.len() as u32;
                    task_indices.push(idx as u32);
                    final_indices.push((final_block_index[&block_id], final_index));
                }
                Entry::Vacant(v) => {
                    v.insert(vec![idx as u32]);
                    final_indices.push((final_block_index[&block_id], 0_u32));
                }
            }
        }

        let mut tasks_handle = Vec::with_capacity(tasks_indices.len());
        let mut blocks_bytes = 0;
        let mut final_blocks = HashMap::with_capacity(tasks_indices.len());
        let mut tasks_indices = tasks_indices.into_iter().peekable();
        while let Some((block_id, task_indices)) = tasks_indices.next() {
            let metadata = &metadata[&block_id];
            blocks_bytes += metadata.block_bytes;

            let final_take_index = final_block_index[&block_id];
            let join_handle =
                spawn(self.fetch_block(metadata.clone(), final_take_index, task_indices));
            tasks_handle.push(join_handle);

            // To prevent excessive memory usage, we need to perform a join when the threshold is reached.
            if blocks_bytes >= 50 * 1024 * 1024 || tasks_indices.peek().is_none() {
                let tasks_handle = std::mem::take(&mut tasks_handle);
                let tasks_block = future::try_join_all(tasks_handle).await.unwrap();
                for task_block in tasks_block {
                    let (final_index, block) = task_block?;
                    final_blocks.insert(final_index, block);
                }
            }
        }

        let final_blocks = final_blocks
            .into_iter()
            .sorted_by_key(|(idx, _)| *idx)
            .map(|(_idx, block)| block)
            .collect::<Vec<_>>();

        // check if row index is in valid bounds cause we don't ensure rowid is valid
        for (block_idx, row_idx) in final_indices.iter() {
            if *block_idx as usize >= final_blocks.len()
                || *row_idx as usize >= final_blocks[*block_idx as usize].num_rows()
            {
                return Err(ErrorCode::Internal(format!(
                    "RowID is invalid, block idx {block_idx}, row idx {row_idx}, blocks len {}, block idx len {:?}",
                    final_blocks.len(),
                    final_blocks.get(*block_idx as usize).map(|b| b.num_rows()),
                )));
            }
        }

        Ok(DataBlock::take_blocks(&final_blocks, &final_indices))
    }
}

impl ParquetRowsFetcher {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        settings: ReadSettings,
    ) -> Self {
        let schema = table.schema();
        let operator = table.operator.clone();
        let segment_reader = MetaReaders::segment_info_reader(operator, schema.clone());
        ParquetRowsFetcher {
            table,
            snapshot: None,
            segment_reader,
            projection,
            schema,
            reader,
            settings,
            block_meta_lru_cache: InMemoryLruCache::with_items_capacity(
                String::from("RowFetchBlockMetaCache"),
                128,
            ),
        }
    }

    fn fetch_block(
        &self,
        metadata: Arc<RowsFetchMetadataImpl>,
        final_index: u32,
        take_indices: Vec<u32>,
    ) -> impl Future<Output = Result<(u32, DataBlock)>> + use<> {
        {
            let settings = self.settings;
            let reader = self.reader.clone();
            async move {
                let chunk = reader
                    .read_columns_data_by_merge_io(
                        &settings,
                        &metadata.location,
                        &metadata.columns_meta,
                        &None,
                    )
                    .await?;

                Ok((
                    final_index,
                    Self::build_block(&reader, &metadata, chunk)?.take(take_indices.as_slice())?,
                ))
            }
        }
    }

    fn build_metadata(&self, meta: &[Arc<BlockMeta>]) -> Result<Vec<RowsFetchMetadataImpl>> {
        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        let mut metadata = Vec::with_capacity(meta.len());
        for block_meta in meta {
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
            );

            let fuse_part = FuseBlockPartInfo::from_part(&part_info)?;

            let compression_ratio = block_meta.block_size as f64 / block_meta.file_size as f64;
            let mut block_bytes = 0;
            let mut average_bytes = 0;
            for (column_id, column_meta) in &fuse_part.columns_meta {
                if let Some(columns_stat) = &fuse_part.columns_stat {
                    if let Some(column_stat) = columns_stat.get(column_id) {
                        average_bytes += column_stat.in_memory_size as usize / fuse_part.nums_rows;
                        block_bytes += column_stat.in_memory_size as usize;

                        continue;
                    }
                }

                let compressed_size = column_meta.read_bytes(&None);
                let estimate_memory_size = (compressed_size as f64 * compression_ratio) as usize;
                block_bytes += estimate_memory_size;
                average_bytes += estimate_memory_size / fuse_part.nums_rows;
            }

            metadata.push(RowsFetchMetadataImpl {
                row_bytes: average_bytes,
                block_bytes,
                nums_rows: fuse_part.nums_rows,
                compression: fuse_part.compression,
                location: fuse_part.location.clone(),
                columns_meta: fuse_part.columns_meta.clone(),
                columns_stat: fuse_part.columns_stat.clone(),
            });
        }

        Ok(metadata)
    }

    fn build_block(
        reader: &BlockReader,
        metadata: &RowsFetchMetadataImpl,
        chunk: BlockReadResult,
    ) -> Result<DataBlock> {
        let columns_chunks = chunk.columns_chunks()?;
        reader.deserialize_parquet_chunks(
            metadata.nums_rows,
            &metadata.columns_meta,
            columns_chunks,
            &metadata.compression,
            &metadata.location,
            None,
            metadata.columns_stat.as_ref(),
        )
    }
}

impl From<RowsFetchMetadataImpl> for CacheValue<RowsFetchMetadataImpl> {
    fn from(value: RowsFetchMetadataImpl) -> Self {
        CacheValue::new(value, 0)
    }
}
