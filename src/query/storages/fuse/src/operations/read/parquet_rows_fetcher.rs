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
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::stream::FuturesUnordered;
use futures_util::stream::StreamExt;
use itertools::Itertools;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::fuse_rows_fetcher::RowsFetchMetadata;
use super::fuse_rows_fetcher::RowsFetcher;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;

struct AbortOnDropTasks<T> {
    inner: FuturesUnordered<JoinHandle<T>>,
}

impl<T> AbortOnDropTasks<T> {
    fn new() -> Self {
        Self {
            inner: FuturesUnordered::new(),
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&self, task: JoinHandle<T>) {
        self.inner.push(task);
    }

    async fn next(&mut self) -> Option<std::result::Result<T, tokio::task::JoinError>> {
        self.inner.next().await
    }
}

impl<T> Drop for AbortOnDropTasks<T> {
    fn drop(&mut self) {
        for task in self.inner.iter() {
            task.abort();
        }
    }
}

pub struct RowsFetchMetadataImpl {
    // Average bytes per row
    pub row_bytes: usize,
    // block_bytes after projection
    pub block_bytes: usize,

    pub location: String,
    pub nums_rows: usize,
    pub compression: Compression,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
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
    // Per-lane cap on the number of block-fetch tasks spawned at once, so a single
    // `fetch()` call does not spawn one task per hit block up front.
    max_threads: usize,
    // Shared across every RowFetch lane of the query. A mutation plan can fan
    // RowFetch out to `max_threads` lanes (see physical_row_fetch.rs), each with
    // its own fetcher; without a shared gate the aggregate in-flight decoded
    // chunks would be `lanes * max_threads`, so peak memory would not be bounded
    // by `max_threads` and could OOM on wide/long string columns. Each in-flight
    // `fetch_block` holds one permit for the lifetime of its decoded chunk, so the
    // query-wide count of concurrently-held chunks never exceeds the permit count.
    io_semaphore: Arc<Semaphore>,

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

        let mut final_blocks = HashMap::with_capacity(tasks_indices.len());

        // Concurrency is bounded in two layers, neither of which is the old
        // accumulated-byte barrier (which collapsed to a single in-flight task
        // whenever one block's chunk already exceeded the budget — common for
        // wide/long string columns — and thereby serialized the whole fetch;
        // see #19677 and the row-fetch perf regression):
        //   1. This per-lane window (FuturesUnordered) caps how many fetch tasks
        //      one `fetch()` call keeps in flight, draining by completion order
        //      so S3 tail latency on one block does not stall the window.
        //   2. `io_semaphore` (shared across all RowFetch lanes of the query) caps
        //      how many decoded column chunks are actually alive at once, so peak
        //      memory stays bounded even when a mutation plan fans RowFetch out to
        //      `max_threads` lanes.
        let max_concurrency = self.max_threads.max(1);
        let mut in_flight = AbortOnDropTasks::new();
        let mut tasks_indices = tasks_indices.into_iter();
        loop {
            // Fill the window up to max_concurrency.
            while in_flight.len() < max_concurrency {
                let Some((block_id, task_indices)) = tasks_indices.next() else {
                    break;
                };
                let metadata = &metadata[&block_id];
                let final_take_index = final_block_index[&block_id];
                in_flight.push(spawn(self.fetch_block(
                    metadata.clone(),
                    final_take_index,
                    task_indices,
                )));
            }

            // Drain the first completed task to make room for the next block.
            let Some(result) = in_flight.next().await else {
                break;
            };
            let (final_index, block) = match result.expect("row-fetch task panicked") {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            final_blocks.insert(final_index, block);
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
        max_threads: usize,
        io_semaphore: Arc<Semaphore>,
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
            max_threads: max_threads.max(1),
            io_semaphore,
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
            let io_semaphore = self.io_semaphore.clone();
            async move {
                // Hold a permit for the whole decode: the query-wide number of
                // decoded chunks alive at once is capped by the shared semaphore,
                // regardless of how many RowFetch lanes the plan created.
                let _permit = io_semaphore
                    .acquire_owned()
                    .await
                    .expect("row-fetch io semaphore never closed");
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
        )
    }
}

impl From<RowsFetchMetadataImpl> for CacheValue<RowsFetchMetadataImpl> {
    fn from(value: RowsFetchMetadataImpl) -> Self {
        CacheValue::new(value, 0)
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn aborts_running_and_waiting_tasks_on_drop() {
        let semaphore = Arc::new(Semaphore::new(1));
        let tasks = AbortOnDropTasks::new();

        let (running_started_tx, running_started_rx) = oneshot::channel();
        let (running_dropped_tx, running_dropped_rx) = oneshot::channel();
        let running_semaphore = semaphore.clone();
        tasks.push(spawn(async move {
            let _drop_signal = DropSignal(Some(running_dropped_tx));
            let _permit = running_semaphore.acquire_owned().await.unwrap();
            running_started_tx.send(()).unwrap();
            pending::<()>().await;
        }));

        running_started_rx.await.unwrap();

        let (waiting_started_tx, waiting_started_rx) = oneshot::channel();
        let (waiting_dropped_tx, waiting_dropped_rx) = oneshot::channel();
        let waiting_semaphore = semaphore.clone();
        tasks.push(spawn(async move {
            let _drop_signal = DropSignal(Some(waiting_dropped_tx));
            waiting_started_tx.send(()).unwrap();
            let _permit = waiting_semaphore.acquire_owned().await.unwrap();
            pending::<()>().await;
        }));

        waiting_started_rx.await.unwrap();
        drop(tasks);

        timeout(Duration::from_secs(1), running_dropped_rx)
            .await
            .expect("running task should be aborted")
            .unwrap();
        timeout(Duration::from_secs(1), waiting_dropped_rx)
            .await
            .expect("task waiting for a permit should be aborted")
            .unwrap();

        let permit = timeout(Duration::from_secs(1), semaphore.acquire())
            .await
            .expect("aborted task should release its permit")
            .unwrap();
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
