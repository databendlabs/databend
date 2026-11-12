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

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::TableSnapshot;

use super::meta::RowFetchTask;
use super::meta::RowFetched;
use super::meta::take_row_fetch_meta;
use super::selection::BlockRowsSelection;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::FuseCaches;
use crate::io::SegmentCache;
use crate::io::SegmentInfoReader;
use crate::io::cache::BlockMetas;

pub struct RowsFetchMetadata {
    // Average bytes per row
    pub row_bytes: usize,

    pub location: String,
    pub nums_rows: usize,
    pub compression: Compression,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
}

/// Looks up the metadata of the blocks to fetch rows from.
pub(super) struct ParquetRowsFetcher {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    projection: Projection,
    // Column nodes of the table schema, to project the block metas.
    column_nodes: ColumnNodes,

    segment_cache: SegmentCache,
    segment_reader: SegmentInfoReader,
    // Decoded block metas of the segments this fetcher has read rows of. The
    // per-block metadata is built from them on each call: it is cheap next to
    // reading the block, and a block is looked up once per batch.
    fetched_segments: RowFetchSegmentCache,
}

impl ParquetRowsFetcher {
    pub(super) fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: &BlockReader,
    ) -> Self {
        let schema = table.schema();
        let operator = table.operator.clone();
        let segment_reader = SegmentInfoReader::new(operator, schema.clone());
        let column_nodes = ColumnNodes::new_from_schema(&schema.as_ref().into(), Some(&schema));
        ParquetRowsFetcher {
            segment_cache: FuseCaches::instance().segments.with_policy(reader.policy),
            table,
            snapshot: None,
            segment_reader,
            projection,
            column_nodes,
            fetched_segments: RowFetchSegmentCache::new(SEGMENT_CACHE_CAPACITY),
        }
    }

    #[async_backtrace::framed]
    pub(super) async fn initialize(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    pub(super) async fn fetch_metadata(&mut self, block_id: u64) -> Result<Arc<RowsFetchMetadata>> {
        let (segment, block) = split_prefix(block_id);
        let blocks = self
            .fetched_segments
            .get_or_load(segment, || async {
                let snapshot = self.snapshot.as_ref().unwrap();
                let (location, ver) = snapshot.segments[segment as usize].clone();
                // The process-wide `segment_block_metas` cache, when enabled, saves the decode.
                if let Some(blocks) = self.segment_cache.lookup_block_metas(&location) {
                    return Ok(blocks);
                }
                let compact_segment_info = self
                    .segment_cache
                    .get(&self.segment_reader, (location.clone(), ver))
                    .await?;
                self.segment_cache
                    .block_metas(&location, || compact_segment_info.block_metas())
            })
            .await?;
        let block_idx = block_idx_in_segment(blocks.len(), block as usize);
        Ok(Arc::new(self.build_metadata(&blocks[block_idx])?))
    }

    fn build_metadata(&self, block_meta: &BlockMeta) -> Result<RowsFetchMetadata> {
        let part_info = FuseTable::projection_part(
            block_meta,
            &None,
            &self.column_nodes,
            None,
            &self.projection,
        );

        let fuse_part = FuseBlockPartInfo::from_part(&part_info)?;

        let compression_ratio = block_meta.block_size as f64 / block_meta.file_size as f64;
        let mut average_bytes = 0;
        for (column_id, column_meta) in &fuse_part.columns_meta {
            if let Some(columns_stat) = &fuse_part.columns_stat {
                if let Some(column_stat) = columns_stat.get(column_id) {
                    average_bytes += column_stat.in_memory_size as usize / fuse_part.nums_rows;

                    continue;
                }
            }

            let compressed_size = column_meta.read_bytes(&None);
            let estimate_memory_size = (compressed_size as f64 * compression_ratio) as usize;
            average_bytes += estimate_memory_size / fuse_part.nums_rows;
        }

        Ok(RowsFetchMetadata {
            row_bytes: average_bytes,
            nums_rows: fuse_part.nums_rows,
            compression: fuse_part.compression,
            location: fuse_part.location.clone(),
            columns_meta: fuse_part.columns_meta.clone(),
        })
    }
}

/// Fetches the rows of a block, one task at a time: reads the column chunks of the
/// block asynchronously, then deserializes the requested rows synchronously.
pub(super) struct TransformRowFetch {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    reader: Arc<BlockReader>,
    settings: ReadSettings,

    task: Option<RowFetchTask>,
    chunk: Option<BlockReadResult>,
    output_data: Option<DataBlock>,
}

impl TransformRowFetch {
    pub(super) fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        reader: Arc<BlockReader>,
        settings: ReadSettings,
    ) -> Box<dyn Processor> {
        Box::new(TransformRowFetch {
            input,
            output,
            reader,
            settings,
            task: None,
            chunk: None,
            output_data: None,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRowFetch {
    fn name(&self) -> String {
        String::from("TransformRowFetch")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.output_data.take() {
            self.output.push_data(Ok(data));
            return Ok(Event::NeedConsume);
        }

        if self.chunk.is_some() {
            return Ok(Event::Sync);
        }

        if self.task.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let task: RowFetchTask = take_row_fetch_meta(self.input.pull_data().unwrap()?)?;
            if task.block.is_some() {
                self.task = Some(task);
                return Ok(Event::Async);
            }
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(Box::new(RowFetched {
                    batch: task.batch,
                    block: None,
                }))));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let (Some(task), Some(chunk)) = (self.task.take(), self.chunk.take()) else {
            return Err(ErrorCode::Internal("Row fetch deserializes nothing"));
        };
        let batch = task.batch;
        let task = task
            .block
            .ok_or_else(|| ErrorCode::Internal("Row fetch deserializes no block"))?;
        let metadata = &task.metadata;

        let rows = BlockRowsSelection::try_create(metadata.nums_rows, task.take_indices)?;
        let block = self.reader.deserialize_parquet_chunks(
            metadata.nums_rows,
            &metadata.columns_meta,
            chunk.columns_chunks()?,
            &metadata.compression,
            &metadata.location,
            rows.selection.as_ref(),
        )?;
        let block = rows.take_decoded_rows(&block)?;

        self.output_data = Some(DataBlock::empty_with_meta(Box::new(RowFetched {
            batch,
            block: Some((task.part, block)),
        })));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let metadata = self
            .task
            .as_ref()
            .and_then(|task| task.block.as_ref())
            .map(|block| block.metadata.clone())
            .ok_or_else(|| ErrorCode::Internal("Row fetch reads no block"))?;
        let chunk = self
            .reader
            .read_columns_data_by_merge_io(
                &self.settings,
                &metadata.location,
                &metadata.columns_meta,
                &None,
            )
            .await?;
        self.chunk = Some(chunk);
        Ok(())
    }
}

/// Segments whose decoded block metas a fetcher keeps.
const SEGMENT_CACHE_CAPACITY: usize = 16;

/// Decoded block metas of segments, by segment index in the snapshot. Decoding
/// decompresses and deserializes the metas of every block of the segment, so rows
/// of many blocks of one segment decode it once rather than once per block.
struct RowFetchSegmentCache {
    cache: InMemoryLruCache<FetchedSegment>,
}

struct FetchedSegment(BlockMetas);

impl From<FetchedSegment> for CacheValue<FetchedSegment> {
    fn from(value: FetchedSegment) -> Self {
        CacheValue::new(value, 0)
    }
}

impl RowFetchSegmentCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: InMemoryLruCache::with_items_capacity(
                String::from("RowFetchSegmentCache"),
                capacity,
            ),
        }
    }

    /// The block metas of `segment`, decoded by `load` if not cached.
    async fn get_or_load<F, Fut>(&self, segment: u64, load: F) -> Result<BlockMetas>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<BlockMetas>>,
    {
        let key = segment.to_string();
        if let Some(blocks) = self.cache.get(&key) {
            return Ok(blocks.0.clone());
        }
        let blocks = load().await?;
        self.cache.insert(key, FetchedSegment(blocks.clone()));
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_segment_cache_decode_once() -> Result<()> {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        let loads = AtomicUsize::new(0);
        let cache = RowFetchSegmentCache::new(2);
        let get = |segment| {
            cache.get_or_load(segment, || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok(Arc::new(Vec::new()))
            })
        };
        // Rows of any number of blocks of one segment load it once.
        for _ in 0..5 {
            get(0).await?;
        }
        assert_eq!(loads.load(Ordering::Relaxed), 1);
        // Beyond the capacity, the least recently used segment is loaded again.
        get(1).await?;
        get(2).await?;
        assert_eq!(loads.load(Ordering::Relaxed), 3);
        get(0).await?;
        assert_eq!(loads.load(Ordering::Relaxed), 4);
        Ok(())
    }
}
