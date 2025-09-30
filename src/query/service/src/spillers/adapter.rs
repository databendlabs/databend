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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::DerefMut;
use std::ops::Range;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_storages_common_cache::ParquetMetaData;
use databend_storages_common_cache::TempPath;
use opendal::Buffer;
use opendal::Operator;

use super::async_buffer::BufferPool;
use super::inner::*;
use super::serialize::*;
use super::union_file::FileWriter;
use super::union_file::UnionFile;
use super::union_file::UnionFileWriter;
use super::Location;
use crate::pipelines::processors::transforms::SpillBuilder as WindowSpillBuilder;
use crate::pipelines::processors::transforms::SpillReader as WindowSpillReader;
use crate::pipelines::processors::transforms::SpillWriter as WindowSpillWriter;
use crate::sessions::QueryContext;
use crate::spillers::block_reader::BlocksReader;
use crate::spillers::block_writer::BlocksWriter;

#[derive(Clone)]
pub struct PartitionAdapter {
    ctx: Arc<QueryContext>,
    // Stores the spilled files that controlled by current spiller
    private_spilled_files: Arc<RwLock<HashMap<Location, Layout>>>,
    /// 1 partition -> N partition files
    partition_location: HashMap<usize, Vec<(Location, usize, usize)>>,
}

impl SpillAdapter for PartitionAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        self.private_spilled_files
            .write()
            .unwrap()
            .insert(location.clone(), layout.clone());
        self.ctx.as_ref().add_spill_file(location, layout, size);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.ctx.as_ref().get_spill_layout(location)
    }
}

pub type Spiller = SpillerInner<PartitionAdapter>;

impl Spiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
    ) -> Result<Self> {
        Self::new(
            PartitionAdapter {
                ctx,
                private_spilled_files: Default::default(),
                partition_location: Default::default(),
            },
            operator,
            config,
        )
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.adapter.partition_location.keys().copied().collect()
    }

    pub fn partition_blocks_reader(&mut self, id: &usize) -> BlocksReader<'_> {
        let locations: &[_] = match self.adapter.partition_location.get(id) {
            None => &[],
            Some(locations) => locations,
        };

        BlocksReader::new(locations, self.operator.clone())
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, partition_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(locs) = self.adapter.partition_location.get(partition_id) {
            let mut spilled_data = Vec::with_capacity(locs.len());
            for (loc, _data_size, _blocks_num) in locs.iter() {
                let block = self.read_spilled_file(loc).await?;

                if block.num_rows() != 0 {
                    spilled_data.push(block);
                }
            }
            Ok(spilled_data)
        } else {
            Ok(vec![])
        }
    }

    pub fn get_partition_locations(&self, partition_id: &usize) -> Option<Vec<Location>> {
        let locations = self.adapter.partition_location.get(partition_id)?;
        Some(locations.iter().map(|(loc, _, _)| loc.clone()).collect())
    }

    pub async fn block_stream_writer(&mut self) -> Result<BlocksWriter> {
        let location = self.create_unique_location();
        let writer = self.operator.writer_with(&location).await?;
        Ok(BlocksWriter::create(writer, Location::Remote(location)))
    }

    pub fn inc_progress(&self, progress_val: ProgressValues) {
        self.adapter
            .ctx
            .get_join_spill_progress()
            .incr(&progress_val);
    }

    pub fn add_hash_join_location(
        &mut self,
        partition: usize,
        location: Location,
        block_num: usize,
        data_size: usize,
    ) {
        self.adapter
            .add_spill_file(location.clone(), Layout::Parquet, data_size);

        if let Some(v) = self.adapter.partition_location.get_mut(&partition) {
            v.push((location, data_size, block_num));
            return;
        }

        self.adapter
            .partition_location
            .insert(partition, vec![(location, data_size, block_num)]);
    }

    #[async_backtrace::framed]
    /// Spill data block with partition
    pub async fn spill_with_partition(
        &mut self,
        partition_id: usize,
        data: Vec<DataBlock>,
    ) -> Result<()> {
        let (num_rows, memory_size) = data
            .iter()
            .map(|b| (b.num_rows(), b.memory_size()))
            .reduce(|acc, x| (acc.0 + x.0, acc.1 + x.1))
            .unwrap();

        let progress_val = ProgressValues {
            rows: num_rows,
            bytes: memory_size,
        };

        let location = self.spill(data).await?;

        match self.adapter.partition_location.entry(partition_id) {
            Entry::Vacant(v) => {
                v.insert(vec![(location, 0, 0)]);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().push((location, 0, 0));
            }
        };

        self.adapter
            .ctx
            .get_join_spill_progress()
            .incr(&progress_val);
        Ok(())
    }

    pub async fn spill_with_merged_partitions(
        &mut self,
        partitioned_data: Vec<(usize, Vec<DataBlock>)>,
    ) -> Result<MergedPartition> {
        // Serialize data block.
        let mut encoder = self.block_encoder();
        let mut partition_ids = Vec::new();
        for (partition_id, data_blocks) in partitioned_data.into_iter() {
            partition_ids.push(partition_id);
            encoder.add_blocks(data_blocks);
        }

        let write_bytes = encoder.size();
        let BlocksEncoder {
            buf,
            offsets,
            columns_layout,
            ..
        } = encoder;

        let layout = columns_layout.last().unwrap().clone();
        let partitions = partition_ids
            .into_iter()
            .zip(
                offsets
                    .windows(2)
                    .map(|x| x[0]..x[1])
                    .zip(columns_layout.into_iter()),
            )
            .map(|(id, (range, layout))| (id, Chunk { range, layout }))
            .collect();

        // Spill data to storage.
        let instant = Instant::now();
        let location = self.write_encodes(write_bytes, buf).await?;
        // Record statistics.
        record_write_profile(&location, &instant, write_bytes);

        self.adapter
            .add_spill_file(location.clone(), layout, write_bytes);
        Ok(MergedPartition {
            location,
            partitions,
        })
    }

    pub async fn read_merged_partitions(
        &self,
        MergedPartition {
            location,
            partitions,
        }: &MergedPartition,
    ) -> Result<Vec<(usize, DataBlock)>> {
        // Read spilled data from storage.
        let instant = Instant::now();

        let data = match (location, &self.local_operator) {
            (Location::Local(path), None) => {
                let file_size = path.size();
                debug_assert_eq!(
                    file_size,
                    if let Some((_, Chunk { range, .. })) = partitions.last() {
                        range.end
                    } else {
                        0
                    }
                );

                let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                Buffer::from(dma_buffer_to_bytes(buf)).slice(range)
            }
            (Location::Local(path), Some(ref local)) => {
                local
                    .read(path.file_name().unwrap().to_str().unwrap())
                    .await?
            }
            (Location::Remote(loc), _) => self.operator.read(loc).await?,
        };

        // Record statistics.
        record_read_profile(location, &instant, data.len());

        // Deserialize partitioned data block.
        let mut partitioned_data = Vec::with_capacity(partitions.len());
        for (partition_id, Chunk { range, layout }) in partitions {
            let block = deserialize_block(layout, data.slice(range.clone()))?;
            partitioned_data.push((*partition_id, block));
        }

        Ok(partitioned_data)
    }

    pub async fn read_chunk(&self, location: &Location, chunk: &Chunk) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let Chunk { range, layout } = chunk;
        let data_range = range.start as u64..range.end as u64;

        let data = match location {
            Location::Local(path) => match &self.local_operator {
                Some(ref local) => {
                    local
                        .read_with(path.file_name().unwrap().to_str().unwrap())
                        .range(data_range)
                        .await?
                }
                None => {
                    let (buf, range) = dma_read_file_range(path, data_range).await?;
                    Buffer::from(dma_buffer_to_bytes(buf)).slice(range)
                }
            },
            Location::Remote(loc) => self.operator.read_with(loc).range(data_range).await?,
        };

        record_read_profile(location, &instant, data.len());

        deserialize_block(layout, data)
    }

    pub async fn spill_stream_aggregate_buffer(
        &self,
        location: Option<String>,
        write_data: Vec<Vec<Vec<u8>>>,
    ) -> Result<(String, usize)> {
        let mut write_bytes = 0;
        let location = location.unwrap_or_else(|| self.create_unique_location());

        let mut writer = self
            .operator
            .writer_with(&location)
            .chunk(8 * 1024 * 1024)
            .await?;
        for write_bucket_data in write_data.into_iter() {
            for data in write_bucket_data.into_iter() {
                write_bytes += data.len();
                writer.write(data).await?;
            }
        }

        writer.close().await?;
        self.adapter.add_spill_file(
            Location::Remote(location.clone()),
            Layout::Aggregate,
            write_bytes,
        );
        Ok((location, write_bytes))
    }

    pub(crate) fn private_spilled_files(&self) -> Vec<Location> {
        self.adapter
            .private_spilled_files
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

pub struct MergedPartition {
    pub location: Location,
    pub partitions: Vec<(usize, Chunk)>,
}

pub struct Chunk {
    pub range: Range<usize>,
    pub layout: Layout,
}

const WINDOW_SPILL_BUFFER_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const WINDOW_SPILL_BUFFER_WORKERS: usize = 2;
const WINDOW_SPILL_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct WindowPartitionSpillWriter {
    spiller: Spiller,
    buffer_pool: Arc<BufferPool>,
    dio: bool,
    chunk_size: usize,
    schema: Arc<DataSchema>,
    file_writer: Option<FileWriter<UnionFileWriter>>,
}

pub struct WindowPartitionSpillReader {
    spiller: Spiller,
    schema: Arc<DataSchema>,
    parquet_metadata: Arc<ParquetMetaData>,
    union_file: Option<UnionFile>,
    dio: bool,
}

#[async_trait::async_trait]
impl WindowSpillWriter for WindowPartitionSpillWriter {
    type Reader = WindowPartitionSpillReader;

    async fn spill(&mut self, blocks: Vec<DataBlock>) -> Result<usize> {
        let file_writer = match &mut self.file_writer {
            Some(file_writer) => file_writer,
            file_writer @ None => {
                let writer = self
                    .spiller
                    .new_file_writer(&self.schema, &self.buffer_pool, self.dio, self.chunk_size)
                    .await?;
                file_writer.insert(writer)
            }
        };

        let row_group_meta = file_writer.spill(blocks)?;
        let ordinal = row_group_meta.ordinal().unwrap();
        Ok(ordinal as _)
    }

    async fn close(self) -> Result<WindowPartitionSpillReader> {
        let Some(file_writer) = self.file_writer else {
            return Err(ErrorCode::Internal(
                "attempted to close window spill writer without data".to_string(),
            ));
        };

        let (metadata, union_file) = file_writer.finish()?;
        let remote_path = union_file.remote_path().to_string();
        let parquet_metadata = Arc::new(metadata);

        let total_size = parquet_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size().max(0) as usize)
            .sum();

        self.spiller.adapter.add_spill_file(
            Location::Remote(remote_path),
            Layout::Parquet,
            total_size,
        );

        Ok(WindowPartitionSpillReader {
            spiller: self.spiller,
            schema: self.schema,
            parquet_metadata,
            union_file: Some(union_file),
            dio: self.dio,
        })
    }
}

#[async_trait::async_trait]
impl WindowSpillReader for WindowPartitionSpillReader {
    async fn restore(&mut self, ordinals: Vec<usize>) -> Result<Vec<DataBlock>> {
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }

        let union_file = self.union_file.take().ok_or_else(|| {
            ErrorCode::Internal("window spill reader already consumed".to_string())
        })?;

        self.spiller
            .load_row_groups(
                union_file,
                self.parquet_metadata.clone(),
                &self.schema,
                ordinals,
                self.dio,
            )
            .await
    }
}

#[async_trait::async_trait]
impl WindowSpillBuilder for Spiller {
    type Writer = WindowPartitionSpillWriter;

    async fn create(
        &self,
        schema: Arc<DataSchema>,
    ) -> Result<<Self as WindowSpillBuilder>::Writer> {
        if !self.use_parquet {
            return Err(ErrorCode::Internal(
                "window spill requires Parquet spill format, please set `set global spilling_file_format='parquet'`"
                    .to_string(),
            ));
        }

        let runtime = GlobalIORuntime::instance();
        let buffer_pool = BufferPool::create(
            runtime,
            WINDOW_SPILL_BUFFER_MEMORY_BYTES,
            WINDOW_SPILL_BUFFER_WORKERS,
        );

        Ok(WindowPartitionSpillWriter {
            spiller: self.clone(),
            buffer_pool,
            dio: self.temp_dir.is_some(),
            chunk_size: WINDOW_SPILL_CHUNK_SIZE,
            schema,
            file_writer: None,
        })
    }
}

impl SpillAdapter for Arc<QueryContext> {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        self.as_ref().add_spill_file(location, layout, size);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.as_ref().get_spill_layout(location)
    }
}

pub struct SortAdapter {
    ctx: Arc<QueryContext>,
    local_files: Arc<RwLock<HashMap<TempPath, Layout>>>,
}

impl SpillAdapter for SortAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        match location {
            Location::Remote(_) => self.ctx.as_ref().add_spill_file(location, layout, size),
            Location::Local(temp_path) => {
                self.local_files.write().unwrap().insert(temp_path, layout);
            }
        }
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        match location {
            Location::Remote(_) => self.ctx.as_ref().get_spill_layout(location),
            Location::Local(temp_path) => self.local_files.read().unwrap().get(temp_path).cloned(),
        }
    }
}

#[derive(Clone)]
pub struct SortSpiller(Arc<SpillerInner<SortAdapter>>);

#[async_trait::async_trait]
impl DataBlockSpill for SortSpiller {
    async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        self.0.spill(data_block).await
    }

    async fn restore(&self, location: &Location) -> Result<DataBlock> {
        self.0.read_spilled_file(location).await
    }
}

impl SortSpiller {
    pub fn new(ctx: Arc<QueryContext>, operator: Operator, config: SpillerConfig) -> Result<Self> {
        Ok(SortSpiller(Arc::new(SpillerInner::new(
            SortAdapter {
                ctx,
                local_files: Default::default(),
            },
            operator,
            config,
        )?)))
    }

    // todo: We need to drop [TempPath] earlier
    pub fn remove_local_file(&self, local: &TempPath) -> Option<Layout> {
        self.0.adapter.local_files.write().unwrap().remove(local)
    }
}

#[derive(Clone, Default)]
pub struct LiteAdapter {
    files: Arc<RwLock<HashMap<Location, Layout>>>,
}

impl SpillAdapter for LiteAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, _: usize) {
        self.files.write().unwrap().insert(location, layout);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.files.read().unwrap().get(location).cloned()
    }
}

#[derive(Clone)]
pub struct LiteSpiller(Arc<SpillerInner<LiteAdapter>>);

impl LiteSpiller {
    pub fn new(operator: Operator, config: SpillerConfig) -> Result<LiteSpiller> {
        Ok(LiteSpiller(Arc::new(SpillerInner::new(
            Default::default(),
            operator,
            config,
        )?)))
    }

    pub async fn cleanup(self) -> Result<()> {
        let files = std::mem::take(self.0.adapter.files.write().unwrap().deref_mut());
        let files: Vec<_> = files
            .into_keys()
            .filter_map(|location| match location {
                Location::Remote(path) => Some(path),
                Location::Local(_) => None,
            })
            .collect();
        let op = self.0.local_operator.as_ref().unwrap_or(&self.0.operator);

        op.delete_iter(files).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataBlockSpill for LiteSpiller {
    async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        self.0.spill(data_block).await
    }

    async fn restore(&self, location: &Location) -> Result<DataBlock> {
        self.0.read_spilled_file(location).await
    }
}
