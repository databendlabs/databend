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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_storages_common_cache::ParquetMetaData;
use databend_storages_common_cache::TempPath;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::RowGroupMetaDataPtr;

use super::block_reader::BlocksReader;
use super::block_writer::BlocksWriter;
use super::inner::*;
use super::row_group_encoder::*;
use super::serialize::*;
use super::{Location, SpillsBufferPool};
use crate::sessions::QueryContext;

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

        if location.is_remote() {
            self.ctx.as_ref().incr_spill_progress(1, size);
        }
        self.ctx.as_ref().add_spill_file(location, layout);
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
        record_write_profile(location.is_local(), &instant, write_bytes);

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
        record_read_profile(location.is_local(), &instant, data.len());

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

        record_read_profile(location.is_local(), &instant, data.len());

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

#[derive(Clone)]
pub struct BackpressureAdapter {
    ctx: Arc<QueryContext>,
    buffer_pool: Arc<SpillsBufferPool>,
    chunk_size: usize,
}

impl BackpressureAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout) {
        if location.is_remote() {
            self.ctx
                .as_ref()
                .add_spill_file(location.clone(), layout.clone());
        }
    }

    fn update_progress(&self, file: usize, bytes: usize) {
        self.ctx.as_ref().incr_spill_progress(file, bytes);
    }
}

pub type BackpressureSpiller = SpillerInner<BackpressureAdapter>;

impl BackpressureSpiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
        buffer_pool: Arc<SpillsBufferPool>,
        chunk_size: usize,
    ) -> Result<Self> {
        Self::new(
            BackpressureAdapter {
                ctx,
                buffer_pool,
                chunk_size,
            },
            operator,
            config,
        )
    }

    pub fn new_writer_creator(&self, schema: Arc<DataSchema>) -> Result<WriterCreator> {
        let props = Properties::new(&schema)?;
        Ok(WriterCreator {
            spiller: self.clone(),
            chunk_size: self.adapter.chunk_size,
            schema,
            props,
        })
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

pub struct WriterCreator {
    spiller: BackpressureSpiller,
    chunk_size: usize,
    schema: Arc<DataSchema>,
    props: Properties,
}

impl WriterCreator {
    pub async fn open(&mut self, local_file_size: Option<usize>) -> Result<SpillWriter> {
        let writer = self
            .spiller
            .new_file_writer(
                &self.props,
                &self.spiller.adapter.buffer_pool,
                self.chunk_size,
                local_file_size,
            )
            .await?;
        self.spiller.adapter.update_progress(1, 0);

        Ok(SpillWriter {
            spiller: self.spiller.clone(),
            schema: self.schema.clone(),
            file_writer: writer,
        })
    }

    pub fn new_encoder(&self) -> RowGroupEncoder {
        self.props.new_encoder()
    }
}

pub struct SpillWriter {
    spiller: BackpressureSpiller,
    schema: Arc<DataSchema>,
    file_writer: AnyFileWriter,
}

impl SpillWriter {
    pub const MAX_ORDINAL: usize = 2 << 15;

    pub fn file_writer(&self) -> &AnyFileWriter {
        &self.file_writer
    }

    pub fn add_row_group(&mut self, blocks: Vec<DataBlock>) -> Result<usize> {
        let mut encoder = self.new_row_group_encoder();
        for block in blocks {
            encoder.add(block)?;
        }

        let row_group_meta = self.add_encoded_row_group(encoder)?;
        Ok(row_group_meta.ordinal().unwrap() as _)
    }

    pub fn add_encoded_row_group(
        &mut self,
        row_group: RowGroupEncoder,
    ) -> Result<RowGroupMetaDataPtr> {
        let start = std::time::Instant::now();

        match &mut self.file_writer {
            AnyFileWriter::Local(file_writer) => {
                let row_group_meta = file_writer.flush_row_group(row_group)?;
                let size = row_group_meta.compressed_size() as _;
                self.spiller.adapter.update_progress(0, size);
                record_write_profile(true, &start, size);
                Ok(row_group_meta)
            }
            AnyFileWriter::Remote(_, file_writer) => {
                let row_group_meta = file_writer.flush_row_group(row_group)?;
                let size = row_group_meta.compressed_size() as _;
                self.spiller.adapter.update_progress(0, size);
                record_write_profile(false, &start, size);
                Ok(row_group_meta)
            }
        }
    }

    pub fn new_row_group_encoder(&self) -> RowGroupEncoder {
        self.file_writer.new_row_group()
    }

    pub fn close(self) -> Result<SpillReader> {
        let (metadata, location) = match self.file_writer {
            AnyFileWriter::Local(file_writer) => {
                let (metadata, path) = file_writer.finish()?;
                self.spiller
                    .adapter
                    .add_spill_file(Location::Local(path.clone()), Layout::Parquet);
                (metadata, Location::Local(path))
            }
            AnyFileWriter::Remote(path, file_writer) => {
                let (metadata, _) = file_writer.finish()?;
                let location = Location::Remote(path);

                self.spiller
                    .adapter
                    .add_spill_file(location.clone(), Layout::Parquet);

                (metadata, location)
            }
        };

        Ok(SpillReader {
            spiller: self.spiller,
            schema: self.schema,
            parquet_metadata: Arc::new(metadata),
            location,
        })
    }
}

pub struct SpillReader {
    spiller: BackpressureSpiller,
    schema: Arc<DataSchema>,
    parquet_metadata: Arc<ParquetMetaData>,
    location: Location,
}

impl SpillReader {
    pub async fn restore(&mut self, row_groups: Vec<usize>) -> Result<Vec<DataBlock>> {
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }
        let start = std::time::Instant::now();

        let blocks = self
            .spiller
            .load_row_groups(
                &self.location,
                self.parquet_metadata.clone(),
                &self.schema,
                row_groups,
            )
            .await?;

        record_read_profile(
            self.location.is_local(),
            &start,
            blocks.iter().map(DataBlock::memory_size).sum(),
        );

        Ok(blocks)
    }
}

impl SpillAdapter for Arc<QueryContext> {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        if matches!(location, Location::Remote(_)) {
            self.incr_spill_progress(1, size);
        }
        self.as_ref().add_spill_file(location, layout);
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
            Location::Remote(_) => {
                self.ctx.as_ref().incr_spill_progress(1, size);
                self.ctx.as_ref().add_spill_file(location, layout);
            }
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
        self.0.operator.delete_iter(files).await?;
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
