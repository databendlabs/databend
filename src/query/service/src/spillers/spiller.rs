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
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::Alignment;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use opendal::Buffer;
use opendal::Operator;
use parking_lot::RwLock;

use super::serialize::*;
use crate::sessions::QueryContext;

/// Spiller type, currently only supports HashJoin
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpillerType {
    HashJoinBuild,
    HashJoinProbe,
    Window,
    OrderBy,
    Aggregation,
}

impl Display for SpillerType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SpillerType::HashJoinBuild => write!(f, "HashJoinBuild"),
            SpillerType::HashJoinProbe => write!(f, "HashJoinProbe"),
            SpillerType::Window => write!(f, "Window"),
            SpillerType::OrderBy => write!(f, "OrderBy"),
            SpillerType::Aggregation => write!(f, "Aggregation"),
        }
    }
}

/// Spiller configuration
#[derive(Clone)]
pub struct SpillerConfig {
    pub spiller_type: SpillerType,
    pub location_prefix: String,
    pub disk_spill: Option<SpillerDiskConfig>,
    pub use_parquet: bool,
}

#[derive(Clone)]
pub struct SpillerDiskConfig {
    pub temp_dir: Arc<TempDir>,
    pub local_operator: Option<Operator>,
}

/// Spiller is a unified framework for operators which need to spill data from memory.
/// It provides the following features:
/// 1. Collection data that needs to be spilled.
/// 2. Partition data by the specified algorithm which specifies by operator
/// 3. Serialization and deserialization input data
/// 4. Interact with the underlying storage engine to write and read spilled data
#[derive(Clone)]
pub struct Spiller {
    ctx: Arc<QueryContext>,
    operator: Operator,
    location_prefix: String,
    temp_dir: Option<Arc<TempDir>>,
    // for dio disabled
    local_operator: Option<Operator>,
    use_parquet: bool,
    _spiller_type: SpillerType,

    // Stores the spilled files that controlled by current spiller
    private_spilled_files: Arc<RwLock<HashMap<Location, Layout>>>,
    pub join_spilling_partition_bits: usize,
    /// 1 partition -> N partition files
    pub partition_location: HashMap<usize, Vec<Location>>,
    /// Record how many bytes have been spilled for each partition.
    pub partition_spilled_bytes: HashMap<usize, u64>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let SpillerConfig {
            location_prefix,
            disk_spill,
            spiller_type,
            use_parquet,
        } = config;

        let (temp_dir, local_operator) = match disk_spill {
            Some(SpillerDiskConfig {
                temp_dir,
                local_operator,
            }) => (Some(temp_dir), local_operator),
            None => (None, None),
        };

        Ok(Self {
            ctx,
            operator,
            location_prefix,
            temp_dir,
            local_operator,
            use_parquet,
            _spiller_type: spiller_type,
            private_spilled_files: Default::default(),
            join_spilling_partition_bits: settings.get_join_spilling_partition_bits()?,
            partition_location: Default::default(),
            partition_spilled_bytes: Default::default(),
        })
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.partition_location.keys().copied().collect()
    }

    /// Spill some [`DataBlock`] to storage. These blocks will be concat into one.
    pub async fn spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        let (location, layout, data_size) = self.spill_unmanage(data_block).await?;

        // Record columns layout for spilled data.
        self.ctx
            .add_spill_file(location.clone(), layout.clone(), data_size);
        self.private_spilled_files
            .write()
            .insert(location.clone(), layout);
        Ok(location)
    }

    async fn spill_unmanage(
        &self,
        data_block: Vec<DataBlock>,
    ) -> Result<(Location, Layout, usize)> {
        debug_assert!(!data_block.is_empty());
        let instant = Instant::now();

        // Spill data to storage.
        let mut encoder = self.block_encoder();
        encoder.add_blocks(data_block);
        let data_size = encoder.size();
        let BlocksEncoder {
            buf,
            mut columns_layout,
            ..
        } = encoder;

        let location = self.write_encodes(data_size, buf).await?;

        // Record statistics.
        record_write_profile(&location, &instant, data_size);
        let layout = columns_layout.pop().unwrap();
        Ok((location, layout, data_size))
    }

    pub fn create_unique_location(&self) -> String {
        format!("{}/{}", self.location_prefix, GlobalUniqName::unique())
    }

    pub async fn spill_stream_aggregate_buffer(
        &self,
        location: Option<String>,
        write_data: Vec<Vec<Vec<u8>>>,
    ) -> Result<(String, usize)> {
        let mut write_bytes = 0;
        let location = location
            .unwrap_or_else(|| format!("{}/{}", self.location_prefix, GlobalUniqName::unique()));

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
        self.ctx.add_spill_file(
            Location::Remote(location.clone()),
            Layout::Aggregate,
            write_bytes,
        );

        self.private_spilled_files
            .write()
            .insert(Location::Remote(location.clone()), Layout::Aggregate);
        Ok((location, write_bytes))
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

        self.partition_spilled_bytes
            .entry(partition_id)
            .and_modify(|bytes| {
                *bytes += memory_size as u64;
            })
            .or_insert(memory_size as u64);

        let location = self.spill(data).await?;
        self.partition_location
            .entry(partition_id)
            .and_modify(|locs| {
                locs.push(location.clone());
            })
            .or_insert(vec![location.clone()]);

        self.ctx.get_join_spill_progress().incr(&progress_val);
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

        self.ctx
            .add_spill_file(location.clone(), layout.clone(), write_bytes);
        self.private_spilled_files
            .write()
            .insert(location.clone(), layout);
        Ok(MergedPartition {
            location,
            partitions,
        })
    }

    /// Read a certain file to a [`DataBlock`].
    /// We should guarantee that the file is managed by this spiller.
    pub async fn read_spilled_file(&self, location: &Location) -> Result<DataBlock> {
        let layout = self.ctx.get_spill_layout(location).unwrap();
        self.read_unmanage_spilled_file(location, &layout).await
    }

    async fn read_unmanage_spilled_file(
        &self,
        location: &Location,
        columns_layout: &Layout,
    ) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let data = match location {
            Location::Local(path) => {
                match columns_layout {
                    Layout::ArrowIpc(layout) => {
                        debug_assert_eq!(path.size(), layout.iter().sum::<usize>())
                    }
                    Layout::Parquet => {}
                    Layout::Aggregate => {}
                }

                match self.local_operator {
                    Some(ref local) => {
                        local
                            .read(path.file_name().unwrap().to_str().unwrap())
                            .await?
                    }
                    None => {
                        let file_size = path.size();
                        let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                        Buffer::from(dma_buffer_to_bytes(buf)).slice(range)
                    }
                }
            }
            Location::Remote(loc) => self.operator.read(loc).await?,
        };

        record_read_profile(location, &instant, data.len());

        deserialize_block(columns_layout, data)
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, p_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(locs) = self.partition_location.get(p_id) {
            let mut spilled_data = Vec::with_capacity(locs.len());
            for loc in locs.iter() {
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

    async fn write_encodes(&self, size: usize, buf: DmaWriteBuf) -> Result<Location> {
        let location = match &self.temp_dir {
            None => None,
            Some(disk) => disk.new_file_with_size(size)?.map(Location::Local),
        }
        .unwrap_or(Location::Remote(format!(
            "{}/{}",
            self.location_prefix,
            GlobalUniqName::unique(),
        )));

        let mut writer = match (&location, &self.local_operator) {
            (Location::Local(path), None) => {
                let written = buf.into_file(path, true).await?;
                debug_assert_eq!(size, written);
                return Ok(location);
            }
            (Location::Local(path), Some(local)) => {
                local.writer_with(path.file_name().unwrap().to_str().unwrap())
            }
            (Location::Remote(loc), _) => self.operator.writer_with(loc),
        }
        .chunk(8 * 1024 * 1024)
        .await?;

        let buf = buf
            .into_data()
            .into_iter()
            .map(dma_buffer_to_bytes)
            .collect::<Buffer>();
        let written = buf.len();
        writer.write(buf).await?;
        writer.close().await?;

        debug_assert_eq!(size, written);
        Ok(location)
    }

    fn block_encoder(&self) -> BlocksEncoder {
        let align = self
            .temp_dir
            .as_ref()
            .map(|dir| dir.block_alignment())
            .unwrap_or(Alignment::MIN);
        BlocksEncoder::new(self.use_parquet, align, 8 * 1024 * 1024)
    }

    pub(crate) fn private_spilled_files(&self) -> Vec<Location> {
        let r = self.private_spilled_files.read();
        r.keys().cloned().collect()
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Location {
    Remote(String),
    Local(TempPath),
}

fn record_write_profile(location: &Location, start: &Instant, write_bytes: usize) {
    match location {
        Location::Remote(_) => {
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillWriteBytes,
                write_bytes,
            );
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillWriteTime,
                start.elapsed().as_millis() as usize,
            );
        }
        Location::Local(_) => {
            Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteBytes, write_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::LocalSpillWriteTime,
                start.elapsed().as_millis() as usize,
            );
        }
    }
}

fn record_read_profile(location: &Location, start: &Instant, read_bytes: usize) {
    match location {
        Location::Remote(_) => {
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillReadTime,
                start.elapsed().as_millis() as usize,
            );
        }
        Location::Local(_) => {
            Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadBytes, read_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::LocalSpillReadTime,
                start.elapsed().as_millis() as usize,
            );
        }
    }
}
