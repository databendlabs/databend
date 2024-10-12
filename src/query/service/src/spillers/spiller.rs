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

use buf_list::BufList;
use buf_list::Cursor;
use bytes::Buf;
use bytes::Bytes;
use databend_common_base::base::dma_buffer_as_vec;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::Alignment;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::arrow::read_column;
use databend_common_expression::arrow::write_column;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use opendal::Buffer;
use opendal::Operator;

use crate::sessions::QueryContext;

/// Spiller type, currently only supports HashJoin
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpillerType {
    HashJoinBuild,
    HashJoinProbe,
    Window,
    OrderBy,
    // Todo: Add more spillers type
    // Aggregation
}

impl Display for SpillerType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SpillerType::HashJoinBuild => write!(f, "HashJoinBuild"),
            SpillerType::HashJoinProbe => write!(f, "HashJoinProbe"),
            SpillerType::Window => write!(f, "Window"),
            SpillerType::OrderBy => write!(f, "OrderBy"),
        }
    }
}

/// Spiller configuration
#[derive(Clone)]
pub struct SpillerConfig {
    pub location_prefix: String,
    pub disk_spill: Option<SpillerDiskConfig>,
    pub spiller_type: SpillerType,
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
    _spiller_type: SpillerType,
    pub join_spilling_partition_bits: usize,
    /// 1 partition -> N partition files
    pub partition_location: HashMap<usize, Vec<Location>>,
    /// Record columns layout for spilled data, will be used when read data from disk
    pub columns_layout: HashMap<Location, Vec<usize>>,
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
            _spiller_type: spiller_type,
            join_spilling_partition_bits: settings.get_join_spilling_partition_bits()?,
            partition_location: Default::default(),
            columns_layout: Default::default(),
            partition_spilled_bytes: Default::default(),
        })
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.partition_location.keys().copied().collect()
    }

    /// Spill a [`DataBlock`] to storage.
    pub async fn spill(&mut self, data_block: DataBlock) -> Result<Location> {
        let instant = Instant::now();

        // Spill data to storage.
        let mut encoder = self.block_encoder();
        encoder.add_block(data_block);
        let data_size = encoder.size();
        let BlocksEncoder {
            buf,
            mut columns_layout,
            ..
        } = encoder;

        let location = self.write_encodes(data_size, buf).await?;

        // Record statistics.
        match location {
            Location::Remote(_) => record_remote_write_profile(&instant, data_size),
            Location::Local(_) => record_local_write_profile(&instant, data_size),
        }

        // Record columns layout for spilled data.
        self.columns_layout
            .insert(location.clone(), columns_layout.pop().unwrap());

        Ok(location)
    }

    #[async_backtrace::framed]
    /// Spill data block with partition
    pub async fn spill_with_partition(
        &mut self,
        partition_id: usize,
        data: DataBlock,
    ) -> Result<()> {
        let progress_val = ProgressValues {
            rows: data.num_rows(),
            bytes: data.memory_size(),
        };

        self.partition_spilled_bytes
            .entry(partition_id)
            .and_modify(|bytes| {
                *bytes += data.memory_size() as u64;
            })
            .or_insert(data.memory_size() as u64);

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
        partitioned_data: Vec<(usize, DataBlock)>,
    ) -> Result<SpilledData> {
        // Serialize data block.
        let mut encoder = self.block_encoder();
        let mut partition_ids = Vec::new();
        for (partition_id, data_block) in partitioned_data.into_iter() {
            partition_ids.push(partition_id);
            encoder.add_block(data_block);
        }

        let write_bytes = encoder.size();
        let BlocksEncoder {
            buf,
            offsets,
            columns_layout,
            ..
        } = encoder;

        let partitions = partition_ids
            .into_iter()
            .zip(
                offsets
                    .windows(2)
                    .map(|x| x[0]..x[1])
                    .zip(columns_layout.into_iter()),
            )
            .map(|(id, (range, layout))| (id, range, layout))
            .collect();

        // Spill data to storage.
        let instant = Instant::now();
        let location = self.write_encodes(write_bytes, buf).await?;

        // Record statistics.
        match location {
            Location::Remote(_) => record_remote_write_profile(&instant, write_bytes),
            Location::Local(_) => record_local_write_profile(&instant, write_bytes),
        }

        Ok(SpilledData::MergedPartition {
            location,
            partitions,
        })
    }

    /// Read a certain file to a [`DataBlock`].
    /// We should guarantee that the file is managed by this spiller.
    pub async fn read_spilled_file(&self, location: &Location) -> Result<DataBlock> {
        let columns_layout = self.columns_layout.get(location).unwrap();

        // Read spilled data from storage.
        let instant = Instant::now();
        let data = match (location, &self.local_operator) {
            (Location::Local(path), None) => {
                debug_assert_eq!(path.size(), columns_layout.iter().sum::<usize>());
                let file_size = path.size();
                let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                Buffer::from(dma_buffer_as_vec(buf)).slice(range)
            }
            (Location::Local(path), Some(ref local)) => {
                debug_assert_eq!(path.size(), columns_layout.iter().sum::<usize>());
                local
                    .read(path.file_name().unwrap().to_str().unwrap())
                    .await?
            }
            (Location::Remote(loc), _) => self.operator.read(loc).await?,
        };

        match location {
            Location::Remote(_) => record_remote_read_profile(&instant, data.len()),
            Location::Local(_) => record_local_read_profile(&instant, data.len()),
        }

        let block = deserialize_block(columns_layout, data);
        Ok(block)
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
        merged_partitions: &SpilledData,
    ) -> Result<Vec<(usize, DataBlock)>> {
        if let SpilledData::MergedPartition {
            location,
            partitions,
        } = merged_partitions
        {
            // Read spilled data from storage.
            let instant = Instant::now();

            let data = match (location, &self.local_operator) {
                (Location::Local(path), None) => {
                    let file_size = path.size();
                    debug_assert_eq!(
                        file_size,
                        if let Some((_, range, _)) = partitions.last() {
                            range.end
                        } else {
                            0
                        }
                    );

                    let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                    Buffer::from(dma_buffer_as_vec(buf)).slice(range)
                }
                (Location::Local(path), Some(ref local)) => {
                    local
                        .read(path.file_name().unwrap().to_str().unwrap())
                        .await?
                }
                (Location::Remote(loc), _) => self.operator.read(loc).await?,
            };

            // Record statistics.
            match location {
                Location::Remote(_) => record_remote_read_profile(&instant, data.len()),
                Location::Local(_) => record_local_read_profile(&instant, data.len()),
            };

            // Deserialize partitioned data block.
            let partitioned_data = partitions
                .iter()
                .map(|(partition_id, range, columns_layout)| {
                    let block = deserialize_block(columns_layout, data.slice(range.clone()));
                    (*partition_id, block)
                })
                .collect();

            return Ok(partitioned_data);
        }
        Ok(vec![])
    }

    pub async fn read_range(
        &self,
        location: &Location,
        data_range: Range<usize>,
        columns_layout: &[usize],
    ) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let data_range = data_range.start as u64..data_range.end as u64;

        let data = match (location, &self.local_operator) {
            (Location::Local(path), None) => {
                let (buf, range) = dma_read_file_range(path, data_range).await?;
                Buffer::from(dma_buffer_as_vec(buf)).slice(range)
            }
            (Location::Local(path), Some(ref local)) => {
                local
                    .read_with(path.file_name().unwrap().to_str().unwrap())
                    .range(data_range)
                    .await?
            }
            (Location::Remote(loc), _) => self.operator.read_with(loc).range(data_range).await?,
        };

        match location {
            Location::Remote(_) => record_remote_read_profile(&instant, data.len()),
            Location::Local(_) => record_local_read_profile(&instant, data.len()),
        }
        Ok(deserialize_block(columns_layout, data))
    }

    async fn write_encodes(&mut self, size: usize, buf: DmaWriteBuf) -> Result<Location> {
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
            .map(|x| Bytes::from(dma_buffer_as_vec(x)))
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
        BlocksEncoder::new(align, 8 * 1024 * 1024)
    }

    pub(crate) fn spilled_files(&self) -> Vec<Location> {
        self.columns_layout.keys().cloned().collect()
    }
}

pub enum SpilledData {
    Partition(Location),
    MergedPartition {
        location: Location,
        partitions: Vec<(usize, Range<usize>, Vec<usize>)>,
    },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Location {
    Remote(String),
    Local(TempPath),
}

struct BlocksEncoder {
    buf: DmaWriteBuf,
    offsets: Vec<usize>,
    columns_layout: Vec<Vec<usize>>,
}

impl BlocksEncoder {
    fn new(align: Alignment, chunk: usize) -> Self {
        Self {
            buf: DmaWriteBuf::new(align, chunk),
            offsets: vec![0],
            columns_layout: Vec::new(),
        }
    }

    fn add_block(&mut self, block: DataBlock) {
        let columns_layout = std::iter::once(self.size())
            .chain(block.columns().iter().map(|entry| {
                let column = entry
                    .value
                    .convert_to_full_column(&entry.data_type, block.num_rows());
                write_column(&column, &mut self.buf).unwrap();
                self.size()
            }))
            .map_windows(|x: &[_; 2]| x[1] - x[0])
            .collect();

        self.columns_layout.push(columns_layout);
        self.offsets.push(self.size())
    }

    fn size(&self) -> usize {
        self.buf.size()
    }
}

pub fn deserialize_block(columns_layout: &[usize], mut data: Buffer) -> DataBlock {
    let columns = columns_layout
        .iter()
        .map(|&layout| {
            let ls = BufList::from_iter(data.slice(0..layout));
            data.advance(layout);
            let mut cursor = Cursor::new(ls);
            read_column(&mut cursor).unwrap()
        })
        .collect::<Vec<_>>();

    DataBlock::new_from_columns(columns)
}

pub fn record_remote_write_profile(start: &Instant, write_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteBytes, write_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::RemoteSpillWriteTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_remote_read_profile(start: &Instant, read_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::RemoteSpillReadTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_local_write_profile(start: &Instant, write_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteBytes, write_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::LocalSpillWriteTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_local_read_profile(start: &Instant, read_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadBytes, read_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::LocalSpillReadTime,
        start.elapsed().as_millis() as usize,
    );
}
