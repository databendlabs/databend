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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::Alignment;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::GlobalUniqName;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::TempDir;
use opendal::services::Fs;
use opendal::Buffer;
use opendal::Operator;

use super::async_buffer::SpillTarget;
use super::serialize::*;
use super::Location;

/// Spiller type, currently only supports HashJoin
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpillerType {
    HashJoinBuild,
    HashJoinProbe,
    Window,
    OrderBy,
    Aggregation,
    ResultSet,
}

impl Display for SpillerType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SpillerType::HashJoinBuild => write!(f, "HashJoinBuild"),
            SpillerType::HashJoinProbe => write!(f, "HashJoinProbe"),
            SpillerType::Window => write!(f, "Window"),
            SpillerType::OrderBy => write!(f, "OrderBy"),
            SpillerType::Aggregation => write!(f, "Aggregation"),
            SpillerType::ResultSet => write!(f, "ResultSet"),
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
    temp_dir: Arc<TempDir>,
    local_operator: Option<Operator>,
}

impl SpillerDiskConfig {
    pub fn new(temp_dir: Arc<TempDir>, enable_dio: bool) -> Result<SpillerDiskConfig> {
        let local_operator = if !enable_dio {
            let builder = Fs::default().root(temp_dir.path().to_str().unwrap());
            Some(Operator::new(builder)?.finish())
        } else {
            None
        };
        Ok(SpillerDiskConfig {
            temp_dir,
            local_operator,
        })
    }
}

pub trait SpillAdapter: Send + Sync + 'static {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize);
    fn get_spill_layout(&self, location: &Location) -> Option<Layout>;
}

/// Spiller is a unified framework for operators which need to spill data from memory.
/// It provides the following features:
/// 1. Collection data that needs to be spilled.
/// 2. Partition data by the specified algorithm which specifies by operator
/// 3. Serialization and deserialization input data
/// 4. Interact with the underlying storage engine to write and read spilled data
#[derive(Clone)]
pub struct SpillerInner<A> {
    pub(super) adapter: A,
    pub(super) operator: Operator,
    location_prefix: String,
    pub(super) temp_dir: Option<Arc<TempDir>>,
    // for dio disabled
    pub(super) local_operator: Option<Operator>,
    pub(super) use_parquet: bool,
    _spiller_type: SpillerType,
}

impl<A> SpillerInner<A> {
    pub fn new(adapter: A, operator: Operator, config: SpillerConfig) -> Result<Self> {
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
            adapter,
            operator,
            location_prefix,
            temp_dir,
            local_operator,
            use_parquet,
            _spiller_type: spiller_type,
        })
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

    pub(super) fn new_location(&self, size: usize) -> Result<Location> {
        let location = match &self.temp_dir {
            None => None,
            Some(disk) => disk.new_file_with_size(size)?.map(Location::Local),
        }
        .unwrap_or(Location::Remote(self.create_unique_location()));
        Ok(location)
    }

    pub(super) async fn write_encodes(&self, size: usize, buf: DmaWriteBuf) -> Result<Location> {
        let location = self.new_location(size)?;

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

    pub(super) fn block_encoder(&self) -> BlocksEncoder {
        let align = self
            .temp_dir
            .as_ref()
            .map(|dir| dir.block_alignment())
            .unwrap_or(Alignment::MIN);
        BlocksEncoder::new(self.use_parquet, align, 8 * 1024 * 1024)
    }
}

impl<A: SpillAdapter> SpillerInner<A> {
    /// Spill some [`DataBlock`] to storage. These blocks will be concat into one.
    #[fastrace::trace(name = "Spiller::spill")]
    pub async fn spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        let (location, layout, data_size) = self.spill_unmanage(data_block).await?;

        // Record columns layout for spilled data.
        self.adapter
            .add_spill_file(location.clone(), layout, data_size);
        Ok(location)
    }

    /// Read a certain file to a [`DataBlock`].
    #[fastrace::trace(name = "Spiller::read_spilled_file")]
    pub async fn read_spilled_file(&self, location: &Location) -> Result<DataBlock> {
        let layout = self.adapter.get_spill_layout(location).unwrap();
        self.read_unmanage_spilled_file(location, &layout).await
    }
}

impl From<&Location> for SpillTarget {
    fn from(value: &Location) -> Self {
        if value.is_local() {
            SpillTarget::Local
        } else {
            SpillTarget::Remote
        }
    }
}

impl From<Location> for SpillTarget {
    fn from(value: Location) -> Self {
        (&value).into()
    }
}

fn record_spill_profile(
    locality: SpillTarget,
    start: &Instant,
    bytes: usize,
    local_count: ProfileStatisticsName,
    local_bytes: ProfileStatisticsName,
    local_time: ProfileStatisticsName,
    remote_count: ProfileStatisticsName,
    remote_bytes: ProfileStatisticsName,
    remote_time: ProfileStatisticsName,
) {
    match locality {
        SpillTarget::Local => {
            Profile::record_usize_profile(local_count, 1);
            Profile::record_usize_profile(local_bytes, bytes);
            Profile::record_usize_profile(local_time, start.elapsed().as_millis() as usize);
        }
        SpillTarget::Remote => {
            Profile::record_usize_profile(remote_count, 1);
            Profile::record_usize_profile(remote_bytes, bytes);
            Profile::record_usize_profile(remote_time, start.elapsed().as_millis() as usize);
        }
    }
}

pub fn record_write_profile<T: Into<SpillTarget>>(
    locality: T,
    start: &Instant,
    write_bytes: usize,
) {
    record_spill_profile(
        locality.into(),
        start,
        write_bytes,
        ProfileStatisticsName::LocalSpillWriteCount,
        ProfileStatisticsName::LocalSpillWriteBytes,
        ProfileStatisticsName::LocalSpillWriteTime,
        ProfileStatisticsName::RemoteSpillWriteCount,
        ProfileStatisticsName::RemoteSpillWriteBytes,
        ProfileStatisticsName::RemoteSpillWriteTime,
    );
}

pub fn record_read_profile<T: Into<SpillTarget>>(locality: T, start: &Instant, read_bytes: usize) {
    record_spill_profile(
        locality.into(),
        start,
        read_bytes,
        ProfileStatisticsName::LocalSpillReadCount,
        ProfileStatisticsName::LocalSpillReadBytes,
        ProfileStatisticsName::LocalSpillReadTime,
        ProfileStatisticsName::RemoteSpillReadCount,
        ProfileStatisticsName::RemoteSpillReadBytes,
        ProfileStatisticsName::RemoteSpillReadTime,
    );
}
