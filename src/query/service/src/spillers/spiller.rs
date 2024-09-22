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

use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::DataBlock;
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
}

impl SpillerConfig {
    pub fn create(location_prefix: String) -> Self {
        Self { location_prefix }
    }
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
    config: SpillerConfig,
    _spiller_type: SpillerType,
    pub join_spilling_partition_bits: usize,
    /// 1 partition -> N partition files
    pub partition_location: HashMap<usize, Vec<String>>,
    /// Record columns layout for spilled data, will be used when read data from disk
    pub columns_layout: HashMap<String, Vec<u64>>,
    /// Record how many bytes have been spilled for each partition.
    pub partition_spilled_bytes: HashMap<usize, u64>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
        spiller_type: SpillerType,
    ) -> Result<Self> {
        let join_spilling_partition_bits = ctx.get_settings().get_join_spilling_partition_bits()?;
        Ok(Self {
            ctx: ctx.clone(),
            operator,
            config,
            _spiller_type: spiller_type,
            join_spilling_partition_bits,
            partition_location: Default::default(),
            columns_layout: Default::default(),
            partition_spilled_bytes: Default::default(),
        })
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.partition_location.keys().copied().collect()
    }

    /// Spill a [`DataBlock`] to storage.
    pub async fn spill(&mut self, data_block: DataBlock) -> Result<String> {
        // Serialize data block.
        let (data_size, columns_data, columns_layout) = self.serialize_data_block(data_block)?;

        // Spill data to storage.
        let instant = Instant::now();
        let unique_name = GlobalUniqName::unique();
        let location = format!("{}/{}", self.config.location_prefix, unique_name);
        let mut writer = self
            .operator
            .writer_with(&location)
            .chunk(8 * 1024 * 1024)
            .await?;
        for data in columns_data.into_iter() {
            writer.write(data).await?;
        }
        writer.close().await?;

        // Record statistics.
        Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, data_size as usize);
        Profile::record_usize_profile(
            ProfileStatisticsName::SpillWriteTime,
            instant.elapsed().as_millis() as usize,
        );

        // Record columns layout for spilled data.
        self.columns_layout.insert(location.clone(), columns_layout);

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
        let mut write_bytes = 0;
        let mut write_data = Vec::with_capacity(partitioned_data.len());
        let mut spilled_partitions = Vec::with_capacity(partitioned_data.len());
        for (partition_id, data_block) in partitioned_data.into_iter() {
            let begin = write_bytes;
            let (data_size, columns_data, columns_layout) =
                self.serialize_data_block(data_block)?;

            write_bytes += data_size;
            write_data.push(columns_data);
            spilled_partitions.push((partition_id, begin..write_bytes, columns_layout));
        }

        // Spill data to storage.
        let instant = Instant::now();
        let unique_name = GlobalUniqName::unique();
        let location = format!("{}/{}", self.config.location_prefix, unique_name);
        let mut writer = self
            .operator
            .writer_with(&location)
            .chunk(8 * 1024 * 1024)
            .await?;
        for write_bucket_data in write_data.into_iter() {
            for data in write_bucket_data.into_iter() {
                writer.write(data).await?;
            }
        }
        writer.close().await?;

        // Record statistics.
        Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, write_bytes as usize);
        Profile::record_usize_profile(
            ProfileStatisticsName::SpillWriteTime,
            instant.elapsed().as_millis() as usize,
        );

        Ok(SpilledData::MergedPartition {
            location,
            partitions: spilled_partitions,
        })
    }

    /// Read a certain file to a [`DataBlock`].
    /// We should guarantee that the file is managed by this spiller.
    pub async fn read_spilled_file(&self, file: &str) -> Result<DataBlock> {
        debug_assert!(self.columns_layout.contains_key(file));

        // Read spilled data from storage.
        let instant = Instant::now();
        let data = self.operator.read(file).await?.to_bytes();

        // Record statistics.
        Profile::record_usize_profile(ProfileStatisticsName::SpillReadCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::SpillReadBytes, data.len());
        Profile::record_usize_profile(
            ProfileStatisticsName::SpillReadTime,
            instant.elapsed().as_millis() as usize,
        );

        // Deserialize data block.
        let mut begin = 0;
        let mut columns = Vec::with_capacity(self.columns_layout.len());
        let columns_layout = self.columns_layout.get(file).unwrap();
        for column_layout in columns_layout.iter() {
            columns.push(
                deserialize_column(&data[begin as usize..(begin + column_layout) as usize])
                    .unwrap(),
            );
            begin += column_layout;
        }

        Ok(DataBlock::new_from_columns(columns))
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, p_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(files) = self.partition_location.get(p_id) {
            let mut spilled_data = Vec::with_capacity(files.len());
            for file in files.iter() {
                let block = self.read_spilled_file(file).await?;
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
            let data = self.operator.read(location).await?.to_bytes();

            // Record statistics.
            Profile::record_usize_profile(ProfileStatisticsName::SpillReadCount, 1);
            Profile::record_usize_profile(ProfileStatisticsName::SpillReadBytes, data.len());
            Profile::record_usize_profile(
                ProfileStatisticsName::SpillReadTime,
                instant.elapsed().as_millis() as usize,
            );

            // Deserialize partitioned data block.
            let mut partitioned_data = Vec::with_capacity(partitions.len());
            for (partition_id, range, columns_layout) in partitions.iter() {
                let mut begin = range.start;
                let mut columns = Vec::with_capacity(columns_layout.len());
                for column_layout in columns_layout.iter() {
                    columns.push(
                        deserialize_column(&data[begin as usize..(begin + column_layout) as usize])
                            .unwrap(),
                    );
                    begin += column_layout;
                }
                partitioned_data.push((*partition_id, DataBlock::new_from_columns(columns)));
            }
            return Ok(partitioned_data);
        }
        Ok(vec![])
    }

    pub async fn read_range(
        &self,
        location: &str,
        data_range: Range<u64>,
        columns_layout: &[u64],
    ) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let data = self
            .operator
            .read_with(location)
            .range(data_range)
            .await?
            .to_vec();

        // Record statistics.
        Profile::record_usize_profile(ProfileStatisticsName::SpillReadCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::SpillReadBytes, data.len());
        Profile::record_usize_profile(
            ProfileStatisticsName::SpillReadTime,
            instant.elapsed().as_millis() as usize,
        );

        // Deserialize data block.
        let mut begin = 0;
        let mut columns = Vec::with_capacity(columns_layout.len());
        for column_layout in columns_layout.iter() {
            columns.push(
                deserialize_column(&data[begin as usize..(begin + column_layout) as usize])
                    .unwrap(),
            );
            begin += column_layout;
        }

        Ok(DataBlock::new_from_columns(columns))
    }

    pub(crate) fn spilled_files(&self) -> Vec<String> {
        self.columns_layout.keys().cloned().collect()
    }

    // Serialize data block to (data_size, columns_data, columns_layout).
    fn serialize_data_block(&self, data_block: DataBlock) -> Result<(u64, Vec<Vec<u8>>, Vec<u64>)> {
        let num_columns = data_block.num_columns();
        let mut data_size = 0;
        let mut columns_data = Vec::with_capacity(num_columns);
        let mut columns_layout = Vec::with_capacity(num_columns);

        for column in data_block.columns() {
            let column = column
                .value
                .convert_to_full_column(&column.data_type, data_block.num_rows());
            let column_data = serialize_column(&column);

            data_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }
        Ok((data_size, columns_data, columns_layout))
    }
}

pub enum SpilledData {
    Partition(String),
    MergedPartition {
        location: String,
        partitions: Vec<(usize, Range<u64>, Vec<u64>)>,
    },
}
