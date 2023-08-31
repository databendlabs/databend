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

use common_base::base::GlobalUniqName;
use common_exception::Result;
use common_expression::arrow::deserialize_column;
use common_expression::arrow::serialize_column;
use common_expression::DataBlock;
use opendal::Operator;

/// Spiller type, currently only supports HashJoin
pub enum SpillerType {
    HashJoin, /* Todo: Add more spiller type
               * OrderBy
               * Aggregation */
}

/// Spiller configuration
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
pub struct Spiller {
    operator: Operator,
    config: SpillerConfig,
    spiller_type: SpillerType,
    /// Partition set, which records there are how many partitions.
    pub partition_set: Vec<u8>,
    /// Spilled partition set, after one partition is spilled, it will be added to this set.
    pub spilled_partition_set: HashSet<u8>,
    /// Key is partition id, value is rows which have same partition id
    pub partitions: Vec<(u8, DataBlock)>,
    /// Record the location of the spilled partitions
    /// 1 partition -> N partition files
    pub partition_location: HashMap<u8, Vec<String>>,
    /// Record columns layout for spilled data, will be used when read data from disk
    pub columns_layout: Vec<usize>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(operator: Operator, config: SpillerConfig, spiller_type: SpillerType) -> Self {
        Self {
            operator,
            config,
            spiller_type,
            partition_set: vec![],
            spilled_partition_set: Default::default(),
            partitions: vec![],
            partition_location: Default::default(),
            columns_layout: vec![],
        }
    }

    #[async_backtrace::framed]
    /// Spill partition set
    pub async fn spill(&mut self, partitions: &[(u8, DataBlock)]) -> Result<()> {
        for (partition_id, partition) in partitions.iter() {
            self.spilled_partition_set.insert(*partition_id);
            self.spill_with_partition(partition_id, partition).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    /// Spill data block with location
    pub async fn spill_with_partition(&mut self, p_id: &u8, data: &DataBlock) -> Result<()> {
        let unique_name = GlobalUniqName::unique();
        let location = format!("{}/{}", self.config.location_prefix, unique_name);
        self.partition_location
            .entry(*p_id)
            .and_modify(|locs| {
                locs.push(location.clone());
            })
            .or_insert(vec![location.clone()]);
        let mut writer = self.operator.writer(location.as_str()).await?;
        let columns = data.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            self.columns_layout.push(column_data.len());
            columns_data.push(column_data);
        }
        for data in columns_data.into_iter() {
            writer.write(data).await?;
        }
        writer.close().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_data(&self, p_id: &u8) -> Result<Vec<DataBlock>> {
        debug_assert!(self.partition_location.contains_key(p_id));
        let files = self.partition_location.get(p_id).unwrap();
        let mut spilled_data = Vec::with_capacity(files.len());
        // Make it parallel
        for file in files.iter() {
            let data = self.operator.read(file).await?;
            let mut begin = 0;
            let mut columns = Vec::with_capacity(self.columns_layout.len());
            for column_layout in self.columns_layout.iter() {
                columns.push(deserialize_column(&data[begin..begin + column_layout]).unwrap());
                begin += column_layout;
            }
            spilled_data.push(DataBlock::new_from_columns(columns));
        }
        Ok(spilled_data)
    }

    /// Check if all partitions have been spilled
    pub fn is_all_spilled(&self) -> bool {
        self.partition_set.len() == self.spilled_partition_set.len()
    }

    /// Check if any partition has been spilled
    pub fn is_any_spilled(&self) -> bool {
        !self.spilled_partition_set.is_empty()
    }
}
