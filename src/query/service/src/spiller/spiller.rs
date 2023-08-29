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
use common_expression::arrow::serialize_column;
use common_expression::DataBlock;
use opendal::Operator;
use parking_lot::RwLock;

/// Spiller type, currently only supports HashJoin
enum SpillerType {
    HashJoin, /* Todo: Add more spiller type
               * OrderBy
               * Aggregation */
}

/// Spiller configuration
pub struct SpillerConfig {
    location_prefix: String,
}

impl SpillerConfig {
    pub fn create(location_prefix: String) -> Self {
        Self { location_prefix }
    }
}

/// Spiller state
pub struct SpillerState {}

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
    spiller_state: SpillerState,
    /// DataBlocks need to be spilled for the processor
    pub(crate) input_data: Vec<DataBlock>,
    /// Partition set, which records there are how many partitions.
    pub(crate) partition_set: Vec<u8>,
    /// Spilled partition set, after one partition is spilled, it will be added to this set.
    pub(crate) spilled_partition_set: HashSet<u8>,
    /// Key is partition id, value is rows which have same partition id
    pub(crate) partitions: Vec<(u8, DataBlock)>,
    /// Record the location of the spilled partitions
    pub(crate) partition_location: HashMap<u8, String>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(operator: Operator, config: SpillerConfig) -> Self {
        let spiller_type = SpillerType::HashJoin;
        Self {
            operator,
            config,
            spiller_type,
            spiller_state: SpillerState {},
            input_data: Default::default(),
            partition_set: vec![],
            spilled_partition_set: Default::default(),
            partitions: vec![],
            partition_location: Default::default(),
        }
    }

    /// Spill partition set
    pub async fn spill(&mut self) -> Result<()> {
        for (partition_id, partition) in self.partitions.iter() {
            let unique_name = GlobalUniqName::unique();
            let location = format!("{}/{}", self.config.location_prefix, unique_name);
            // Record the location of the spilled partition
            self.partition_location
                .insert(*partition_id, location.clone());
            self.spilled_partition_set.insert(*partition_id);
            let columns = partition.columns().to_vec();
            let mut columns_data = Vec::with_capacity(columns.len());
            for column in columns.into_iter() {
                let column = column.value.as_column().unwrap();
                let column_data = serialize_column(column);
                columns_data.push(column_data);
            }

            let mut writer = self.operator.writer(location.as_str()).await?;
            for data in columns_data.into_iter() {
                writer.write(data).await?;
            }
            writer.close().await?;
        }
        Ok(())
    }

    /// Spill data block with location
    pub async fn spill_with_location(&mut self, loc: &str, data: &DataBlock) -> Result<()> {
        let mut writer = self.operator.writer(loc).await?;
        let columns = data.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            columns_data.push(column_data);
        }
        for data in columns_data.into_iter() {
            writer.write(data).await?;
        }
        writer.close().await?;
        Ok(())
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
