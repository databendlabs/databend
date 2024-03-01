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
use std::sync::Arc;

use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_hashtable::hash2bucket;
use log::info;
use opendal::Operator;

use crate::sessions::QueryContext;

/// Spiller type, currently only supports HashJoin
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpillerType {
    HashJoinBuild,
    HashJoinProbe,
    OrderBy, /* Todo: Add more spillers type
              * Aggregation */
}

impl Display for SpillerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillerType::HashJoinBuild => write!(f, "HashJoinBuild"),
            SpillerType::HashJoinProbe => write!(f, "HashJoinProbe"),
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
    spiller_type: SpillerType,
    /// Record the location of the spilled partitions
    /// 1 partition -> 1 partition file
    pub partition_location: HashMap<u8, String>,
    /// Record columns layouts for spilled data, will be used when read data from disk
    pub columns_layouts: HashMap<String, Vec<Vec<usize>>>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
        spiller_type: SpillerType,
    ) -> Self {
        Self {
            ctx,
            operator,
            config,
            spiller_type,
            partition_location: Default::default(),
            columns_layouts: Default::default(),
        }
    }

    pub fn spilled_partitions(&self) -> HashSet<u8> {
        self.partition_location.keys().copied().collect()
    }

    /// Read a certain file to a [`DataBlock`].
    /// We should guarantee that the file is managed by this spiller.
    pub async fn read_spilled(&self, file: &str) -> Result<(DataBlock, u64)> {
        debug_assert!(self.columns_layouts.contains_key(file));
        let data = self.operator.read(file).await?;
        let bytes = data.len() as u64;

        let mut begin = 0;
        let mut columns = vec![];
        let columns_layouts = self.columns_layouts.get(file).unwrap();
        for (idx, columns_layout) in columns_layouts.iter().enumerate() {
            for (col_idx, column_layout) in columns_layout.iter().enumerate() {
                if idx == 0 {
                    columns.push(vec![
                        deserialize_column(&data[begin..begin + column_layout]).unwrap(),
                    ]);
                    begin += column_layout;
                    continue;
                }
                columns[col_idx]
                    .push(deserialize_column(&data[begin..begin + column_layout]).unwrap());
                begin += column_layout;
            }
        }
        // Concat `columns`
        let columns = columns
            .into_iter()
            .map(|v| Column::concat_columns(v.into_iter()))
            .collect::<Result<Vec<_>>>()?;
        let block = DataBlock::new_from_columns(columns);
        Ok((block, bytes))
    }

    /// Write a [`DataBlock`] to storage with specific location.
    pub async fn spill_block_with_location(
        &mut self,
        data: DataBlock,
        location: &str,
    ) -> Result<u64> {
        let mut write_bytes = 0;
        let mut writer = self
            .operator
            .writer_with(location)
            .append(true)
            .buffer(8 * 1024 * 1024)
            .await?;
        let columns = data.columns().to_vec();
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            columns_layout.push(column_data.len());
            write_bytes += column_data.len() as u64;
            columns_data.push(column_data);
        }
        self.columns_layouts
            .entry(location.to_string())
            .and_modify(|layouts| {
                layouts.push(columns_layout.clone());
            })
            .or_insert(vec![columns_layout]);
        for data in columns_data.into_iter() {
            writer.write(data).await?;
        }
        writer.close().await?;

        Ok(write_bytes)
    }

    /// Write a [`DataBlock`] to storage.
    /// Todo: change sort call
    pub async fn spill_block(&mut self, data: DataBlock) -> Result<(String, u64)> {
        let unique_name = GlobalUniqName::unique();
        let location = format!("{}/{}", self.config.location_prefix, unique_name);
        let mut write_bytes = 0;

        let mut writer = self
            .operator
            .writer_with(&location)
            .buffer(8 * 1024 * 1024)
            .await?;
        let columns = data.columns().to_vec();
        let mut columns_layout = Vec::with_capacity(columns.len());
        let mut columns_data = Vec::with_capacity(columns.len());
        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            columns_layout.push(column_data.len());
            write_bytes += column_data.len() as u64;
            columns_data.push(column_data);
        }
        self.columns_layouts
            .insert(location.clone(), vec![columns_layout]);
        for data in columns_data.into_iter() {
            writer.write(data).await?;
        }
        writer.close().await?;

        Ok((location, write_bytes))
    }

    #[async_backtrace::framed]
    /// Spill partition set
    pub async fn spill(
        &mut self,
        partitions: Vec<(u8, DataBlock)>,
        worker_id: usize,
    ) -> Result<()> {
        for (partition_id, partition) in partitions {
            self.spill_with_partition(partition_id, partition, worker_id)
                .await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    /// Spill data block with location
    pub async fn spill_with_partition(
        &mut self,
        p_id: u8,
        data: DataBlock,
        worker_id: usize,
    ) -> Result<()> {
        let progress_val = ProgressValues {
            rows: data.num_rows(),
            bytes: data.memory_size(),
        };

        if let Some(location) = self.partition_location.get(&p_id) {
            // Append data
            let _ = self
                .spill_block_with_location(data, location.clone().as_str())
                .await?;
        } else {
            let (location, _) = self.spill_block(data).await?;
            self.partition_location.insert(p_id, location);
        }

        self.ctx.get_join_spill_progress().incr(&progress_val);

        info!(
            "{:?} spilled {:?} rows data, partition id is {:?}, worker id is {:?}",
            self.spiller_type, progress_val.rows, p_id, worker_id
        );
        Ok(())
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_data(&self, p_id: &u8, worker_id: usize) -> Result<DataBlock> {
        debug_assert!(self.partition_location.contains_key(p_id));
        let file = self.partition_location.get(p_id).unwrap();
        let (block, _) = self.read_spilled(file).await?;
        info!(
            "{:?} read partition {:?}, work id: {:?}",
            self.spiller_type, p_id, worker_id
        );
        Ok(block)
    }

    #[async_backtrace::framed]
    // Directly spill input data without buffering.
    // Need to compute hashes for data block advanced.
    // For probe, only need to spill rows in build spilled partitions.
    pub(crate) async fn spill_input(
        &mut self,
        data_block: DataBlock,
        hashes: &[u64],
        spilled_partitions: Option<&HashSet<u8>>,
        worker_id: usize,
    ) -> Result<()> {
        // Key is partition, value is row indexes
        let mut partition_rows = HashMap::new();
        // Classify rows to spill or not spill.
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = hash2bucket::<3, false>(*hash as usize) as u8;
            if let Some(spilled_partitions) = spilled_partitions {
                if !spilled_partitions.contains(&partition_id) {
                    continue;
                }
            }
            // the row can be directly spilled to corresponding partition
            partition_rows
                .entry(partition_id)
                .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                .or_insert(vec![row_idx]);
        }
        for (p_id, row_indexes) in partition_rows.iter() {
            let block_row_indexes = row_indexes
                .iter()
                .map(|idx| (0_u32, *idx as u32, 1_usize))
                .collect::<Vec<_>>();
            let block = DataBlock::take_blocks(
                &[data_block.clone()],
                &block_row_indexes,
                row_indexes.len(),
            );
            // Spill block with partition id
            self.spill_with_partition(*p_id, block, worker_id).await?;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn spilled_files_num(&self, pid: u8) -> usize {
        self.partition_location[&pid].len()
    }
}
