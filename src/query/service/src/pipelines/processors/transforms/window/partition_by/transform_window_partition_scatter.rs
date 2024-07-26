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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::group_hash_columns_slice;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use super::WindowPartitionMeta;
use crate::sessions::QueryContext;

pub static PARTITION_COUNT: usize = 256;

#[derive(Default)]
pub struct PartitionHashTable {
    pub buckets_blocks: BTreeMap<usize, Vec<DataBlock>>,
    hash_keys: Vec<usize>,
    allocated_bytes: usize,
}

impl PartitionHashTable {
    pub fn new(hash_keys: Vec<usize>) -> Self {
        Self {
            buckets_blocks: BTreeMap::new(),
            hash_keys,
            allocated_bytes: 0,
        }
    }

    pub fn add_block(&mut self, block: DataBlock) -> Result<()> {
        let num_rows = block.num_rows();

        let hash_cols = self
            .hash_keys
            .iter()
            .map(|&offset| {
                let entry = block.get_by_offset(offset);
                match &entry.value {
                    Value::Scalar(s) => {
                        ColumnBuilder::repeat(&s.as_ref(), num_rows, &entry.data_type).build()
                    }
                    Value::Column(c) => c.clone(),
                }
            })
            .collect::<Vec<_>>();

        let mut hashes = vec![0u64; num_rows];
        group_hash_columns_slice(&hash_cols, &mut hashes);

        let indices = hashes
            .iter()
            .map(|&hash| (hash % PARTITION_COUNT as u64) as u8)
            .collect::<Vec<_>>();
        let scatter_blocks = DataBlock::scatter(&block, &indices, PARTITION_COUNT)?;
        debug_assert_eq!(scatter_blocks.len(), PARTITION_COUNT);

        for (bucket, block) in scatter_blocks.into_iter().enumerate() {
            self.allocated_bytes += block.memory_size();
            match self.buckets_blocks.entry(bucket) {
                Entry::Vacant(v) => {
                    v.insert(vec![block]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(block);
                }
            };
        }

        Ok(())
    }

    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }
}

pub fn convert_to_partitions(
    mut buckets_blocks: BTreeMap<usize, Vec<DataBlock>>,
) -> Result<Vec<DataBlock>> {
    let mut partitions = Vec::with_capacity(PARTITION_COUNT);
    while let Some((_, blocks)) = buckets_blocks.pop_first() {
        let payload = DataBlock::concat(&blocks)?;
        partitions.push(payload);
    }

    Ok(partitions)
}

struct WindowPartitionSettings {
    max_memory_usage: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for WindowPartitionSettings {
    type Error = ErrorCode;

    fn try_from(ctx: Arc<QueryContext>) -> std::result::Result<Self, Self::Error> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let mut memory_ratio =
            settings.get_window_partition_spilling_memory_ratio()? as f64 / 100_f64;

        if memory_ratio > 1_f64 {
            memory_ratio = 1_f64;
        }

        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => match memory_ratio {
                x if x == 0_f64 => usize::MAX,
                memory_ratio => (max_memory_usage as f64 * memory_ratio) as usize,
            },
        };

        Ok(WindowPartitionSettings {
            max_memory_usage,
            spilling_bytes_threshold_per_proc: match settings
                .get_window_partition_spilling_bytes_threshold_per_proc()?
            {
                0 => max_memory_usage / max_threads,
                spilling_bytes_threshold_per_proc => spilling_bytes_threshold_per_proc,
            },
        })
    }
}

pub struct TransformWindowPartitionScatter {
    hash_table: PartitionHashTable,
    settings: WindowPartitionSettings,
}

impl TransformWindowPartitionScatter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        hash_keys: Vec<usize>,
    ) -> Result<Box<dyn Processor>> {
        let hash_table = PartitionHashTable::new(hash_keys.clone());

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformWindowPartitionScatter {
                hash_table,
                settings: WindowPartitionSettings::try_from(ctx)?,
            },
        ))
    }
}

impl AccumulatingTransform for TransformWindowPartitionScatter {
    const NAME: &'static str = "TransformWindowPartitionScatter";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.hash_table.add_block(block)?;

        if self.hash_table.allocated_bytes() > self.settings.spilling_bytes_threshold_per_proc
            || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.settings.max_memory_usage
        {
            let hash_table = std::mem::take(&mut self.hash_table);
            let blocks = vec![DataBlock::empty_with_meta(
                WindowPartitionMeta::create_spilling(hash_table.buckets_blocks),
            )];

            self.hash_table = PartitionHashTable::new(hash_table.hash_keys);
            return Ok(blocks);
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let mut blocks = Vec::with_capacity(PARTITION_COUNT);
        let hash_table = std::mem::take(&mut self.hash_table);

        let partitions = convert_to_partitions(hash_table.buckets_blocks)?;
        for (bucket, block) in partitions.into_iter().enumerate() {
            if block.num_rows() != 0 {
                blocks.push(DataBlock::empty_with_meta(
                    WindowPartitionMeta::create_payload(bucket as isize, block),
                ));
            }
        }

        Ok(blocks)
    }
}
