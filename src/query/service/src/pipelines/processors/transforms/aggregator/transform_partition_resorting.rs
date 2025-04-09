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

use std::cmp::Ordering;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

pub struct ResortingPartition {
    global_max_partition: AtomicUsize,
}

impl ResortingPartition {
    pub fn create() -> Self {
        ResortingPartition {
            global_max_partition: AtomicUsize::new(0),
        }
    }

    fn block_number(meta: &AggregateMeta) -> (isize, usize) {
        (meta.get_sorting_partition(), meta.get_max_partition())
    }
}

impl Exchange for ResortingPartition {
    const NAME: &'static str = "PartitionResorting";
    const MULTIWAY_SORT: bool = true;

    fn partition(&self, mut data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        debug_assert_eq!(n, 1);

        let Some(meta) = data_block.take_meta() else {
            return Ok(vec![data_block]);
        };

        let Some(_) = AggregateMeta::downcast_ref_from(&meta) else {
            return Ok(vec![data_block]);
        };

        let global_max_partition = self.global_max_partition.load(AtomicOrdering::SeqCst);
        let mut meta = AggregateMeta::downcast_from(meta).unwrap();
        meta.set_global_max_partition(global_max_partition);

        Ok(vec![data_block.add_meta(Some(Box::new(meta)))?])
    }

    fn init_way(
        &self,
        _index: usize,
        first_data: &DataBlock,
    ) -> databend_common_exception::Result<()> {
        let max_partition = match first_data.get_meta() {
            None => 0,
            Some(meta) => match AggregateMeta::downcast_ref_from(meta) {
                None => 0,
                Some(v) => v.get_global_max_partition(),
            },
        };

        self.global_max_partition
            .fetch_max(max_partition, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn sorting_function(left_block: &DataBlock, right_block: &DataBlock) -> Ordering {
        let Some(left_meta) = left_block.get_meta() else {
            return Ordering::Equal;
        };
        let Some(left_meta) = AggregateMeta::downcast_ref_from(left_meta) else {
            return Ordering::Equal;
        };

        let Some(right_meta) = right_block.get_meta() else {
            return Ordering::Equal;
        };
        let Some(right_meta) = AggregateMeta::downcast_ref_from(right_meta) else {
            return Ordering::Equal;
        };

        let (l_partition, l_max_partition) = ResortingPartition::block_number(left_meta);
        let (r_partition, r_max_partition) = ResortingPartition::block_number(right_meta);

        // ORDER BY max_partition asc, partition asc, idx asc
        match l_max_partition.cmp(&r_max_partition) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => l_partition.cmp(&r_partition),
        }
    }
}
