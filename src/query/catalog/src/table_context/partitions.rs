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

use databend_common_exception::Result;

use crate::plan::PartInfoPtr;
use crate::plan::PartStatistics;
use crate::plan::Partitions;

pub trait TableContextPartitionStats: Send + Sync {
    fn get_partition(&self) -> Option<PartInfoPtr> {
        unimplemented!()
    }

    fn get_partitions(&self, _num: usize) -> Vec<PartInfoPtr> {
        unimplemented!()
    }

    fn partition_num(&self) -> usize {
        unimplemented!()
    }

    fn set_partitions(&self, _partitions: Partitions) -> Result<()> {
        unimplemented!()
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        unimplemented!()
    }

    fn set_can_scan_from_agg_index(&self, _enable: bool) {
        unimplemented!()
    }

    fn get_enable_sort_spill(&self) -> bool {
        unimplemented!()
    }

    fn set_enable_sort_spill(&self, _enable: bool) {
        unimplemented!()
    }

    fn set_compaction_num_block_hint(&self, _table_name: &str, _hint: u64) {
        unimplemented!()
    }

    fn get_compaction_num_block_hint(&self, _table_name: &str) -> u64 {
        unimplemented!()
    }

    fn get_enable_auto_analyze(&self) -> bool {
        unimplemented!()
    }

    fn set_enable_auto_analyze(&self, _enable: bool) {
        unimplemented!()
    }

    fn get_pruned_partitions_stats(&self) -> HashMap<u32, PartStatistics> {
        unimplemented!()
    }

    fn set_pruned_partitions_stats(&self, _plan_id: u32, _stats: PartStatistics) {
        unimplemented!()
    }

    fn merge_pruned_partitions_stats(&self, _other: &HashMap<u32, PartStatistics>) {
        unimplemented!()
    }
}
