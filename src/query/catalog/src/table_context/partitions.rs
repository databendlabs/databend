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

use crate::plan::PartStatistics;

pub trait TableContextPartitionStats: Send + Sync {
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
