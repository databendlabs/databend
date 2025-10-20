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

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

pub struct FinalAggregateSharedState {
    pub aggregate_queues: Vec<Vec<AggregateMeta>>,
}

impl FinalAggregateSharedState {
    pub fn create(partition_count: usize) -> Self {
        let mut aggregate_queues = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            aggregate_queues.push(Vec::new());
        }

        FinalAggregateSharedState { aggregate_queues }
    }

    pub fn merge_aggregate_queues(&mut self, metas: Vec<Vec<AggregateMeta>>) {
        debug_assert_eq!(self.aggregate_queues.len(), metas.len());
        for (i, meta_queue) in metas.into_iter().enumerate() {
            self.aggregate_queues[i].extend(meta_queue);
        }
    }

    pub fn take_aggregate_queue(&mut self, index: usize) -> Vec<AggregateMeta> {
        std::mem::take(&mut self.aggregate_queues[index])
    }
}
