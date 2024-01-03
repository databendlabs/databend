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

mod compact;
mod mutation_meta;
mod mutation_part;
mod mutation_source;
mod recluster_aggregator;
mod recluster_mutator;

pub use compact::BlockCompactMutator;
pub use compact::CompactExtraInfo;
pub use compact::CompactLazyPartInfo;
pub use compact::CompactPartInfo;
pub use compact::CompactSource;
pub use compact::CompactTaskInfo;
pub use compact::SegmentCompactMutator;
pub use compact::SegmentCompactionState;
pub use compact::SegmentCompactor;
pub use mutation_meta::ClusterStatsGenType;
pub use mutation_meta::SerializeDataMeta;
pub use mutation_part::DeletedSegmentInfo;
pub use mutation_part::Mutation;
pub use mutation_part::MutationPartInfo;
pub use mutation_source::MutationAction;
pub use mutation_source::MutationSource;
pub use recluster_aggregator::ReclusterAggregator;
pub use recluster_mutator::ReclusterMutator;

pub static MAX_BLOCK_COUNT: usize = 10_1000;
pub type SegmentIndex = usize;
pub type BlockIndex = usize;
