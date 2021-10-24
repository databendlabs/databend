//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

pub use block_meta_accumulator::BlockMetaAccumulator;
pub use block_meta_accumulator::BlockStats;
pub use stats_accumulator::StatisticsAccumulator;
pub use util::block_stats;
pub use util::column_stats_reduce_with_schema;
pub use util::merge_stats;

mod block_meta_accumulator;
mod stats_accumulator;
mod util;

#[cfg(test)]
mod util_test;
