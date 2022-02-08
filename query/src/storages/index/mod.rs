// Copyright 2021 Datafuse Labs.
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

// TODO: refactor bloom_filter into datavalues2
// mod bloom_filter;
mod index_min_max;
mod index_sparse;
pub mod range_filter;

// pub use bloom_filter::BloomFilter;
// pub use bloom_filter::BloomFilterExprEvalResult;
// pub use bloom_filter::BloomFilterIndexer;
pub use index_min_max::MinMaxIndex;
pub use index_sparse::SparseIndex;
pub use index_sparse::SparseIndexValue;
pub use range_filter::BlockStatistics;
pub use range_filter::ColumnStatistics;
pub use range_filter::RangeFilter;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IndexSchemaVersion {
    V1,
}
