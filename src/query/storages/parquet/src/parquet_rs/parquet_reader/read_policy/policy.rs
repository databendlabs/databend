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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TopKSorter;
use parquet::arrow::arrow_reader::RowSelection;

use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;

/// The policy for reading a row group.
///
/// When conducting parquet reading for row-group-level partition,
/// we can split data fetching into different parts to improve the performance.
/// For example, we can fetch predicate columns first and evaluate the predicate to get a filter bitmap,
/// and we can use this bitmap to reduce IO and deserialization for other columns.
///
/// There are several cases:
///
/// 1. predicate is [None] and topk is [None]: (1) read output columns;
/// 2. predicate is [None] and topk is [Some]: (1) read topk columns;               (2) read other columns (output - topk);
/// 3. predicate is [Some] and topk is [None]: (1) read predicate columns;          (2) read other columns (output - predicate);
/// 4. predicate is [Some] and topk is [Some]: (1) read predicate and topk columns; (2) read other columns (output - predicate ∪ topk);
pub trait ReadPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>>;
}

pub type ReadPolicyImpl = Box<dyn ReadPolicy + Send>;

pub type PolicyType = u8;
pub const POLICY_NUM: usize = 4;
pub const POLICY_NO_PREFETCH: PolicyType = 0; // 0b00
pub const POLICY_PREDICATE_ONLY: PolicyType = 1; // 0b01
pub const POLICY_TOPK_ONLY: PolicyType = 2; // 0b10
pub const POLICY_PREDICATE_AND_TOPK: PolicyType = 3; // 0b11

pub type PolicyBuilders = [Box<dyn ReadPolicyBuilder>; POLICY_NUM];

pub fn default_policy_builders() -> PolicyBuilders {
    [
        Box::new(DummyBuilder {}),
        Box::new(DummyBuilder {}),
        Box::new(DummyBuilder {}),
        Box::new(DummyBuilder {}),
    ]
}

#[async_trait::async_trait]
pub trait ReadPolicyBuilder: Send + Sync {
    async fn build(
        &self,
        _row_group: InMemoryRowGroup<'_>,
        _row_selection: Option<RowSelection>,
        _sorter: &mut Option<TopKSorter>,
        _batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        unreachable!()
    }
}

pub struct DummyBuilder {}

#[async_trait::async_trait]
impl ReadPolicyBuilder for DummyBuilder {}
