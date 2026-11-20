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

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockProfileStatistics;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use parquet::file::metadata::RowGroupMetaData;

pub struct SpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub row_group: RowGroupMetaData,
}

/// The partial aggregate states of several buckets, passed between the processors of a node.
/// States from other nodes arrive serialized, see `AggregateStatesMeta`.
pub struct AggregateMeta {
    pub payloads: Vec<AggregatePayload>,
}

impl AggregateMeta {
    /// A block carrying the partial aggregate states of several buckets.
    pub fn payloads_block(payloads: Vec<AggregatePayload>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(AggregateMeta { payloads }))
    }
}

impl Debug for AggregateMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("AggregateMeta")
            .field("payloads", &self.payloads.len())
            .finish()
    }
}

local_block_meta_serde!(AggregateMeta);

#[typetag::serde(name = "aggregate_payloads")]
impl BlockMetaInfo for AggregateMeta {
    fn output_stats(&self) -> Option<BlockProfileStatistics> {
        Some(BlockProfileStatistics {
            rows: self.payloads.iter().map(|p| p.payload.len()).sum(),
            bytes: self.payloads.iter().map(|p| p.payload.memory_size()).sum(),
        })
    }
}
