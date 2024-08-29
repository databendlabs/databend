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

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;

use super::Projection;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AggIndexInfo {
    pub index_id: u64,
    /// The selection on the aggregating index.
    ///
    /// - The first element in the tuple is the expression.
    ///     - The index in the [`RemoteExpr`] is the offset of `schema`.
    /// - The seoncd element in the tuple is the offset of the output schema of the table scan plan.
    ///     - If the offset is [None], it means the selection item will be appended to the end of the output block;
    ///     - else the selection item will be placed at the offset.
    ///
    /// The offsets are used to place each output of the index at the right position of the output block.
    /// The right positions are the column positions after executing `EvalScalar` plan
    /// because index scan will skip the execution of `EvalScalar` and `Filter`.
    pub selection: Vec<(RemoteExpr, Option<usize>)>,
    pub filter: Option<RemoteExpr>,
    pub schema: TableSchemaRef,
    /// Columns in the index block to read.
    pub projection: Projection,

    /// The size of the output fields of a table scan plan without the index.
    pub actual_table_field_len: usize,

    // If the index is the result of an aggregation query.
    pub is_agg: bool,
    pub num_agg_funcs: usize,
}

/// This meta just indicate the block is from aggregating index.
#[derive(Debug, Clone)]
pub struct AggIndexMeta {
    pub is_agg: bool,
    // Number of aggregation functions.
    pub num_agg_funcs: usize,
    // Number of eval expressions (contains aggregation).
    pub num_evals: usize,
}

impl AggIndexMeta {
    pub fn create(is_agg: bool, num_evals: usize, num_agg_funcs: usize) -> BlockMetaInfoPtr {
        Box::new(Self {
            is_agg,
            num_evals,
            num_agg_funcs,
        })
    }
}

impl serde::Serialize for AggIndexMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize AggIndexMeta")
    }
}

impl<'de> serde::Deserialize<'de> for AggIndexMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize AggIndexMeta")
    }
}

#[typetag::serde(name = "agg_index_meta")]
impl BlockMetaInfo for AggIndexMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals AggIndexMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
