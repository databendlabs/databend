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

use std::any::Any;
use std::fmt::Debug;

use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataSchema;
use common_expression::RemoteExpr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, PartialEq, Eq)]
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
    pub schema: DataSchema,

    /// The size of the output fields of a table scan plan without the index.
    pub actual_table_field_len: usize,
}

/// This meta just indicate the block is from aggregating index.
#[derive(Debug)]
pub struct AggIndexMeta {}

impl AggIndexMeta {
    pub fn create() -> BlockMetaInfoPtr {
        Box::new(Self {})
    }
}

impl serde::Serialize for AggIndexMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize AggIndexMeta")
    }
}

impl<'de> serde::Deserialize<'de> for AggIndexMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize AggIndexMeta")
    }
}

#[typetag::serde(name = "agg_index_meta")]
impl BlockMetaInfo for AggIndexMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals AggIndexMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone AggIndexMeta")
    }
}
