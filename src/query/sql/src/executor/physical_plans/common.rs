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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Scalar;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use crate::IndexType;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub params: Vec<Scalar>,
    pub args: Vec<DataType>,
}

impl AggregateFunctionSignature {
    pub fn return_type(&self) -> Result<DataType> {
        AggregateFunctionFactory::instance()
            .get(&self.name, self.params.clone(), self.args.clone())?
            .return_type()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub output_column: IndexType,
    /// Bound indices of arguments. Only used in partial aggregation.
    pub arg_indices: Vec<IndexType>,
    pub display: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: IndexType,
    pub display_name: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OnConflictField {
    pub table_field: databend_common_expression::TableField,
    pub field_index: databend_common_expression::FieldIndex,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FragmentKind {
    // Init-partition
    Init,
    // Partitioned by hash
    Normal,
    // Broadcast
    Expansive,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Copy)]
pub enum MutationKind {
    Delete,
    Update,
    Replace,
    Recluster,
    Insert,
    Compact,
    MergeInto,
}

impl Display for MutationKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            MutationKind::Delete => write!(f, "Delete"),
            MutationKind::Insert => write!(f, "Insert"),
            MutationKind::Recluster => write!(f, "Recluster"),
            MutationKind::Update => write!(f, "Update"),
            MutationKind::Replace => write!(f, "Replace"),
            MutationKind::Compact => write!(f, "Compact"),
            MutationKind::MergeInto => write!(f, "MergeInto"),
        }
    }
}
