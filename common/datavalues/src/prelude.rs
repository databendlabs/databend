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

//! Everything you need to get started with this crate.
use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::array::MutableBinaryArray;

pub use crate::columns::*;
pub use crate::data_group_value::*;
pub use crate::data_value::DFTryFrom;
pub use crate::data_value::*;
pub use crate::macros::*;
pub use crate::scalars::*;
pub use crate::types::*;
pub use crate::utils::*;
// common structs
pub use crate::DataField;
pub use crate::DataSchema;
pub use crate::DataSchemaRef;
pub use crate::DataSchemaRefExt;
pub use crate::DataValue;
//operators
pub use crate::DataValueBinaryOperator;
pub use crate::DataValueComparisonOperator;
pub use crate::DataValueLogicOperator;
pub use crate::DataValueUnaryOperator;

pub type MutableLargeBinaryArray = MutableBinaryArray<i64>;
pub type LargeBinaryArray = BinaryArray<i64>;

pub type Vu8 = Vec<u8>;
