// Copyright 2020 Datafuse Labs.
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
pub use crate::arrays::to_primitive;
// arrays
pub use crate::arrays::*;
pub use crate::bit_util::*;
// columns
pub use crate::columns::DataColumn;
pub use crate::columns::DataColumnCommon;
pub use crate::data_array_filter::*;
pub use crate::data_value::DFTryFrom;
// series
pub use crate::series::IntoSeries;
pub use crate::series::Series;
pub use crate::series::SeriesFrom;
pub use crate::series::SeriesTrait;
pub use crate::types::*;
pub use crate::utils::*;
pub use crate::DFHasher;
// common structs
pub use crate::DataField;
pub use crate::DataGroupValue;
pub use crate::DataSchema;
pub use crate::DataSchemaRef;
pub use crate::DataSchemaRefExt;
pub use crate::DataValue;
pub use crate::DataValueAggregateOperator;
pub use crate::DataValueAggregateOperator::*;
//operators
pub use crate::DataValueArithmeticOperator;
pub use crate::DataValueArithmeticOperator::*;
pub use crate::DataValueComparisonOperator;
pub use crate::DataValueComparisonOperator::*;
pub use crate::DataValueLogicOperator;
pub use crate::DataValueLogicOperator::*;

pub type AlignedVec<T> = common_arrow::arrow::buffer::MutableBuffer<T>;
pub type LargeUtf8Array = common_arrow::arrow::array::Utf8Array<i64>;
pub type LargeBinaryArray = common_arrow::arrow::array::BinaryArray<i64>;
pub type LargeListArray = common_arrow::arrow::array::ListArray<i64>;
