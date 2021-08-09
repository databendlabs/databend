// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! Everything you need to get started with this crate.
pub use std::sync::Arc;

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
