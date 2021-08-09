// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! Everything you need to get started with this crate.
pub use std::sync::Arc;

pub use crate::arrays::to_primitive;
// arrays
pub use crate::arrays::ArrayApply;
pub use crate::arrays::ArrayApplyKernel;
pub use crate::arrays::ArrayBuilder;
pub use crate::arrays::ArrayCast;
pub use crate::arrays::ArrayCompare;
pub use crate::arrays::ArrayFillNone;
pub use crate::arrays::ArrayFull;
pub use crate::arrays::ArrayFullNull;
pub use crate::arrays::ArrayScatter;
pub use crate::arrays::ArrayTake;
pub use crate::arrays::BooleanArrayBuilder;
pub use crate::arrays::DFUInt16ArrayBuilder;
pub use crate::arrays::DFUInt32ArrayBuilder;
pub use crate::arrays::DFUInt64ArrayBuilder;
pub use crate::arrays::DFUInt8ArrayBuilder;
pub use crate::arrays::DataArray;
pub use crate::arrays::IntoTakeRandom;
pub use crate::arrays::NewDataArray;
pub use crate::arrays::PrimitiveArrayBuilder;
pub use crate::arrays::TakeRandom;
pub use crate::arrays::Utf8ArrayBuilder;
pub use crate::bit_util::*;
// columns
pub use crate::columns::DataColumn;
pub use crate::columns::DataColumnCommon;
pub use crate::data_array_filter::*;
pub use crate::data_df_type::*;
pub use crate::data_type::*;
pub use crate::data_type_coercion::*;
pub use crate::data_value::DFTryFrom;
// series
pub use crate::series::IntoSeries;
pub use crate::series::Series;
pub use crate::series::SeriesFrom;
pub use crate::series::SeriesTrait;
pub use crate::utils::*;
pub use crate::DFHasher;
pub use crate::DFNumericType;
pub use crate::DFPrimitiveType;
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
