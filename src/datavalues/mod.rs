// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod data_array_aggregate_test;
mod data_array_arithmetic_test;
mod data_array_comparison_test;
mod data_array_logic_test;
mod data_value_aggregate_test;
mod data_value_arithmetic_test;

#[macro_use]
mod macros;

mod data_array;
mod data_array_aggregate;
mod data_array_arithmetic;
mod data_array_comparison;
mod data_array_logic;
mod data_columnar_value;
mod data_field;
mod data_schema;
mod data_type;
mod data_value;
mod data_value_aggregate;
mod data_value_arithmetic;
mod data_value_operator;

pub use self::data_array_aggregate::data_array_aggregate_op;
pub use self::data_array_arithmetic::data_array_arithmetic_op;
pub use self::data_array_comparison::data_array_comparison_op;
pub use self::data_array_logic::data_array_logic_op;
pub use self::data_type::numerical_coercion;
pub use self::data_value_aggregate::data_value_aggregate_op;
pub use self::data_value_arithmetic::data_value_add;

pub use self::data_array::{
    BooleanArray, DataArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, NullArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
pub use self::data_columnar_value::DataColumnarValue;
pub use self::data_field::DataField;
pub use self::data_schema::{DataSchema, DataSchemaRef};
pub use self::data_type::DataType;
pub use self::data_value::{DataValue, DataValueRef};
pub use self::data_value_operator::{
    DataValueAggregateOperator, DataValueArithmeticOperator, DataValueComparisonOperator,
    DataValueLogicOperator,
};
