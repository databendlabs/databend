// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#![feature(const_generics)]
#![feature(const_evaluatable_checked)]

#[cfg(test)]
mod data_array_arithmetic_test;
#[cfg(test)]
mod data_array_comparison_test;
#[cfg(test)]
mod data_array_logic_test;
#[cfg(test)]
mod data_array_merge_sort_test;
#[cfg(test)]
mod data_value_aggregate_test;
#[cfg(test)]
mod data_value_arithmetic_test;
#[cfg(test)]
mod data_value_kernel_test;

#[cfg(test)]
mod data_array_scatter_test;

#[macro_use]
mod macros;

mod data_array;
mod data_array_trait;
mod data_arrow_array;
mod data_column;
mod data_df_type;
mod data_field;
mod data_schema;
mod data_type;
mod data_type_coercion;
mod data_value;

pub use data_array::*;
pub use data_df_type::*;
pub use data_field::DataField;
pub use data_schema::DataSchema;
pub use data_schema::DataSchemaRef;
pub use data_schema::DataSchemaRefExt;
pub use data_type::DataType;
pub use data_type::*;
pub use data_type_coercion::*;
pub use data_value::DataValue;
pub use data_value::DataValueRef;
