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
use std::fmt::Display;
use std::fmt::Write;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::display::scalar_ref_to_string;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::assert_variadic_arguments;
use super::batch_merge1;
use super::batch_serialize1;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::StateSerde;
use super::UnaryState;

#[derive(Default)]
struct StringAggState {
    values: String,
}

struct StringAggFunctionData {
    delimiter: String,
}

impl FunctionData for StringAggFunctionData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StringAggState {
    fn delimiter(function_data: Option<&dyn FunctionData>) -> &str {
        &function_data
            .and_then(|data| data.as_any().downcast_ref::<StringAggFunctionData>())
            .expect("string_agg function data is missing")
            .delimiter
    }
}

impl<T> UnaryState<T, StringType> for StringAggState
where T: ToStringType
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let delimiter = Self::delimiter(function_data);
        write!(self.values, "{}{delimiter}", T::format(&other)).unwrap();
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let delimiter = Self::delimiter(function_data);
        match validity {
            Some(validity) => {
                for (value, valid) in T::iter_column(&other).zip(validity.iter()) {
                    if valid {
                        write!(self.values, "{}{delimiter}", T::format(&value)).unwrap();
                    }
                }
            }
            None => {
                for value in T::iter_column(&other) {
                    write!(self.values, "{}{delimiter}", T::format(&value)).unwrap();
                }
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.push_str(&rhs.values);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, StringType>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let delimiter = Self::delimiter(function_data);
        if self.values.is_empty() {
            builder.put_and_commit("");
        } else {
            let len = self.values.len() - delimiter.len();
            builder.put_and_commit(&self.values[..len]);
        }
        Ok(())
    }
}

trait ToStringType: AccessType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display;
}

impl ToStringType for BooleanType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl ToStringType for StringType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl<T: Number + Display> ToStringType for NumberType<T> {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl ToStringType for AnyType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        scalar_ref_to_string(v)
    }
}

impl StateSerde for StringAggState {
    fn serialize_type(_function_data: Option<&dyn FunctionData>) -> Vec<StateSerdeItem> {
        vec![DataType::String.into()]
    }

    fn batch_serialize(
        places: &[super::StateAddr],
        loc: &[super::AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<StringType, Self, _>(places, loc, builders, |state, builder| {
            builder.put_and_commit(&state.values);
            Ok(())
        })
    }

    fn batch_merge(
        places: &[super::StateAddr],
        loc: &[super::AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<StringType, Self, _>(places, loc, state, filter, |state, values| {
            state.values.push_str(values);
            Ok(())
        })
    }
}

pub fn try_create_aggregate_string_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_variadic_arguments(display_name, argument_types.len(), (1, 2))?;
    let value_type = argument_types[0].remove_nullable();
    let delimiter = if params.len() == 1 {
        params[0].as_string().unwrap().clone()
    } else {
        String::new()
    };

    match_template::match_template! {
        T = [
            Boolean => BooleanType,
            String => StringType,
        ],
        match value_type {
            DataType::T => {
                AggregateUnaryFunction::<StringAggState, T, StringType>::create(
                    display_name,
                    DataType::String,
                )
                .with_need_drop(true)
                .with_function_data(Box::new(StringAggFunctionData { delimiter }))
                .finish()
            },
            DataType::Number(num_type) => {
                with_number_mapped_type!(|NUM| match num_type {
                    NumberDataType::NUM => {
                        AggregateUnaryFunction::<StringAggState, NumberType<NUM>, StringType>::create(
                            display_name,
                            DataType::String,
                        )
                        .with_need_drop(true)
                        .with_function_data(Box::new(StringAggFunctionData { delimiter }))
                        .finish()
                    }
                })
            },
            DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Date
            | DataType::Variant
            | DataType::Interval => {
                AggregateUnaryFunction::<StringAggState, AnyType, StringType>::create(
                    display_name,
                    DataType::String,
                )
                .with_need_drop(true)
                .with_function_data(Box::new(StringAggFunctionData { delimiter }))
                .finish()
            },
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, value_type
            ))),
        }
    }
}

pub fn aggregate_string_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_string_agg_function))
}
