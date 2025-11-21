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

use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use num_traits::AsPrimitive;

use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge2;
use super::batch_serialize2;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::UnaryState;

struct NumberAvgState<T, TSum>
where TSum: ValueType
{
    pub value: TSum::Scalar,
    pub count: u64,
    _t: PhantomData<fn(T)>,
}

impl<T, TSum> Default for NumberAvgState<T, TSum>
where
    T: ValueType,
    TSum: ValueType,
    T::Scalar: Number + AsPrimitive<TSum::Scalar>,
    TSum::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn default() -> Self {
        Self {
            value: TSum::Scalar::default(),
            count: 0,
            _t: PhantomData,
        }
    }
}

impl<T, TSum> UnaryState<T, Float64Type> for NumberAvgState<T, TSum>
where
    T: ValueType,
    TSum: ArgType,
    T::Scalar: Number + AsPrimitive<TSum::Scalar>,
    TSum::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.count += 1;
        let other = T::to_owned_scalar(other).as_();
        self.value += other;
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.count += rhs.count;
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, Float64Type>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = self.value.as_() / (self.count as f64);
        builder.push(F64::from(value));
        Ok(())
    }
}

impl<T, TSum> StateSerde for NumberAvgState<T, TSum>
where
    T: ValueType,
    TSum: ArgType,
    T::Scalar: Number + AsPrimitive<TSum::Scalar>,
    TSum::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        std::vec![
            StateSerdeItem::DataType(TSum::data_type()),
            StateSerdeItem::DataType(UInt64Type::data_type())
        ]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize2::<TSum, UInt64Type, Self, _>(
            places,
            loc,
            builders,
            |state, (sum, count)| {
                sum.push_item(TSum::to_scalar_ref(&state.value));
                count.push_item(state.count);
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge2::<TSum, UInt64Type, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, (sum, count)| {
                state.value += TSum::to_owned_scalar(sum);
                state.count += count;
                Ok(())
            },
        )
    }
}

struct DecimalAvgData {
    // only for decimals
    // AVG：AVG(DECIMAL(a, b)) -> DECIMAL(38 or 76, max(b, 4))。
    pub scale_add: u8,
}

struct DecimalAvgState<const OVERFLOW: bool, T> {
    pub value: T,
    pub count: u64,
}

impl<const OVERFLOW: bool, T> Default for DecimalAvgState<OVERFLOW, T>
where T: Default
{
    fn default() -> Self {
        Self {
            value: T::default(),
            count: 0,
        }
    }
}

impl<const OVERFLOW: bool, T> DecimalAvgState<OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    fn add_internal(&mut self, count: u64, value: T) -> Result<()> {
        self.count += count;
        self.value += value;
        if OVERFLOW && (self.value > T::DECIMAL_MAX || self.value < T::DECIMAL_MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.value,
                T::DECIMAL_MIN,
                T::DECIMAL_MAX,
            )));
        }
        Ok(())
    }
}

impl<const OVERFLOW: bool, T> UnaryState<DecimalType<T>, DecimalType<T>>
    for DecimalAvgState<OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    type FunctionInfo = DecimalAvgData;

    fn add(&mut self, other: T, _: &Self::FunctionInfo) -> Result<()> {
        self.add_internal(1, other)
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.add_internal(rhs.count, rhs.value)
    }

    fn merge_result(
        &mut self,
        mut builder: <DecimalType<T> as ValueType>::ColumnBuilderMut<'_>,
        decimal_avg_data: &DecimalAvgData,
    ) -> Result<()> {
        match self
            .value
            .checked_mul(T::e(decimal_avg_data.scale_add))
            .and_then(|v| v.checked_div(T::from_i128_uncheck(self.count.into())))
        {
            Some(value) => {
                builder.push_item(value);
                Ok(())
            }
            None => Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} mul {}",
                self.value,
                T::e(decimal_avg_data.scale_add)
            ))),
        }
    }
}

impl<const OVERFLOW: bool, T> StateSerde for DecimalAvgState<OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        std::vec![
            DataType::Decimal(T::default_decimal_size()).into(),
            UInt64Type::data_type().into()
        ]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize2::<DecimalType<T>, UInt64Type, Self, _>(
            places,
            loc,
            builders,
            |state, (sum, count)| {
                sum.push_item(state.value);
                count.push_item(state.count);
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge2::<DecimalType<T>, UInt64Type, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, (sum, count)| state.add_internal(count, sum),
        )
    }
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = if arguments[0].is_null() {
        Int8Type::data_type()
    } else {
        arguments[0].clone()
    };

    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            type TSum = <NUM as ResultTypeOfUnary>::Sum;
            let return_type = Float64Type::data_type();
            AggregateUnaryFunction::<
                NumberAvgState<NumberType<NUM>, NumberType<TSum>>,
                NumberType<NUM>,
                Float64Type,
            >::create(display_name, return_type)
        }
        DataType::Decimal(s) => {
            with_decimal_mapped_type!(|DECIMAL| match s.data_kind() {
                DecimalDataKind::DECIMAL => {
                    let decimal_size =
                        DecimalSize::new_unchecked(DECIMAL::MAX_PRECISION, s.scale().max(4));

                    let overflow = DECIMAL::MAX_PRECISION > i64::MAX_PRECISION
                        && s.precision() > i64::MAX_PRECISION;
                    let scale_add = decimal_size.scale() - s.scale();
                    let return_type = DataType::Decimal(decimal_size);

                    if overflow {
                        AggregateUnaryFunction::<
                            DecimalAvgState<true, DECIMAL>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::with_function_info(
                            display_name,
                            return_type,
                            DecimalAvgData { scale_add },
                        )
                        .finish()
                    } else {
                        AggregateUnaryFunction::<
                            DecimalAvgState<false, DECIMAL>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::with_function_info(
                            display_name,
                            return_type,
                            DecimalAvgData { scale_add },
                        )
                        .finish()
                    }
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{display_name} does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_avg_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_avg_function),
        features,
    )
}
