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

use boolean::TrueIdxIter;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::*;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use num_traits::AsPrimitive;

use super::aggregate_unary::UnaryState;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_serialize1;
use super::AggrStateLoc;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;

pub struct NumberSumState<N>
where N: ArgType
{
    pub value: N::Scalar,
}

impl<N> Default for NumberSumState<N>
where
    N: ArgType,
    N::Scalar: Number,
{
    fn default() -> Self {
        NumberSumState::<N> {
            value: N::Scalar::default(),
        }
    }
}

// #[multiversion::multiversion(targets("x86_64+avx", "x86_64+sse"))]
#[inline]
pub fn sum_batch<T, TSum>(inner: Buffer<T>, validity: Option<&Bitmap>) -> TSum
where
    T: Number + AsPrimitive<TSum>,
    TSum: Number + std::ops::AddAssign,
{
    match validity {
        Some(v) => {
            let mut sum = TSum::default();
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                inner.iter().zip(v.iter()).for_each(|(t, b)| {
                    if b {
                        sum += t.as_();
                    }
                });
            } else {
                TrueIdxIter::new(v.len(), Some(v)).for_each(|idx| {
                    sum += unsafe { inner.get_unchecked(idx).as_() };
                });
            }
            sum
        }
        _ => {
            let mut sum = TSum::default();
            inner.iter().for_each(|t| {
                sum += t.as_();
            });

            sum
        }
    }
}

impl<T, N> UnaryState<NumberType<T>, NumberType<N>> for NumberSumState<NumberType<N>>
where
    T: Number + AsPrimitive<N>,
    N: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn add(&mut self, other: T, _: &Self::FunctionInfo) -> Result<()> {
        self.value += other.as_();
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: ColumnView<NumberType<T>>,
        validity: Option<&Bitmap>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        match other {
            ColumnView::Const(v, n) => {
                let sum: N = v.as_();
                let count = validity.map(|v| v.true_count()).unwrap_or(n);
                for _ in 0..count {
                    self.value += sum;
                }
            }
            ColumnView::Column(buffer) => {
                self.value += sum_batch::<T, N>(buffer, validity);
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <NumberType<N> as ValueType>::ColumnBuilderMut<'_>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push(self.value);
        Ok(())
    }
}

impl<N> StateSerde for NumberSumState<N>
where
    N: ArgType,
    N::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        std::vec![N::data_type().into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<N, Self, _>(places, loc, builders, |state, builder| {
            builder.push_item(N::to_scalar_ref(&state.value));
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<N, Self, _>(places, loc, state, filter, |state, data| {
            state.value += N::to_owned_scalar(data);
            Ok(())
        })
    }
}

pub struct DecimalSumState<const SHOULD_CHECK_OVERFLOW: bool, T>
where T: Decimal
{
    pub value: T::U64Array,
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> Default for DecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal
{
    fn default() -> Self {
        Self {
            value: T::U64Array::default(),
        }
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> UnaryState<DecimalType<T>, DecimalType<T>>
    for DecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    fn add(&mut self, other: T, _: &Self::FunctionInfo) -> Result<()> {
        let mut value = T::from_u64_array(self.value);
        value += other;

        if SHOULD_CHECK_OVERFLOW && (value > T::DECIMAL_MAX || value < T::DECIMAL_MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                value,
                T::DECIMAL_MIN,
                T::DECIMAL_MAX,
            )));
        }
        self.value = value.to_u64_array();
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: ColumnView<DecimalType<T>>,
        validity: Option<&Bitmap>,
        function_data: &Self::FunctionInfo,
    ) -> Result<()> {
        if !SHOULD_CHECK_OVERFLOW {
            let mut sum = T::from_u64_array(self.value);
            let buffer = other;
            match validity {
                Some(validity) if validity.null_count() > 0 => {
                    buffer.iter().zip(validity.iter()).for_each(|(t, b)| {
                        if b {
                            sum += t;
                        }
                    });
                }
                _ => {
                    buffer.iter().for_each(|t| {
                        sum += t;
                    });
                }
            }
            self.value = sum.to_u64_array();
        } else {
            match validity {
                Some(validity) => {
                    for (data, valid) in other.iter().zip(validity.iter()) {
                        if valid {
                            self.add(data, function_data)?;
                        }
                    }
                }
                None => {
                    for value in other.iter() {
                        self.add(value, function_data)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let v = T::from_u64_array(rhs.value);
        self.add(v, &())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, DecimalType<T>>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        let v = T::from_u64_array(self.value);
        builder.push(v);
        Ok(())
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> StateSerde for DecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Decimal(T::default_decimal_size()).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<DecimalType<T>, Self, _>(places, loc, builders, |state, builder| {
            builder.push_item(T::from_u64_array(state.value));
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<DecimalType<T>, Self, _>(places, loc, state, filter, |state, value| {
            let rhs = Self {
                value: value.to_u64_array(),
            };
            <Self as UnaryState<_, _>>::merge(state, &rhs)
        })
    }
}

#[derive(Default)]
pub struct IntervalSumState {
    pub value: months_days_micros,
}

impl UnaryState<IntervalType, IntervalType> for IntervalSumState {
    fn add(&mut self, other: months_days_micros, _: &Self::FunctionInfo) -> Result<()> {
        self.value += other;
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let res = self.value.total_micros() + rhs.value.total_micros();
        self.value = months_days_micros(res as i128);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, IntervalType>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(IntervalType::to_scalar_ref(&self.value));
        Ok(())
    }
}

impl StateSerde for IntervalSumState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Interval.into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<IntervalType, Self, _>(places, loc, builders, |state, builder| {
            builder.push(state.value);
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<IntervalType, Self, _>(places, loc, state, filter, |state, value| {
            let rhs = IntervalSumState { value };
            <Self as UnaryState<_, _>>::merge(state, &rhs)
        })
    }
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, arguments.len())?;

    let mut data_type = arguments[0].clone();
    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = Int8Type::data_type();
    }

    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            type TSum = <NUM as ResultTypeOfUnary>::Sum;
            let return_type = NumberType::<TSum>::data_type();
            AggregateUnaryFunction::<
                NumberSumState<NumberType<TSum>>,
                NumberType<NUM>,
                NumberType<TSum>,
            >::create(display_name, return_type)
        }
        DataType::Interval => {
            let return_type = DataType::Interval;
            AggregateUnaryFunction::<IntervalSumState, IntervalType, IntervalType>::create(
                display_name,
                return_type,
            )
        }
        DataType::Decimal(s) => {
            with_decimal_mapped_type!(|DECIMAL| match s.data_kind() {
                DecimalDataKind::DECIMAL => {
                    let decimal_size =
                        DecimalSize::new_unchecked(DECIMAL::MAX_PRECISION, s.scale());

                    let should_check_overflow = DECIMAL::MAX_PRECISION > i64::MAX_PRECISION
                        && s.precision() > i64::MAX_PRECISION;
                    let return_type = DataType::Decimal(decimal_size);
                    if should_check_overflow {
                        AggregateUnaryFunction::<
                            DecimalSumState<true, DECIMAL>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::create(display_name, return_type)
                    } else {
                        AggregateUnaryFunction::<
                            DecimalSumState<false, DECIMAL>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::create(display_name, return_type)
                    }
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_sum_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_sum_function),
        features,
    )
}
