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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateAddr;
use num_traits::AsPrimitive;

use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_serialize1;
use super::get_levels;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;

const MEDIAN: u8 = 0;
const QUANTILE_CONT: u8 = 1;

pub(crate) struct QuantileData {
    pub(crate) levels: Vec<f64>,
}

#[derive(Default)]
struct QuantileContState {
    pub value: Vec<F64>,
}

impl QuantileContState {
    fn compute_result(&mut self, whole: usize, frac: f64, value_len: usize) -> f64 {
        self.value.as_mut_slice().select_nth_unstable(whole);
        let value = self.value.get(whole).unwrap().0;
        let value1 = if whole + 1 >= value_len {
            value
        } else {
            self.value.as_mut_slice().select_nth_unstable(whole + 1);
            self.value.get(whole + 1).unwrap().0
        };

        value + (value1 - value) * frac
    }
}

impl<T, R> UnaryState<T, R> for QuantileContState
where
    T: ValueType,
    T::Scalar: Number + AsPrimitive<f64>,
    R: ValueType,
{
    type FunctionInfo = QuantileData;

    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        let other = T::to_owned_scalar(other).as_();
        self.value.push(other.into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(rhs.value.iter());
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: R::ColumnBuilderMut<'_>,
        quantile_cont_data: &QuantileData,
    ) -> Result<()> {
        let value_len = self.value.len();
        if quantile_cont_data.levels.len() > 1 {
            let indices = quantile_cont_data
                .levels
                .iter()
                .map(|level| libm::modf((value_len - 1) as f64 * (*level)))
                .collect::<Vec<(f64, f64)>>();
            // as we already know the return type `R` here is `ArrayType<Float64Type>`
            // we should has a inner builder to build `Float64Type::Column` and
            // provide to `R::ColumnBuilder` when `push_item`
            let mut inner_column_builder = NumberColumnBuilder::Float64(Vec::new());
            for (frac, whole) in indices {
                let whole = whole as usize;
                if whole >= value_len {
                    builder.push_default();
                } else {
                    let n = self.compute_result(whole, frac, value_len);
                    inner_column_builder.push(NumberScalar::Float64(n.into()));
                }
            }
            let float64_column = inner_column_builder.build();
            builder.push_item(
                R::try_downcast_scalar(&ScalarRef::Array(Column::Number(float64_column))).unwrap(),
            );
        } else {
            let (frac, whole) = libm::modf((value_len - 1) as f64 * quantile_cont_data.levels[0]);
            let whole = whole as usize;
            if whole >= value_len {
                builder.push_default();
            } else {
                let n = self.compute_result(whole, frac, value_len);
                builder.push_item(
                    R::try_downcast_scalar(&ScalarRef::Number(NumberScalar::Float64(n.into())))
                        .unwrap(),
                );
            }
        }
        Ok(())
    }
}

impl StateSerde for QuantileContState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(Float64Type::data_type())).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<Float64Type>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in &state.value {
                    builder.put_item(*v);
                }
                builder.commit_row();
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
        batch_merge1::<ArrayType<Float64Type>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, values| {
                state.value.extend(values.iter());
                Ok(())
            },
        )
    }
}

pub struct DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub value: Vec<T::Scalar>,
}

impl<T> Default for DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    fn compute_result(&mut self, whole: usize, frac: f64, value_len: usize) -> Result<T::Scalar> {
        self.value.as_mut_slice().select_nth_unstable(whole);
        let value = *self.value.get(whole).unwrap();
        let value1 = if whole + 1 >= value_len {
            value
        } else {
            self.value.as_mut_slice().select_nth_unstable(whole + 1);
            *self.value.get(whole + 1).unwrap()
        };

        let result = value1
            .checked_sub(value)
            .and_then(|sub_result| sub_result.checked_mul(Decimal::from_float(frac)))
            .and_then(|mul_result| value.checked_add(mul_result));

        match result {
            Some(r) => Ok(r),
            None => Err(ErrorCode::Overflow("Decimal overflow when interpolate")),
        }
    }
}

impl<T> UnaryState<T, ArrayType<T>> for DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    type FunctionInfo = QuantileData;

    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, T>,
        quantile_cont_data: &QuantileData,
    ) -> Result<()> {
        let value_len = self.value.len();

        if quantile_cont_data.levels.len() > 1 {
            let indices = quantile_cont_data
                .levels
                .iter()
                .map(|level| libm::modf((value_len - 1) as f64 * (*level)))
                .collect::<Vec<(f64, f64)>>();

            for (frac, whole) in indices {
                let whole = whole as usize;
                if whole >= value_len {
                    builder.push_default();
                } else {
                    let n = self.compute_result(whole, frac, value_len)?;
                    builder.put_item(T::to_scalar_ref(&n));
                }
            }
            builder.commit_row();
        }

        Ok(())
    }
}

impl<T> UnaryState<T, T> for DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    type FunctionInfo = QuantileData;

    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        quantile_cont_data: &QuantileData,
    ) -> Result<()> {
        let value_len = self.value.len();

        let (frac, whole) = libm::modf((value_len - 1) as f64 * quantile_cont_data.levels[0]);
        let whole = whole as usize;
        if whole >= value_len {
            builder.push_default();
        } else {
            let n = self.compute_result(whole, frac, value_len)?;
            builder.push_item(T::to_scalar_ref(&n));
        }

        Ok(())
    }
}

impl<T> StateSerde for DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(DataType::Decimal(
            T::Scalar::default_decimal_size(),
        )))
        .into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<DecimalType<T::Scalar>>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in &state.value {
                    builder.put_item(*v);
                }
                builder.commit_row();
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
        batch_merge1::<ArrayType<DecimalType<T::Scalar>>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, values| {
                state.value.extend(values.iter());
                Ok(())
            },
        )
    }
}

pub fn try_create_aggregate_quantile_cont_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    if TYPE == MEDIAN {
        assert_params(display_name, params.len(), 0)?;
    }

    assert_unary_arguments(display_name, arguments.len())?;

    let levels = get_levels(&params)?;

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            if params.len() > 1 {
                let return_type =
                    DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)));
                AggregateUnaryFunction::<
                    QuantileContState,
                    NumberType<NUM_TYPE>,
                    ArrayType<Float64Type>,
                >::with_function_info(display_name, return_type, QuantileData {
                    levels,
                })
                .with_need_drop(true)
                .finish()
            } else {
                let return_type = DataType::Number(NumberDataType::Float64);
                AggregateUnaryFunction::<
                    QuantileContState,
                    NumberType<NUM_TYPE>,
                    Float64Type,
                >::with_function_info(
                    display_name, return_type, QuantileData { levels }
                )
                .with_need_drop(true)
                .finish()
            }
        }

        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    let data_type = DataType::Decimal(*size);
                    if params.len() > 1 {
                        AggregateUnaryFunction::<
                            DecimalQuantileContState<DecimalType<DECIMAL>>,
                            DecimalType<DECIMAL>,
                            ArrayType<DecimalType<DECIMAL>>,
                        >::with_function_info(
                            display_name,
                            DataType::Array(Box::new(data_type)),
                            QuantileData { levels },
                        )
                        .with_need_drop(true)
                        .finish()
                    } else {
                        AggregateUnaryFunction::<
                            DecimalQuantileContState<DecimalType<DECIMAL>>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::with_function_info(
                            display_name, data_type, QuantileData { levels }
                        )
                        .with_need_drop(true)
                        .finish()
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

pub fn aggregate_quantile_cont_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_cont_function::<QUANTILE_CONT>,
    ))
}

pub fn aggregate_median_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_cont_function::<MEDIAN>,
    ))
}
