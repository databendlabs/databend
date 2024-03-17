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
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalType;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use ethnum::i256;
use num_traits::AsPrimitive;
use ordered_float::OrderedFloat;

use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_params;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunctionRef;
use crate::BUILTIN_FUNCTIONS;

const MEDIAN: u8 = 0;
const QUANTILE_CONT: u8 = 1;

pub(crate) struct QuantileData {
    pub(crate) levels: Vec<f64>,
}

impl FunctionData for QuantileData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
#[derive(Default, BorshSerialize, BorshDeserialize)]
struct QuantileContState {
    pub value: Vec<OrderedFloat<f64>>,
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
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
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
        builder: &mut R::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value_len = self.value.len();
        let quantile_cont_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<QuantileData>()
        };
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
                    R::push_default(builder);
                } else {
                    let n = self.compute_result(whole, frac, value_len);
                    inner_column_builder.push(NumberScalar::Float64(n.into()));
                }
            }
            let float64_column = inner_column_builder.build();
            R::push_item(
                builder,
                R::try_downcast_scalar(&ScalarRef::Array(Column::Number(float64_column))).unwrap(),
            )
        } else {
            let (frac, whole) = libm::modf((value_len - 1) as f64 * quantile_cont_data.levels[0]);
            let whole = whole as usize;
            if whole >= value_len {
                R::push_default(builder);
            } else {
                let n = self.compute_result(whole, frac, value_len);
                R::push_item(
                    builder,
                    R::try_downcast_scalar(&ScalarRef::Number(NumberScalar::Float64(n.into())))
                        .unwrap(),
                );
            }
        }
        Ok(())
    }
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal + BorshSerialize + BorshDeserialize,
{
    pub value: Vec<T::Scalar>,
}

impl<T> Default for DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: BorshDeserialize + BorshSerialize + Decimal,
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> DecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal + BorshSerialize + BorshDeserialize,
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
    T::Scalar: Decimal + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
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
        builder: &mut ArrayColumnBuilder<T>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value_len = self.value.len();
        let quantile_cont_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<QuantileData>()
        };

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
    T::Scalar: Decimal + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
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
        builder: &mut T::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value_len = self.value.len();
        let quantile_cont_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<QuantileData>()
        };

        let (frac, whole) = libm::modf((value_len - 1) as f64 * quantile_cont_data.levels[0]);
        let whole = whole as usize;
        if whole >= value_len {
            T::push_default(builder);
        } else {
            let n = self.compute_result(whole, frac, value_len)?;
            T::push_item(builder, T::to_scalar_ref(&n));
        }

        Ok(())
    }
}

pub(crate) fn get_levels(params: &Vec<Scalar>) -> Result<Vec<f64>> {
    let levels = if params.len() == 1 {
        let level: F64 = check_number(
            None,
            &FunctionContext::default(),
            &Expr::<usize>::Constant {
                span: None,
                scalar: params[0].clone(),
                data_type: params[0].as_ref().infer_data_type(),
            },
            &BUILTIN_FUNCTIONS,
        )?;
        let level = level.0;
        if !(0.0..=1.0).contains(&level) {
            return Err(ErrorCode::BadDataValueType(format!(
                "level range between [0, 1], got: {:?}",
                level
            )));
        }
        vec![level]
    } else if params.is_empty() {
        vec![0.5f64]
    } else {
        let mut levels = Vec::with_capacity(params.len());
        for param in params {
            let level: F64 = check_number(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: param.clone(),
                        data_type: param.as_ref().infer_data_type(),
                    }),
                    dest_type: DataType::Number(NumberDataType::Float64),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let level = level.0;
            if !(0.0..=1.0).contains(&level) {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level range between [0, 1], got: {:?} in levels",
                    level
                )));
            }
            levels.push(level);
        }
        levels
    };
    Ok(levels)
}

pub fn try_create_aggregate_quantile_cont_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
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
                let func = AggregateUnaryFunction::<
                    QuantileContState,
                    NumberType<NUM_TYPE>,
                    ArrayType<Float64Type>,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);

                Ok(Arc::new(func))
            } else {
                let return_type = DataType::Number(NumberDataType::Float64);
                let func = AggregateUnaryFunction::<
                    QuantileContState,
                    NumberType<NUM_TYPE>,
                    Float64Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);

                Ok(Arc::new(func))
            }
        }

        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if params.len() > 1 {
                let func = AggregateUnaryFunction::<
                    DecimalQuantileContState<DecimalType<i128>>,
                    DecimalType<i128>,
                    ArrayType<DecimalType<i128>>,
                >::try_create(
                    display_name,
                    DataType::Array(Box::new(data_type)),
                    params,
                    arguments[0].clone(),
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    DecimalQuantileContState<DecimalType<i128>>,
                    DecimalType<i128>,
                    DecimalType<i128>,
                >::try_create(
                    display_name, data_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if params.len() > 1 {
                let func = AggregateUnaryFunction::<
                    DecimalQuantileContState<DecimalType<i256>>,
                    DecimalType<i256>,
                    ArrayType<DecimalType<i256>>,
                >::try_create(
                    display_name,
                    DataType::Array(Box::new(data_type)),
                    params,
                    arguments[0].clone(),
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    DecimalQuantileContState<DecimalType<i256>>,
                    DecimalType<i256>,
                    DecimalType<i256>,
                >::try_create(
                    display_name, data_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            }
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
