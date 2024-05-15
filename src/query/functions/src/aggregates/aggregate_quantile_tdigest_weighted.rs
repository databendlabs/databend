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

use std::alloc::Layout;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use num_traits::AsPrimitive;

use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_quantile_tdigest::QuantileTDigestState;
use crate::aggregates::aggregate_quantile_tdigest::MEDIAN;
use crate::aggregates::aggregate_quantile_tdigest::QUANTILE;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::assert_params;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;
use crate::BUILTIN_FUNCTIONS;

#[derive(Clone)]
pub struct AggregateQuantileTDigestWeightedFunction<T0, T1> {
    display_name: String,
    return_type: DataType,
    levels: Vec<f64>,
    _arguments: Vec<DataType>,
    _t0: PhantomData<T0>,
    _t1: PhantomData<T1>,
}

impl<T0, T1> Display for AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T0, T1> AggregateFunction for AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn name(&self) -> &str {
        "AggregateQuantileDiscFunction"
    }
    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn init_state(&self, place: StateAddr) {
        place.write(QuantileTDigestState::new)
    }
    fn state_layout(&self) -> Layout {
        Layout::new::<QuantileTDigestState>()
    }
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();
        let state = place.get::<QuantileTDigestState>();
        match validity {
            Some(bitmap) => {
                for ((value, weight), is_valid) in
                    column.iter().zip(weighted.iter()).zip(bitmap.iter())
                {
                    if is_valid {
                        state.add(value.as_(), Some(weight.as_()));
                    }
                }
            }
            None => {
                for (value, weight) in column.iter().zip(weighted.iter()) {
                    state.add(value.as_(), Some(weight.as_()));
                }
            }
        }

        Ok(())
    }
    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();
        let value = unsafe { column.get_unchecked(row) };
        let weight = unsafe { weighted.get_unchecked(row) };

        let state = place.get::<QuantileTDigestState>();
        state.add(value.as_(), Some(weight.as_()));
        Ok(())
    }
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();
        column
            .iter()
            .zip(weighted.iter())
            .zip(places.iter())
            .for_each(|((value, weight), place)| {
                let addr = place.next(offset);
                let state = addr.get::<QuantileTDigestState>();
                state.add(value.as_(), Some(weight.as_()))
            });
        Ok(())
    }
    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let mut rhs: QuantileTDigestState = borsh_deserialize_state(reader)?;
        state.merge(&mut rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let other = rhs.get::<QuantileTDigestState>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<QuantileTDigestState>();
        std::ptr::drop_in_place(state);
    }
}

impl<T0, T1> AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let levels = if params.len() == 1 {
            let level: F64 = check_number(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: params[0].clone(),
                        data_type: params[0].as_ref().infer_data_type(),
                    }),
                    dest_type: DataType::Number(NumberDataType::Float64),
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
        let func = AggregateQuantileTDigestWeightedFunction::<T0, T1> {
            display_name: display_name.to_string(),
            return_type,
            levels,
            _arguments: arguments,
            _t0: PhantomData,
            _t1: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_quantile_tdigest_weighted_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    if TYPE == MEDIAN {
        assert_params(display_name, params.len(), 0)?;
    }

    assert_binary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE_0| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE_0) => {
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)))
            } else {
                DataType::Number(NumberDataType::Float64)
            };

            with_unsigned_integer_mapped_type!(|NUM_TYPE_1| match &arguments[1] {
                DataType::Number(NumberDataType::NUM_TYPE_1) => {
                    AggregateQuantileTDigestWeightedFunction::<NUM_TYPE_0, NUM_TYPE_1>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments,
                    )
                }
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "weight just support unsigned integer type, but got '{:?}'",
                    arguments[1]
                ))),
            })
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} just support numeric type, but got '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_quantile_tdigest_weighted_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_weighted_function::<QUANTILE>,
    ))
}

pub fn aggregate_median_tdigest_weighted_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_weighted_function::<MEDIAN>,
    ))
}
