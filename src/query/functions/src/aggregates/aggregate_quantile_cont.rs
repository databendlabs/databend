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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::ScalarRef;
use num_traits::AsPrimitive;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_params;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;
use crate::BUILTIN_FUNCTIONS;

const MEDIAN: u8 = 0;
const QUANTILE_CONT: u8 = 1;

#[derive(Default, Serialize, Deserialize)]
struct QuantileContState {
    pub value: Vec<OrderedFloat<f64>>,
}

impl QuantileContState {
    fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    fn add(&mut self, other: f64) {
        self.value.push(other.into());
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(rhs.value.iter());
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder, levels: Vec<f64>) -> Result<()> {
        let value_len = self.value.len();
        if levels.len() > 1 {
            let builder = match builder {
                ColumnBuilder::Array(box b) => b,
                _ => unreachable!(),
            };
            let indices = levels
                .iter()
                .map(|level| libm::modf((value_len - 1) as f64 * (*level)))
                .collect::<Vec<(f64, f64)>>();
            for (frac, whole) in indices {
                let whole = whole as usize;
                if whole >= value_len {
                    builder.push_default();
                } else {
                    let n = self.compute_result(whole, frac, value_len);
                    builder.put_item(ScalarRef::Number(NumberScalar::Float64(n.into())));
                }
            }
            builder.commit_row();
        } else {
            let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();
            let (frac, whole) = libm::modf((value_len - 1) as f64 * levels[0]);
            let whole = whole as usize;
            if whole >= value_len {
                builder.push(0_f64.into());
            } else {
                let n = self.compute_result(whole, frac, value_len);
                builder.push(n.into());
            }
        }
        Ok(())
    }

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

#[derive(Clone)]
pub struct AggregateQuantileContFunction<T> {
    display_name: String,
    return_type: DataType,
    levels: Vec<f64>,
    _arguments: Vec<DataType>,
    _t: PhantomData<T>,
}

impl<T> Display for AggregateQuantileContFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateFunction for AggregateQuantileContFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn name(&self) -> &str {
        "AggregateQuantileContFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(QuantileContState::new)
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<QuantileContState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<QuantileContState>();
        match validity {
            Some(bitmap) => {
                for (value, is_valid) in column.iter().zip(bitmap.iter()) {
                    if is_valid {
                        state.add(value.as_());
                    }
                }
            }
            None => {
                for value in column.iter() {
                    state.add(value.as_());
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();

        let state = place.get::<QuantileContState>();
        let v: f64 = column[row].as_();
        state.add(v);
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();

        column.iter().zip(places.iter()).for_each(|(value, place)| {
            let place = place.next(offset);
            let state = place.get::<QuantileContState>();
            let v: f64 = value.as_();
            state.add(v);
        });
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<QuantileContState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<QuantileContState>();
        let rhs: QuantileContState = deserialize_state(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<QuantileContState>();
        let other = rhs.get::<QuantileContState>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<QuantileContState>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<QuantileContState>();
        std::ptr::drop_in_place(state);
    }
}

impl<T> AggregateQuantileContFunction<T>
where T: Number + AsPrimitive<f64>
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

        let func = AggregateQuantileContFunction::<T> {
            display_name: display_name.to_string(),
            return_type,
            levels,
            _arguments: arguments,
            _t: PhantomData,
        };

        Ok(Arc::new(func))
    }
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

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)))
            } else {
                DataType::Number(NumberDataType::Float64)
            };
            AggregateQuantileContFunction::<NUM_TYPE>::try_create(
                display_name,
                return_type,
                params,
                arguments,
            )
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
