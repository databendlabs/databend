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
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Default, Serialize, Deserialize)]
struct SkewnessState {
    pub n: u64,
    pub sum: f64,
    pub sum_sqr: f64,
    pub sum_cub: f64,
}

impl SkewnessState {
    fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    fn add(&mut self, other: f64) {
        self.n += 1;
        self.sum += other;
        self.sum_sqr += other.powi(2);
        self.sum_cub += other.powi(3);
    }

    fn merge(&mut self, rhs: &Self) {
        if rhs.n == 0 {
            return;
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = match builder {
            ColumnBuilder::Nullable(box b) => b,
            _ => unreachable!(),
        };
        if self.n <= 2 {
            builder.push_null();
            return Ok(());
        }
        let n = self.n as f64;
        let temp = 1.0 / n;
        let div = (temp * (self.sum_sqr - self.sum * self.sum * temp))
            .powi(3)
            .sqrt();
        if div == 0.0 {
            builder.push_null();
            return Ok(());
        }
        let temp1 = (n * (n - 1.0)).sqrt() / (n - 2.0);
        let value = temp1
            * temp
            * (self.sum_cub - 3.0 * self.sum_sqr * self.sum * temp
                + 2.0 * self.sum.powi(3) * temp * temp)
            / div;
        if value.is_infinite() || value.is_nan() {
            builder.push_null();
        } else {
            builder.push(Float64Type::upcast_scalar(value.into()).as_ref());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateSkewnessFunction<T> {
    display_name: String,
    return_type: DataType,
    _arguments: Vec<DataType>,
    _t: PhantomData<T>,
}

impl<T> Display for AggregateSkewnessFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateFunction for AggregateSkewnessFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn name(&self) -> &str {
        "AggregateSkewnessFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(SkewnessState::new)
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<SkewnessState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<SkewnessState>();
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

        let state = place.get::<SkewnessState>();
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
            let state = place.get::<SkewnessState>();
            let v: f64 = value.as_();
            state.add(v);
        });
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<SkewnessState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<SkewnessState>();
        let rhs: SkewnessState = deserialize_state(reader)?;
        state.merge(&rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<SkewnessState>();
        let other = rhs.get::<SkewnessState>();
        state.merge(other);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<SkewnessState>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        false
    }
}

impl<T> AggregateSkewnessFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateSkewnessFunction::<T> {
            display_name: display_name.to_string(),
            return_type,
            _arguments: arguments,
            _t: PhantomData,
        };

        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_skewness_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let return_type =
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64)));
            AggregateSkewnessFunction::<NUM_TYPE>::try_create(
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

pub fn aggregate_skewness_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_skewness_function))
}
