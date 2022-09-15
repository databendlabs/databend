// Copyright 2021 Datafuse Labs.
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
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::arithmetics_type::ResultTypeOfUnary;
use common_expression::types::number::Float64Type;
use common_expression::types::number::Number;
use common_expression::types::number::F64;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::*;
use num_traits::AsPrimitive;
use num_traits::NumCast;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_sum::sum_primitive;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

// count = 0 means it's all nullable
// so we do not need option like sum
#[derive(Serialize, Deserialize)]
struct AggregateAvgState<T: Number> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: T,
    pub count: u64,
}

impl<T> AggregateAvgState<T>
where T: std::ops::AddAssign + Number
{
    #[inline(always)]
    fn add(&mut self, value: T, count: u64) {
        self.value += value;
        self.count += count;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        self.value += other.value;
        self.count += other.count;
    }
}

#[derive(Clone)]
pub struct AggregateAvgFunction<T, SumT> {
    display_name: String,
    _arguments: Vec<DataType>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateAvgFunction<T, SumT>
where
    T: Number + AsPrimitive<SumT>,
    SumT: Number + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(Float64Type::data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateAvgState::<SumT> {
            value: SumT::default(),
            count: 0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateAvgState<SumT>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        let sum = sum_primitive::<T, SumT>(&columns[0], validity)?;
        let cnt = input_rows - validity.map_or(0, |v| v.unset_bits());
        state.add(sum, cnt as u64);
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let darray = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        darray.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateAvgState<SumT>>();
            state.add(c.as_(), 1);
        });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<AggregateAvgState<SumT>>();
        let v = column[row].as_();
        state.add(v, 1);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        serialize_into_buf(writer, state)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        *state = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        let rhs = rhs.get::<AggregateAvgState<SumT>>();
        state.merge(rhs);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();
        let v: f64 = NumCast::from(state.value).unwrap_or_default();
        let val = v / state.count as f64;
        builder.push(val.into());
        Ok(())
    }
}

impl<T, SumT> fmt::Display for AggregateAvgFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateAvgFunction<T, SumT>
where
    T: Number + AsPrimitive<SumT>,
    SumT: Number + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _arguments: arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateAvgFunction::<NUM_TYPE, <NUM_TYPE as ResultTypeOfUnary>::Sum>::try_create(
                display_name,
                arguments,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateSumFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_avg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_avg_function))
}
