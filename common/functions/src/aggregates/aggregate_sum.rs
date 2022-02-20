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

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function::AggregateFunctionRef;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

struct AggregateSumState<T> {
    pub value: T,
}

impl<T> AggregateSumState<T>
where T: std::ops::AddAssign + Serialize + DeserializeOwned + Copy + Clone
{
    #[inline(always)]
    fn add(&mut self, other: T) {
        self.value += other;
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateSumFunction<T, SumT> {
    display_name: String,
    _arguments: Vec<DataField>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateSumFunction<T, SumT>
where
    T: PrimitiveType + AsPrimitive<SumT>,
    SumT: PrimitiveType + ToDataType + std::ops::AddAssign,
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(SumT::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateSumState::<SumT> {
            value: SumT::default(),
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateSumState<SumT>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let value = sum_primitive::<T, SumT>(&columns[0], validity)?;
        let state = place.get::<AggregateSumState<SumT>>();
        state.add(value);
        Ok(())
    }

    // null bits can be ignored above the level of the aggregate function
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let darray: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        darray.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateSumState<SumT>>();
            state.add(c.as_());
        });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let column: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };

        let state = place.get::<AggregateSumState<SumT>>();
        let v: SumT = unsafe { column.value_unchecked(row).as_() };
        state.add(v);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<AggregateSumState<SumT>>();
        let state = place.get::<AggregateSumState<SumT>>();
        state.add(rhs.value);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        let builder: &mut MutablePrimitiveColumn<SumT> = Series::check_get_mutable_column(array)?;
        builder.append_value(state.value);
        Ok(())
    }
}

impl<T, SumT> fmt::Display for AggregateSumFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateSumFunction<T, SumT>
where
    T: PrimitiveType + AsPrimitive<SumT>,
    SumT: PrimitiveType + ToDataType + std::ops::AddAssign,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            _arguments: arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    with_match_primitive_type_id!(data_type.data_type_id(), |$T| {
        AggregateSumFunction::<$T, <$T as PrimitiveType>::LargestType>::try_create(
             display_name,
             arguments,
        )
    },

    // no matching branch
    {
        Err(ErrorCode::BadDataValueType(format!(
            "AggregateSumFunction does not support type '{:?}'",
            data_type
        )))
    })
}

pub fn aggregate_sum_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_sum_function))
}

pub fn sum_primitive<T, SumT>(column: &ColumnRef, validity: Option<&Bitmap>) -> Result<SumT>
where
    T: PrimitiveType + AsPrimitive<SumT>,
    SumT: PrimitiveType + std::ops::AddAssign,
{
    let inner: &PrimitiveColumn<T> = Series::check_get(column)?;

    if let Some(validity) = validity {
        let mut sum = SumT::default();
        // TODO use simd version
        inner.iter().zip(validity.iter()).for_each(|(t, b)| {
            if b {
                sum += t.as_();
            }
        });

        Ok(sum)
    } else {
        let mut sum = SumT::default();
        inner.iter().for_each(|t| {
            sum += t.as_();
        });

        Ok(sum)
    }
}
