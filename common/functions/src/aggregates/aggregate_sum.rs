// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::marker::PhantomData;

use bytes::BytesMut;
use common_arrow::arrow::array::Array;
use common_datavalues::prelude::*;
use common_datavalues::DFTryFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::traits::AsPrimitive;

use super::AggregateFunctionRef;
use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::dispatch_numeric_types;

struct AggregateSumState<T> {
    pub value: Option<T>,
}

impl<'a, T> GetState<'a, AggregateSumState<T>> for AggregateSumState<T> {}

impl<T> AggregateSumState<T>
where
    T: std::ops::Add<Output = T> + Copy + Clone,
    Option<T>: BinarySer + BinaryDe,
{
    #[inline(always)]
    fn add(&mut self, other: T) {
        match &self.value {
            Some(a) => self.value = Some(a.add(other)),
            None => self.value = Some(other),
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        self.value.serialize_to_buf(writer)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = Option::<T>::deserialize(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateSumFunction<T, SumT> {
    display_name: String,
    arguments: Vec<DataField>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateSumFunction<T, SumT>
where
    T: DFNumericType,
    SumT: DFNumericType,

    T::Native: AsPrimitive<SumT::Native>
        + DFTryFrom<DataValue>
        + Clone
        + Into<DataValue>
        + Send
        + Sync
        + 'static,
    SumT::Native: DFTryFrom<DataValue>
        + Into<DataValue>
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = SumT::Native>
        + BinarySer
        + BinaryDe
        + Send
        + Sync
        + 'static,
    Option<SumT::Native>: Into<DataValue>,
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        let value: DataValue = Some(SumT::Native::default()).into();

        Ok(value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateSumState::<SumT::Native> { value: None });
        (state as *mut AggregateSumState<SumT::Native>) as StateAddr
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let value = arrays[0].sum()?;
        let opt_sum: Result<SumT::Native> = DFTryFrom::try_from(value);

        if let Ok(s) = opt_sum {
            let state = AggregateSumState::<SumT::Native>::get(place);
            state.add(s);
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let array: &DataArray<T> = arrays[0].static_cast::<T>();
        let array = array.downcast_ref();

        if array
            .validity()
            .as_ref()
            .map(|c| c.get_bit(row))
            .unwrap_or(true)
        {
            let state = AggregateSumState::<SumT::Native>::get(place);
            state.add(array.value(row).as_());
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateSumState::<SumT::Native>::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateSumState::<SumT::Native>::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = AggregateSumState::<SumT::Native>::get(rhs);
        if let Some(s) = &rhs.value {
            let state = AggregateSumState::<SumT::Native>::get(place);
            state.add(*s);
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateSumState::<SumT::Native>::get(place);
        Ok(state.value.into())
    }
}

impl<T, SumT> fmt::Display for AggregateSumFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateSumFunction<T, SumT>
where
    T: DFNumericType,
    SumT: DFNumericType,

    T::Native: AsPrimitive<SumT::Native>
        + DFTryFrom<DataValue>
        + Clone
        + Into<DataValue>
        + Send
        + Sync
        + 'static,
    SumT::Native: DFTryFrom<DataValue>
        + Into<DataValue>
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = SumT::Native>
        + BinarySer
        + BinaryDe
        + Send
        + Sync
        + 'static,
    Option<SumT::Native>: Into<DataValue>,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

macro_rules! creator {
    ($T: ident, $data_type: expr, $display_name: expr, $arguments: expr) => {
        if $T::data_type() == $data_type {
            return AggregateSumFunction::<$T, <$T as DFNumericType>::LargestType>::try_create(
                $display_name,
                $arguments,
            );
        }
    };
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    dispatch_numeric_types! {creator, data_type.clone(), display_name, arguments}

    Err(ErrorCode::BadDataValueType(format!(
        "AggregateSumFunction does not support type '{:?}'",
        data_type
    )))
}
