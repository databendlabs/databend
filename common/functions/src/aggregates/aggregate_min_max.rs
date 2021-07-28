// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::TryFromDataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use super::GetState;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::apply_numeric_creator;
use crate::apply_string_creator;

struct AggregateMinMaxState<T> {
    pub value: Option<T>,
}

impl<'a, T> GetState<'a, AggregateMinMaxState<T>> for AggregateMinMaxState<T> {}

impl<T> AggregateMinMaxState<T>
where
    T: std::cmp::PartialOrd + Clone,
    Option<T>: BinarySer + BinaryDe,
{
    #[inline(always)]
    fn add(&mut self, other: T, is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.partial_cmp(&other);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(other)
                    }
                    _ => {}
                }
            }
            _ => self.value = Some(other),
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
pub struct AggregateMinMaxFunction<T> {
    display_name: String,
    arguments: Vec<DataField>,
    is_min: bool,
    t: PhantomData<T>,
}

impl<T> AggregateFunction for AggregateMinMaxFunction<T>
where
    T: std::cmp::PartialOrd + TryFromDataValue<DataValue> + Send + Sync + Clone + 'static,
    Option<T>: BinarySer + BinaryDe + Into<DataValue>,
{
    fn name(&self) -> &str {
        "AggregateMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateMinMaxState::<T> { value: None });
        (state as *mut AggregateMinMaxState<T>) as StateAddr
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[DataColumn],
        _input_rows: usize,
    ) -> Result<()> {
        let value = if self.is_min {
            min_batch(&columns[0])
        } else {
            max_batch(&columns[0])
        }?;

        let value: Result<T> = TryFromDataValue::try_from(value);
        if let Ok(v) = value {
            let state = AggregateMinMaxState::<T>::get(place);
            state.add(v, self.is_min);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, columns: &[DataColumn]) -> Result<()> {
        let value = columns[0].try_get(row)?;
        let value: Result<T> = TryFromDataValue::try_from(value);
        if let Ok(v) = value {
            let state = AggregateMinMaxState::<T>::get(place);
            state.add(v, self.is_min);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateMinMaxState::<T>::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateMinMaxState::<T>::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = AggregateMinMaxState::<T>::get(rhs);
        if let Some(v) = &rhs.value {
            let state = AggregateMinMaxState::<T>::get(place);
            state.add(v.clone(), self.is_min);
        }

        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateMinMaxState::<T>::get(place);
        let value = state.value.clone();
        Ok(value.into())
    }
}

pub fn min_batch(column: &DataColumn) -> Result<DataValue> {
    if column.is_empty() {
        return Ok(DataValue::from(&column.data_type()));
    }
    match column {
        DataColumn::Constant(value, _) => Ok(value.clone()),
        DataColumn::Array(array) => array.min(),
    }
}

pub fn max_batch(column: &DataColumn) -> Result<DataValue> {
    if column.is_empty() {
        return Ok(DataValue::from(&column.data_type()));
    }
    match column {
        DataColumn::Constant(value, _) => Ok(value.clone()),
        DataColumn::Array(array) => array.max(),
    }
}

impl<T> fmt::Display for AggregateMinMaxFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateMinMaxFunction<T>
where
    T: std::cmp::PartialOrd + TryFromDataValue<DataValue> + Send + Sync + Clone + 'static,
    Option<T>: BinarySer + BinaryDe + Into<DataValue>,
{
    pub fn try_create_min(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateMinMaxFunction::<T> {
            display_name: display_name.to_owned(),
            arguments,
            t: PhantomData,
            is_min: true,
        }))
    }

    pub fn try_create_max(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateMinMaxFunction::<T> {
            display_name: display_name.to_owned(),
            arguments,
            t: PhantomData,
            is_min: false,
        }))
    }
}

pub fn try_create_aggregate_min_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    let ty = data_type.clone();
    let args = arguments.clone();
    let c =
        apply_numeric_creator! { ty, AggregateMinMaxFunction, try_create_min, display_name, args};

    if c.is_ok() {
        return c;
    }

    apply_string_creator! {data_type, AggregateMinMaxFunction, try_create_min, display_name, arguments}
}

pub fn try_create_aggregate_max_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    let ty = data_type.clone();
    let args = arguments.clone();
    let c =
        apply_numeric_creator! { ty, AggregateMinMaxFunction, try_create_max, display_name, args};

    if c.is_ok() {
        return c;
    }

    apply_string_creator! {data_type, AggregateMinMaxFunction, try_create_max, display_name, arguments}
}
