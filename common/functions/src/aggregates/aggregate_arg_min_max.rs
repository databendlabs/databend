// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DFTryFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use super::AggregateFunctionRef;
use super::StateAddr;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::GetState;
use crate::apply_numeric_creator;
use crate::apply_string_creator;

struct AggregateArgMinMaxState<T> {
    pub value: Option<T>,
    pub data: DataValue,
}

impl<'a, T> GetState<'a, AggregateArgMinMaxState<T>> for AggregateArgMinMaxState<T> {}

impl<T> AggregateArgMinMaxState<T>
where
    T: std::cmp::PartialOrd + Clone,
    Option<T>: BinarySer + BinaryDe,
{
    #[inline(always)]
    fn add(&mut self, value: T, data: DataValue, is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.partial_cmp(&value);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(value);
                        self.data = data;
                    }
                    _ => {}
                }
            }
            None => {
                self.value = Some(value);
                self.data = data;
            }
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        self.value.serialize_to_buf(writer)?;
        self.data.serialize_to_buf(writer)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = Option::<T>::deserialize(reader)?;
        self.data = DataValue::deserialize(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArgMinMaxFunction<T> {
    display_name: String,
    arguments: Vec<DataField>,
    is_min: bool,
    t: PhantomData<T>,
}

impl<T> AggregateFunction for AggregateArgMinMaxFunction<T>
where
    T: std::cmp::PartialOrd + Send + Sync + Clone + 'static + DFTryFrom<DataValue>,
    Option<T>: Into<DataValue> + BinarySer + BinaryDe,
{
    fn name(&self) -> &str {
        "AggregateArgMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateArgMinMaxState::<T> {
            value: None,
            data: self.arguments[0].data_type().into(),
        });
        (state as *mut AggregateArgMinMaxState<T>) as StateAddr
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        if arrays[0].is_empty() {
            return Ok(());
        }

        let arg_result = if self.is_min {
            arrays[1].arg_min()?
        } else {
            arrays[1].arg_max()?
        };

        if let DataValue::Struct(index_value) = arg_result {
            if index_value[0].is_null() {
                return Ok(());
            }
            let value: Result<T> = DFTryFrom::try_from(index_value[1].clone());

            if let Ok(v) = value {
                let data = arrays[0].try_get(index_value[0].as_u64()? as usize)?;
                let state = AggregateArgMinMaxState::<T>::get(place);
                state.add(v, data, self.is_min);
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let value = arrays[1].try_get(row)?;
        let value: Result<T> = DFTryFrom::try_from(value);
        if let Ok(v) = value {
            let data = arrays[0].try_get(row)?;
            let state = AggregateArgMinMaxState::<T>::get(place);
            state.add(v, data, self.is_min);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateArgMinMaxState::<T>::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateArgMinMaxState::<T>::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = AggregateArgMinMaxState::<T>::get(rhs);
        if let Some(v) = &rhs.value {
            let state = AggregateArgMinMaxState::<T>::get(place);
            state.add(v.clone(), rhs.data.clone(), self.is_min);
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateArgMinMaxState::<T>::get(place);
        Ok(state.data.clone())
    }
}

impl<T> fmt::Display for AggregateArgMinMaxFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateArgMinMaxFunction<T>
where
    T: std::cmp::PartialOrd + Send + Sync + Clone + 'static + DFTryFrom<DataValue>,
    Option<T>: Into<DataValue> + BinarySer + BinaryDe,
{
    pub fn try_create_arg_min(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateArgMinMaxFunction::<T> {
            display_name: display_name.to_owned(),
            arguments,
            t: PhantomData,
            is_min: true,
        }))
    }

    pub fn try_create_arg_max(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateArgMinMaxFunction::<T> {
            display_name: display_name.to_owned(),
            arguments,
            t: PhantomData,
            is_min: false,
        }))
    }
}

pub fn try_create_aggregate_arg_min_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, arguments.len())?;

    let data_type = arguments[1].data_type();
    let c = apply_numeric_creator! { data_type.clone(), AggregateArgMinMaxFunction, try_create_arg_min, display_name, arguments.clone()};

    if c.is_ok() {
        return c;
    }
    apply_string_creator! {data_type, AggregateArgMinMaxFunction, try_create_arg_min, display_name, arguments}
}

pub fn try_create_aggregate_arg_max_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, arguments.len())?;

    let data_type = arguments[1].data_type();
    let c = apply_numeric_creator! { data_type.clone(), AggregateArgMinMaxFunction, try_create_arg_max, display_name, arguments.clone()};

    if c.is_ok() {
        return c;
    }
    apply_string_creator! {data_type, AggregateArgMinMaxFunction, try_create_arg_max, display_name, arguments}
}
