// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use common_arrow::arrow::array::Array;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use super::GetState;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::dispatch_numeric_types;

pub trait AggregateMinMaxState: Send + Sync + 'static {
    fn default() -> Self;
    fn get_state<'a>(place: StateAddr) -> &'a mut Self;
    fn add(&mut self, series: &Series, row: usize, is_min: bool) -> Result<()>;
    fn add_batch(&mut self, series: &Series, is_min: bool) -> Result<()>;
    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn merge_result(&mut self) -> Result<DataValue>;
}

struct NumericState<T: DFNumericType> {
    pub value: Option<T::Native>,
}

struct Utf8State {
    pub value: Option<String>,
}

impl<'a, T> GetState<'a, NumericState<T>> for NumericState<T> where T: DFNumericType {}
impl<'a> GetState<'a, Utf8State> for Utf8State {}

impl<T> NumericState<T>
where
    T: DFNumericType,
    T::Native: std::cmp::PartialOrd + Clone + BinarySer + BinaryDe,
    Option<T::Native>: Into<DataValue>,
{
    #[inline]
    fn merge_value(&mut self, other: T::Native, is_min: bool) {
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
}

impl<T> AggregateMinMaxState for NumericState<T>
where
    T: DFNumericType,
    T::Native: std::cmp::PartialOrd + Clone + BinarySer + BinaryDe + DFTryFrom<DataValue>,
    Option<T::Native>: Into<DataValue>,
{
    fn default() -> Self {
        Self { value: None }
    }

    fn get_state<'a>(place: StateAddr) -> &'a mut Self {
        Self::get(place)
    }

    fn add(&mut self, series: &Series, row: usize, is_min: bool) -> Result<()> {
        let array: &DataArray<T> = series.static_cast();
        let array = array.downcast_ref();

        if array
            .validity()
            .as_ref()
            .map(|c| c.get_bit(row))
            .unwrap_or(true)
        {
            let other = array.value(row);
            self.merge_value(other, is_min);
        }
        Ok(())
    }

    fn add_batch(&mut self, series: &Series, is_min: bool) -> Result<()> {
        let c = if is_min { series.min() } else { series.max() }?;
        let other: Result<T::Native> = DFTryFrom::try_from(c);
        if let Ok(other) = other {
            self.merge_value(other, is_min);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()> {
        if let Some(other) = rhs.value {
            self.merge_value(other, is_min);
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        self.value.serialize_to_buf(writer)
    }
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = Option::<T::Native>::deserialize(reader)?;
        Ok(())
    }

    fn merge_result(&mut self) -> Result<DataValue> {
        Ok(self.value.into())
    }
}

impl Utf8State {
    fn merge_value(&mut self, other: &str, is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.as_str().partial_cmp(other);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(other.to_string())
                    }
                    _ => {}
                }
            }
            _ => self.value = Some(other.to_string()),
        }
    }
}
impl AggregateMinMaxState for Utf8State {
    fn default() -> Self {
        Self { value: None }
    }

    fn get_state<'a>(place: StateAddr) -> &'a mut Self {
        Self::get(place)
    }

    fn add(&mut self, series: &Series, row: usize, is_min: bool) -> Result<()> {
        let array: &DataArray<Utf8Type> = series.static_cast();
        let array = array.downcast_ref();

        if array
            .validity()
            .as_ref()
            .map(|c| c.get_bit(row))
            .unwrap_or(true)
        {
            let other = array.value(row);
            self.merge_value(other, is_min);
        }
        Ok(())
    }

    fn add_batch(&mut self, series: &Series, is_min: bool) -> Result<()> {
        let c = if is_min { series.min() } else { series.max() }?;
        let other: Result<String> = DFTryFrom::try_from(c);
        if let Ok(other) = other {
            self.merge_value(other.as_str(), is_min);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()> {
        if let Some(other) = &rhs.value {
            self.merge_value(other.as_str(), is_min);
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        self.value.serialize_to_buf(writer)
    }
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = Option::<String>::deserialize(reader)?;
        Ok(())
    }

    fn merge_result(&mut self) -> Result<DataValue> {
        let v = self.value.clone();
        Ok(v.into())
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
where T: AggregateMinMaxState //  std::cmp::PartialOrd + DFTryFrom<DataValue> + Send + Sync + Clone + 'static,
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
        let state = arena.alloc(T::default());
        (state as *mut T) as StateAddr
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state = T::get_state(place);
        state.add_batch(&arrays[0], self.is_min)
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let state = T::get_state(place);
        state.add(&arrays[0], row, self.is_min)
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = T::get_state(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = T::get_state(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = T::get_state(rhs);
        let state = T::get_state(place);
        state.merge(rhs, self.is_min)
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = T::get_state(place);
        state.merge_result()
    }
}

impl<T> fmt::Display for AggregateMinMaxFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateMinMaxFunction<T>
where T: AggregateMinMaxState
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

macro_rules! creator {
    ($T: ident, $data_type: expr, $is_min: expr, $display_name: expr, $arguments: expr) => {
        if $T::data_type() == $data_type {
            type AggState = NumericState<$T>;
            if $is_min {
                return AggregateMinMaxFunction::<AggState>::try_create_min(
                    $display_name,
                    $arguments,
                );
            } else {
                return AggregateMinMaxFunction::<AggState>::try_create_max(
                    $display_name,
                    $arguments,
                );
            }
        }
    };
}

pub fn try_create_aggregate_minmax_function(
    is_min: bool,
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].data_type();

    dispatch_numeric_types! {creator, data_type.clone(), is_min, display_name, arguments}
    if data_type == &DataType::Utf8 {
        if is_min {
            return AggregateMinMaxFunction::<Utf8State>::try_create_min(display_name, arguments);
        } else {
            return AggregateMinMaxFunction::<Utf8State>::try_create_max(display_name, arguments);
        }
    }

    Err(ErrorCode::BadDataValueType(format!(
        "AggregateMinMaxFunction does not support type '{:?}'",
        data_type
    )))
}
