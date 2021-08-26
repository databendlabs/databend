// Copyright 2020 Datafuse Labs.
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
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::dispatch_numeric_types;

pub trait AggregateMinMaxState: Send + Sync + 'static {
    fn default() -> Self;
    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        series: &Series,
        rows: usize,
        is_min: bool,
    ) -> Result<()>;
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

    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        series: &Series,
        _rows: usize,
        is_min: bool,
    ) -> Result<()> {
        let array: &DataArray<T> = series.static_cast();
        let array = array.downcast_ref();
        array.into_iter().zip(places.iter()).for_each(|(x, place)| {
            if let Some(x) = x {
                let place = place.next(offset);
                let state = place.get::<Self>();
                state.merge_value(*x, is_min);
            }
        });
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

    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        series: &Series,
        _rows: usize,
        is_min: bool,
    ) -> Result<()> {
        let array: &DataArray<Utf8Type> = series.static_cast();
        let array = array.downcast_ref();
        array.into_iter().zip(places.iter()).for_each(|(x, place)| {
            let place = place.next(offset);
            if let Some(x) = x {
                let state = place.get::<Self>();
                state.merge_value(x, is_min);
            }
        });
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

    fn init_state(&self, place: StateAddr) {
        place.write(T::default);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<T>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state = place.get::<T>();
        state.add_batch(&arrays[0], self.is_min)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        input_rows: usize,
    ) -> Result<()> {
        T::add_keys(places, offset, &arrays[0], input_rows, self.is_min)
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<T>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<T>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<T>();
        let state = place.get::<T>();
        state.merge(rhs, self.is_min)
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = place.get::<T>();
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
    _params: Vec<DataValue>,
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
