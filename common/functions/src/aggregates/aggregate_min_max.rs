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
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutableBinaryArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use sha2::digest::generic_array::arr;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::with_match_primitive_type;

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
    fn merge_result(&mut self, array: &mut dyn MutableArray) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
struct NumericState<T: DFPrimitiveType> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: Option<T>,
}

#[derive(Serialize, Deserialize)]
struct StringState {
    pub value: Option<Vec<u8>>,
}

impl<T> NumericState<T>
where
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
{
    #[inline]
    fn merge_value(&mut self, other: T, is_min: bool) {
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
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
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
        let array: &DFPrimitiveArray<T> = series.static_cast();
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
        let other: Result<T> = DFTryFrom::try_from(c);
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
        let writer = BufMut::writer(writer);
        bincode::serialize_into(writer, &self.value)?;
        Ok(())
    }
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = bincode::deserialize_from(reader)?;
        Ok(())
    }

    fn merge_result(&mut self, array: &mut dyn MutableArray) -> Result<()> {
        if let Some(val) = self.value {
            match array.data_type() {
                ArrowDataType::UInt8 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u8>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_u8());
                }
                ArrowDataType::UInt16 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u16>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_u16());
                }
                ArrowDataType::UInt32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u32>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_u32());
                }
                ArrowDataType::UInt64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u64>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_u64());
                }
                ArrowDataType::Int8 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i8>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_i8());
                }
                ArrowDataType::Int16 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i16>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_i16());
                }
                ArrowDataType::Int32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i32>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_i32());
                }
                ArrowDataType::Int64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i64>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_i64());
                }
                ArrowDataType::Float32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f32>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_f32());
                }
                ArrowDataType::Float64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f64>>()
                        .ok_or(ErrorCode::UnexpectedError(format!(
                            "error occured when downcast MutableArray"
                        )))?;
                    array.push(val.to_f64());
                }
                _ => {
                    return Err(ErrorCode::UnexpectedError(format!(
                        "unexpected datatype when aggregate"
                    )))
                }
            }
        } else {
            array.push_null();
        }
        Ok(())
    }
}

impl StringState {
    fn merge_value(&mut self, other: &[u8], is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.as_slice().partial_cmp(other);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(other.to_vec())
                    }
                    _ => {}
                }
            }
            _ => self.value = Some(other.to_vec()),
        }
    }
}
impl AggregateMinMaxState for StringState {
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
        let array: &DFStringArray = series.static_cast();
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
        let other: Result<Vec<u8>> = DFTryFrom::try_from(c);
        if let Ok(other) = other {
            self.merge_value(other.as_slice(), is_min);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()> {
        if let Some(other) = &rhs.value {
            self.merge_value(other.as_slice(), is_min);
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        let writer = BufMut::writer(writer);
        bincode::serialize_into(writer, &self.value)?;
        Ok(())
    }
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = bincode::deserialize_from(reader)?;
        Ok(())
    }

    fn merge_result(&mut self, array: &mut dyn MutableArray) -> Result<()> {
        let v = self.value.clone();
        let mut array = array
            .as_mut_any()
            .downcast_mut::<MutableBinaryArray<i64>>()
            .ok_or(ErrorCode::UnexpectedError(format!(
                "error occured when downcast MutableArray"
            )))?;
        array.push(v);
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

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableArray) -> Result<()> {
        let state = place.get::<T>();
        state.merge_result(array);
        Ok(())
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

pub fn try_create_aggregate_minmax_function<const IS_MIN: bool>(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].data_type();

    with_match_primitive_type!(data_type, |$T| {
        type AggState = NumericState<$T>;
        if IS_MIN {
            AggregateMinMaxFunction::<AggState>::try_create_min(
                display_name,
                arguments,
            )
        } else {
            AggregateMinMaxFunction::<AggState>::try_create_max(
                display_name,
                arguments,
            )
        }
    },

    {
        if data_type == &DataType::String {
            if IS_MIN {
                AggregateMinMaxFunction::<StringState>::try_create_min(display_name, arguments)
            } else {
                AggregateMinMaxFunction::<StringState>::try_create_max(display_name, arguments)
            }
        } else {
            Err(ErrorCode::BadDataValueType(format!(
                "AggregateMinMaxFunction does not support type '{:?}'",
                data_type
            )))
        }
    })
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<true>))
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<false>))
}
