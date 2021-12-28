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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::with_match_primitive_type;

pub trait AggregateArgMinMaxState: Send + Sync + 'static {
    fn new(data_type: &DataType) -> Self;
    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        data_series: &Series,
        series: &Series,
        rows: usize,
        is_min: bool,
    ) -> Result<()>;
    fn add_batch(&mut self, data_series: &Series, series: &Series, is_min: bool) -> Result<()>;
    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn merge_result(&mut self, array: &mut dyn MutableArrayBuilder) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
struct NumericState<T: DFPrimitiveType> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: Option<T>,
    pub data: DataValue,
}

#[derive(Serialize, Deserialize)]
struct StringState {
    pub value: Option<Vec<u8>>,
    pub data: DataValue,
}

impl<T> NumericState<T>
where
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
{
    #[inline]
    fn merge_value(&mut self, data: DataValue, other: T, is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.partial_cmp(&other);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(other);
                        self.data = data;
                    }
                    _ => {}
                }
            }
            None => {
                self.value = Some(other);
                self.data = data;
            }
        }
    }
}

impl<T> AggregateArgMinMaxState for NumericState<T>
where
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
{
    fn new(data_type: &DataType) -> Self {
        Self {
            value: None,
            data: data_type.into(),
        }
    }

    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        data_series: &Series,
        series: &Series,
        _rows: usize,
        is_min: bool,
    ) -> Result<()> {
        let array: &DFPrimitiveArray<T> = series.static_cast();
        array
            .into_iter()
            .zip(places.iter().enumerate())
            .try_for_each(|(key, (idx, addr))| -> Result<()> {
                let data = data_series.try_get(idx)?;
                let place = addr.next(offset);
                let state = place.get::<Self>();
                if let Some(v) = key {
                    state.merge_value(data, *v, is_min);
                }
                Ok(())
            })
    }

    fn add_batch(&mut self, data_series: &Series, series: &Series, is_min: bool) -> Result<()> {
        let arg_result = if is_min {
            series.arg_min()
        } else {
            series.arg_max()
        }?;

        if let DataValue::Struct(index_value) = arg_result {
            if index_value[0].is_null() {
                return Ok(());
            }
            let value: Result<T> = DFTryFrom::try_from(index_value[1].clone());

            if let Ok(other) = value {
                let data = data_series.try_get(index_value[0].as_u64()? as usize)?;
                self.merge_value(data, other, is_min);
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()> {
        if let Some(other) = rhs.value {
            self.merge_value(rhs.data.clone(), other, is_min);
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        let writer = BufMut::writer(writer);
        bincode::serialize_into(writer, self)?;
        Ok(())
    }
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        *self = bincode::deserialize_from(reader)?;
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&mut self, array: &mut dyn MutableArrayBuilder) -> Result<()> {
        let datatype: DataType = array.data_type();
        with_match_primitive_type!(datatype, |$T| {
            let mut array = array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArrayBuilder<$T>>()
                .ok_or_else(|| {
                    ErrorCode::UnexpectedError(
                        "error occured when downcast MutableArray".to_string(),
                    )
                })?;
            if self.data.is_null() {
                array.push_null();
            }else {
                if self.data.is_integer() {
                    let x = self.data.as_i64()?;
                    array.push(x as $T);
                }else {
                    let x = self.data.as_f64()?;
                    array.push(x as $T);
                }
            }
        },
        {
            match &self.data {
                DataValue::Boolean(val) => {
                    let mut array = array
                    .as_mut_any()
                    .downcast_mut::<MutableBooleanArrayBuilder>()
                    .ok_or_else(|| {
                        ErrorCode::UnexpectedError(
                            "error occured when downcast MutableArray".to_string(),
                        )
                    })?;
                    array.push_option(*val);
                }
                DataValue::String(val) => {
                    let mut array = array
                    .as_mut_any()
                    .downcast_mut::<MutableStringArrayBuilder>()
                    .ok_or_else(|| {
                        ErrorCode::UnexpectedError(
                            "error occured when downcast MutableArray".to_string(),
                        )
                    })?;
                    array.push_option(val.as_ref());
                },
                _ => {
                    return Err(ErrorCode::UnexpectedError(
                        "aggregate arg_min_max unexpected datatype".to_string(),
                    ))
                }
            }
        });
        Ok(())
    }
}

impl StringState {
    fn merge_value(&mut self, data: DataValue, other: &[u8], is_min: bool) {
        match &self.value {
            Some(a) => {
                let ord = a.as_slice().partial_cmp(other);
                match (ord, is_min) {
                    (Some(Ordering::Greater), true) | (Some(Ordering::Less), false) => {
                        self.value = Some(other.to_vec());
                        self.data = data;
                    }
                    _ => {}
                }
            }
            _ => {
                self.value = Some(other.to_vec());
                self.data = data;
            }
        }
    }
}
impl AggregateArgMinMaxState for StringState {
    fn new(data_type: &DataType) -> Self {
        Self {
            value: None,
            data: data_type.into(),
        }
    }

    fn add_keys(
        places: &[StateAddr],
        offset: usize,
        data_series: &Series,
        series: &Series,
        _rows: usize,
        is_min: bool,
    ) -> Result<()> {
        let array: &DFStringArray = series.static_cast();
        array
            .into_iter()
            .zip(places.iter().enumerate())
            .try_for_each(|(key, (idx, addr))| -> Result<()> {
                let data = data_series.try_get(idx)?;

                let place = addr.next(offset);
                let state = place.get::<Self>();
                if let Some(v) = key {
                    state.merge_value(data, v, is_min);
                }
                Ok(())
            })
    }

    fn add_batch(&mut self, data_series: &Series, series: &Series, is_min: bool) -> Result<()> {
        let arg_result = if is_min {
            series.arg_min()
        } else {
            series.arg_max()
        }?;

        if let DataValue::Struct(index_value) = arg_result {
            if index_value[0].is_null() {
                return Ok(());
            }
            let value: Result<Vec<u8>> = DFTryFrom::try_from(index_value[1].clone());

            if let Ok(other) = value {
                let data = data_series.try_get(index_value[0].as_u64()? as usize)?;
                self.merge_value(data, &other, is_min);
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self, is_min: bool) -> Result<()> {
        if let Some(other) = &rhs.value {
            self.merge_value(rhs.data.clone(), other.as_slice(), is_min);
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        let writer = BufMut::writer(writer);
        bincode::serialize_into(writer, self)?;
        Ok(())
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        *self = bincode::deserialize_from(reader)?;
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&mut self, array: &mut dyn MutableArrayBuilder) -> Result<()> {
        let datatype: DataType = array.data_type();
        with_match_primitive_type!(datatype, |$T| {
            let mut array = array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArrayBuilder<$T>>()
                .ok_or_else(|| {
                    ErrorCode::UnexpectedError(
                        "error occured when downcast MutableArray".to_string(),
                    )
                })?;
            if self.data.is_null() {
                array.push_null();
            }else {
                if self.data.is_integer() {
                    let x = self.data.as_i64()?;
                    array.push(x as $T);
                }else {
                    let x = self.data.as_f64()?;
                    array.push(x as $T);
                }
            }
        },
        {
            match &self.data {
                DataValue::String(val) => {
                    let mut array = array
                    .as_mut_any()
                    .downcast_mut::<MutableStringArrayBuilder>()
                    .ok_or_else(|| {
                        ErrorCode::UnexpectedError(
                            "error occured when downcast MutableArray".to_string(),
                        )
                    })?;
                array.push_option(val.as_ref());
                },
                _ => {
                    return Err(ErrorCode::UnexpectedError(
                        "aggregate arg_min_max unexpected datatype".to_string(),
                    ))
                }
            }
        });
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
where T: AggregateArgMinMaxState //  std::cmp::PartialOrd + DFTryFrom<DataValue> + Send + Sync + Clone + 'static,
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

    fn init_state(&self, place: StateAddr) {
        place.write(|| T::new(self.arguments[0].data_type()));
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<T>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state: &mut T = place.get();
        state.add_batch(&arrays[0], &arrays[1], self.is_min)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        input_rows: usize,
    ) -> Result<()> {
        T::add_keys(
            places,
            offset,
            &arrays[0],
            &arrays[1],
            input_rows,
            self.is_min,
        )
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

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableArrayBuilder) -> Result<()> {
        let state = place.get::<T>();
        state.merge_result(array)
    }
}

impl<T> fmt::Display for AggregateArgMinMaxFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateArgMinMaxFunction<T>
where T: AggregateArgMinMaxState
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

pub fn try_create_aggregate_arg_minmax_function<const IS_MIN: bool>(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, arguments.len())?;
    let data_type = arguments[1].data_type();

    with_match_primitive_type!(data_type, |$T| {
        type AggState = NumericState<$T>;
        if IS_MIN {
             AggregateArgMinMaxFunction::<AggState>::try_create_arg_min(
                display_name,
                arguments,
            )
        } else {
             AggregateArgMinMaxFunction::<AggState>::try_create_arg_max(
                display_name,
                arguments,
            )
        }

    },

    {
        if data_type == &DataType::String {
            if IS_MIN {
                return AggregateArgMinMaxFunction::<StringState>::try_create_arg_min(
                    display_name,
                    arguments,
                );
            } else {
                return AggregateArgMinMaxFunction::<StringState>::try_create_arg_max(
                    display_name,
                    arguments,
                );
            }
        }

        Err(ErrorCode::BadDataValueType(format!(
            "AggregateArgMinMaxFunction does not support type '{:?}'",
            data_type
        )))
    })
}

pub fn aggregate_arg_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<true>,
    ))
}

pub fn aggregate_arg_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<false>,
    ))
}
