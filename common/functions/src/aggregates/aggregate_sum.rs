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
use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_datavalues::prelude::*;
use common_datavalues::DFTryFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::AggregateFunctionRef;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_match_primitive_type;

struct AggregateSumState<T> {
    pub value: Option<T>,
}

impl<T> AggregateSumState<T>
where
    T: std::ops::Add<Output = T> + Copy + Clone,
    Option<T>: Serialize + DeserializeOwned,
{
    #[inline(always)]
    fn add(&mut self, other: T) {
        match &self.value {
            Some(a) => self.value = Some(a.add(other)),
            None => self.value = Some(other),
        }
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
    T: DFPrimitiveType + AsPrimitive<SumT>,
    SumT: DFPrimitiveType + std::ops::Add<Output = SumT>,
    Option<SumT>: Into<DataValue>,
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(SumT::data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateSumState::<SumT> { value: None });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateSumState<SumT>>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let value = arrays[0].sum()?;
        let opt_sum: Result<SumT> = DFTryFrom::try_from(value);

        if let Ok(s) = opt_sum {
            let state = place.get::<AggregateSumState<SumT>>();
            state.add(s);
        }

        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        let darray: &DFPrimitiveArray<T> = arrays[0].static_cast();
        if darray.null_count() == 0 {
            darray
                .inner()
                .values()
                .as_slice()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let place = place.next(offset);
                    let state = place.get::<AggregateSumState<SumT>>();
                    state.add(v.as_());
                });
        } else {
            darray
                .into_iter()
                .zip(places.iter())
                .for_each(|(c, place)| {
                    if let Some(v) = c {
                        let place = place.next(offset);
                        let state = place.get::<AggregateSumState<SumT>>();
                        state.add(v.as_());
                    }
                });
        }

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
        if let Some(s) = &rhs.value {
            let state = place.get::<AggregateSumState<SumT>>();
            state.add(*s);
        }
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableArray) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        if let Some(val) = &state.value {
            match array.data_type() {
                ArrowDataType::UInt8 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u8>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_u8());
                }
                ArrowDataType::UInt16 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u16>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_u16());
                }
                ArrowDataType::UInt32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u32>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_u32());
                }
                ArrowDataType::UInt64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<u64>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_u64());
                }
                ArrowDataType::Int8 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i8>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_i8());
                }
                ArrowDataType::Int16 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i16>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_i16());
                }
                ArrowDataType::Int32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i32>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_i32());
                }
                ArrowDataType::Int64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i64>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_i64());
                }
                ArrowDataType::Float32 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f32>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_f32());
                }
                ArrowDataType::Float64 => {
                    let mut array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f64>>()
                        .ok_or_else(|| {
                            ErrorCode::UnexpectedError(
                                "error occured when downcast MutableArray".to_string(),
                            )
                        })?;
                    array.push(val.to_f64());
                }
                _ => {
                    return Err(ErrorCode::UnexpectedError(
                        "unexpected datatype when aggregate".to_string(),
                    ))
                }
            }
        } else {
            array.push_null();
        }
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
    T: DFPrimitiveType + AsPrimitive<SumT>,
    SumT: DFPrimitiveType + std::ops::Add<Output = SumT>,
    Option<SumT>: Into<DataValue>,
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
    with_match_primitive_type!(data_type, |$T| {
        AggregateSumFunction::<$T, <$T as DFPrimitiveType>::LargestType>::try_create(
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
