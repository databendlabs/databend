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

use std::fmt::Debug;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

use crate::prelude::*;
use crate::*;

/// This trait is used to compact a column into a Vec<DataValue>.
/// It is mainly used for subquery execution.
/// TODO: This will be very slow, which is not a good way
pub trait ToValues: Debug {
    fn to_values(&self) -> Result<Vec<DataValue>>;
}

fn primitive_type_to_values_impl<T>(array: &DFPrimitiveArray<T>) -> Result<Vec<DataValue>>
where
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
{
    let array = array.downcast_ref();
    let mut values = Vec::with_capacity(array.len());

    if array.null_count() == 0 {
        for index in 0..array.len() {
            values.push(Some(array.value(index)).into())
        }
    } else {
        for index in 0..array.len() {
            let v: Option<T> = match array.is_null(index) {
                true => None,
                false => Some(array.value(index)),
            };
            values.push(v.into())
        }
    }

    Ok(values)
}

impl<T> ToValues for DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    Option<T>: Into<DataValue>,
{
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self)
    }
}

impl ToValues for DFUtf8Array {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        let mut values = Vec::with_capacity(self.len());
        let array = self.downcast_ref();

        if array.null_count() == 0 {
            for index in 0..self.len() {
                values.push(DataValue::Utf8(Some(array.value(index).to_string())))
            }
        } else {
            for index in 0..self.len() {
                match array.is_null(index) {
                    true => values.push(DataValue::Utf8(None)),
                    false => values.push(DataValue::Utf8(Some(array.value(index).to_string()))),
                }
            }
        }

        Ok(values)
    }
}

impl ToValues for DFBooleanArray {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        let mut values = Vec::with_capacity(self.len());
        let array = self.downcast_ref();

        if array.null_count() == 0 {
            for index in 0..self.len() {
                values.push(DataValue::Boolean(Some(array.value(index))))
            }
        } else {
            for index in 0..self.len() {
                match array.is_null(index) {
                    true => values.push(DataValue::Boolean(None)),
                    false => values.push(DataValue::Boolean(Some(array.value(index)))),
                }
            }
        }

        Ok(values)
    }
}

impl ToValues for DFBinaryArray {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        let mut values = Vec::with_capacity(self.len());
        let array = self.downcast_ref();

        if array.null_count() == 0 {
            for index in 0..self.len() {
                values.push(DataValue::Binary(Some(array.value(index).to_vec())))
            }
        } else {
            for index in 0..self.len() {
                match array.is_null(index) {
                    true => values.push(DataValue::Binary(None)),
                    false => values.push(DataValue::Binary(Some(array.value(index).to_vec()))),
                }
            }
        }

        Ok(values)
    }
}

impl ToValues for DFListArray {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        let mut values = Vec::with_capacity(self.len());
        let array = self.downcast_ref();

        if let DataType::List(ele_type) = DataType::from(array.data_type()) {
            let data_type = ele_type.data_type();
            if array.null_count() == 0 {
                for index in 0..self.len() {
                    let list: ArrayRef = Arc::from(array.value(index));
                    let list_values = list.into_series().to_values()?;
                    values.push(DataValue::List(Some(list_values), data_type.clone()))
                }
            } else {
                for index in 0..self.len() {
                    match array.is_null(index) {
                        true => values.push(DataValue::List(None, data_type.clone())),
                        false => {
                            let list: ArrayRef = Arc::from(array.value(index));
                            let list_values = list.into_series().to_values()?;
                            values.push(DataValue::List(Some(list_values), data_type.clone()))
                        }
                    };
                }
            }
        }

        Ok(values)
    }
}

impl ToValues for DFNullArray {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        Ok((0..self.len()).map(|_| DataValue::Null).collect())
    }
}

impl ToValues for DFStructArray {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        let mut values = Vec::with_capacity(self.len());
        let array = self.downcast_ref();

        let mut columns_values = Vec::with_capacity(array.fields().len());

        for column in array.values() {
            let series = column.clone().into_series();
            columns_values.push(series.to_values()?);
        }

        for index in 0..self.len() {
            let mut struct_fields = Vec::with_capacity(columns_values.len());
            for column_values in &columns_values {
                struct_fields.push(column_values[index].clone())
            }

            values.push(DataValue::Struct(struct_fields))
        }

        Ok(values)
    }
}
