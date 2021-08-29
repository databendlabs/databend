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
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

use crate::arrays::DataArray;
use crate::prelude::*;
use crate::*;

/// This trait is used to compact a column into a Vec<DataValue>.
/// It is mainly used for subquery execution.
/// TODO: This will be very slow, which is not a good way
pub trait ToValues: Debug {
    fn to_values(&self) -> Result<Vec<DataValue>>;
}

fn primitive_type_to_values_impl<T, F>(array: &DataArray<T>, f: F) -> Result<Vec<DataValue>>
where
    T: DFPrimitiveType,
    F: Fn(Option<T::Native>) -> DataValue,
{
    let array = array.downcast_ref();
    let mut values = Vec::with_capacity(array.len());

    if array.null_count() == 0 {
        for index in 0..array.len() {
            values.push(f(Some(array.value(index))))
        }
    } else {
        for index in 0..array.len() {
            match array.is_null(index) {
                true => values.push(f(None)),
                false => values.push(f(Some(array.value(index)))),
            }
        }
    }

    Ok(values)
}

impl ToValues for DataArray<Int8Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Int8)
    }
}

impl ToValues for DataArray<Int16Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Int16)
    }
}

impl ToValues for DataArray<Int32Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Int32)
    }
}

impl ToValues for DataArray<Int64Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Int64)
    }
}

impl ToValues for DataArray<UInt8Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::UInt8)
    }
}

impl ToValues for DataArray<UInt16Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::UInt16)
    }
}

impl ToValues for DataArray<UInt32Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::UInt32)
    }
}

impl ToValues for DataArray<UInt64Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::UInt64)
    }
}

impl ToValues for DataArray<Float32Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Float32)
    }
}

impl ToValues for DataArray<Float64Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Float64)
    }
}

impl ToValues for DataArray<IntervalDayTimeType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::IntervalDayTime)
    }
}

impl ToValues for DataArray<IntervalYearMonthType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::IntervalYearMonth)
    }
}

impl ToValues for DataArray<TimestampSecondType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::TimestampSecond)
    }
}

impl ToValues for DataArray<TimestampNanosecondType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::TimestampNanosecond)
    }
}

impl ToValues for DataArray<TimestampMillisecondType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::TimestampMillisecond)
    }
}

impl ToValues for DataArray<TimestampMicrosecondType> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::TimestampMicrosecond)
    }
}

impl ToValues for DataArray<Date32Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Date32)
    }
}

impl ToValues for DataArray<Date64Type> {
    fn to_values(&self) -> Result<Vec<DataValue>> {
        primitive_type_to_values_impl(self, DataValue::Date64)
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
