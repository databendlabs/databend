// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::types::DataTypeImpl;

pub struct MutableArrayColumn {
    inner_data_type: DataTypeImpl,
    last_offset: usize,
    offsets: Vec<i64>,
    pub inner_column: Box<dyn MutableColumn>,
}

impl MutableArrayColumn {
    #[inline]
    pub fn append_value(&mut self, array_value: ArrayValue) {
        let offset = array_value.values.len();
        for value in array_value.values {
            self.inner_column.append_data_value(value).unwrap();
        }
        self.add_offset(offset);
    }

    #[inline]
    pub fn pop_value(&mut self) -> Option<ArrayValue> {
        match self.pop_offset() {
            Some(offset) => {
                let mut values = Vec::with_capacity(offset - self.last_offset);
                for _ in self.last_offset..offset {
                    let value = self.inner_column.pop_data_value().unwrap();
                    values.push(value);
                }
                values.reverse();
                Some(ArrayValue::new(values))
            }
            None => None,
        }
    }

    #[inline]
    pub fn add_offset(&mut self, offset: usize) {
        self.last_offset += offset;
        self.offsets.push(self.last_offset as i64);
    }

    pub fn pop_offset(&mut self) -> Option<usize> {
        (self.offsets.len() > 1).then(|| {
            let offset = self.offsets.pop().unwrap() as usize;
            self.last_offset = *self.offsets.last().unwrap() as usize;
            offset
        })
    }
}

impl Default for MutableArrayColumn {
    fn default() -> Self {
        Self::with_capacity_meta(0, ColumnMeta::Array {
            inner_type: UInt64Type::new_impl(),
        })
    }
}

impl MutableColumn for MutableArrayColumn {
    fn data_type(&self) -> DataTypeImpl {
        ArrayType::new_impl(self.inner_data_type.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn append_default(&mut self) {
        self.add_offset(0);
    }

    fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.inner_column.shrink_to_fit();
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn to_column(&mut self) -> ColumnRef {
        Arc::new(self.finish())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        let mut offset = 0;
        match value {
            DataValue::Array(vals) => {
                offset += vals.len();
                for val in vals {
                    self.inner_column.append_data_value(val)?;
                }
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Cannot convert {:?} to Array",
                    value,
                )))
            }
        }
        self.add_offset(offset);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        match self.pop_value() {
            Some(array_value) => Ok(DataValue::Array(array_value.values)),
            None => Err(ErrorCode::BadDataArrayLength(
                "Array column is empty when pop data value",
            )),
        }
    }
}

impl ScalarColumnBuilder for MutableArrayColumn {
    type ColumnType = ArrayColumn;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use with_capacity_meta.")
    }

    fn with_capacity_meta(capacity: usize, meta: ColumnMeta) -> Self {
        match meta {
            ColumnMeta::Array { inner_type } => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);

                Self {
                    inner_data_type: inner_type.clone(),
                    last_offset: 0,
                    offsets,
                    inner_column: inner_type.create_mutable(capacity),
                }
            }
            _ => panic!("must be ColumnMeta::Array"),
        }
    }

    fn push(&mut self, value: <Self::ColumnType as ScalarColumn>::RefItem<'_>) {
        let mut offset = 0;
        match value {
            ArrayValueRef::Indexed { column, idx } => {
                let value = column.get(idx);
                if let DataValue::Array(vals) = value {
                    offset += vals.len();
                    for val in vals {
                        self.inner_column.append_data_value(val).unwrap();
                    }
                }
            }
            ArrayValueRef::ValueRef { val } => {
                offset += val.values.len();
                for value in val.values.clone() {
                    self.inner_column.append_data_value(value).unwrap();
                }
            }
        }
        self.add_offset(offset);
    }

    fn finish(&mut self) -> Self::ColumnType {
        self.shrink_to_fit();
        ArrayColumn::from_data(
            self.data_type(),
            self.offsets.clone().into(),
            self.inner_column.to_column(),
        )
    }
}
