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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

pub struct MutableStructColumn {
    pub data_type: DataTypeImpl,
    pub inner_columns: Vec<Box<dyn MutableColumn>>,
}

impl MutableStructColumn {
    pub fn from_data(data_type: DataTypeImpl, inner_columns: Vec<Box<dyn MutableColumn>>) -> Self {
        Self {
            data_type,
            inner_columns,
        }
    }
}

impl MutableColumn for MutableStructColumn {
    fn data_type(&self) -> DataTypeImpl {
        self.data_type.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn append_default(&mut self) {
        for inner in self.inner_columns.iter_mut() {
            inner.append_default();
        }
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn shrink_to_fit(&mut self) {
        for inner in self.inner_columns.iter_mut() {
            inner.shrink_to_fit();
        }
    }

    fn len(&self) -> usize {
        if self.inner_columns.is_empty() {
            return 0;
        }
        self.inner_columns[0].len()
    }

    fn to_column(&mut self) -> ColumnRef {
        let columns: Vec<ColumnRef> = self
            .inner_columns
            .iter_mut()
            .map(|inner| inner.to_column())
            .collect();

        StructColumn::from_data(columns, self.data_type.clone()).arc()
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        match &value {
            DataValue::Struct(struct_) => {
                if struct_.len() != self.inner_columns.len() {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "DataValue Error: Cannot convert {:?} to Struct, length mismatch",
                        value,
                    )));
                }

                for (inner, value) in self.inner_columns.iter_mut().zip(struct_.iter()) {
                    inner.append_data_value(value.clone())?;
                }
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Cannot convert {:?} to Struct",
                    value,
                )))
            }
        }
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        let mut values: Vec<DataValue> = Vec::with_capacity(self.inner_columns.len());
        for inner in self.inner_columns.iter_mut() {
            values.push(inner.pop_data_value()?);
        }
        Ok(DataValue::Struct(values))
    }
}
