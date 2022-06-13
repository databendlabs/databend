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

use std::sync::Arc;

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

    #[inline]
    pub fn append_value(&mut self, struct_value: StructValue) {
        for (i, value) in struct_value.values.iter().enumerate() {
            self.inner_columns[i]
                .append_data_value(value.clone())
                .unwrap();
        }
    }

    #[inline]
    pub fn pop_value(&mut self) -> Option<StructValue> {
        if self.inner_columns[0].len() > 0 {
            let mut values = Vec::with_capacity(self.inner_columns.len());
            for inner_column in self.inner_columns.iter_mut() {
                let value = inner_column.pop_data_value().unwrap();
                values.push(value);
            }
            return Some(StructValue::new(values));
        }
        None
    }
}

impl Default for MutableStructColumn {
    fn default() -> Self {
        Self::with_capacity_meta(0, ColumnMeta::Struct {
            inner_names: vec!["default".to_string()],
            inner_types: vec![UInt64Type::new_impl()],
        })
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
        Arc::new(self.finish())
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

impl ScalarColumnBuilder for MutableStructColumn {
    type ColumnType = StructColumn;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use with_capacity_meta.")
    }

    fn with_capacity_meta(capacity: usize, meta: ColumnMeta) -> Self {
        match meta {
            ColumnMeta::Struct {
                inner_names,
                inner_types,
            } => {
                let mut inner_columns = Vec::with_capacity(inner_types.len());
                for inner_type in inner_types.iter() {
                    inner_columns.push(inner_type.create_mutable(capacity));
                }
                let data_type = StructType::new_impl(inner_names, inner_types);

                Self {
                    data_type,
                    inner_columns,
                }
            }
            _ => panic!("must be ColumnMeta::Struct"),
        }
    }

    fn push(&mut self, value: <Self::ColumnType as ScalarColumn>::RefItem<'_>) {
        match value {
            StructValueRef::Indexed { column, idx } => {
                let value = column.get(idx);
                if let DataValue::Struct(vals) = value {
                    for (i, v) in vals.iter().enumerate() {
                        self.inner_columns[i].append_data_value(v.clone()).unwrap();
                    }
                }
            }
            StructValueRef::ValueRef { val } => {
                for (i, v) in val.values.iter().enumerate() {
                    self.inner_columns[i].append_data_value(v.clone()).unwrap();
                }
            }
        }
    }

    fn finish(&mut self) -> Self::ColumnType {
        self.shrink_to_fit();
        let columns: Vec<ColumnRef> = self
            .inner_columns
            .iter_mut()
            .map(|inner| inner.to_column())
            .collect();

        StructColumn::from_data(columns, self.data_type.clone())
    }
}
