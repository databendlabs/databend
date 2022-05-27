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

use crate::ColumnRef;
use crate::DataField;
use crate::DataTypeImpl;

#[derive(Clone, Debug)]
pub struct ColumnWithField {
    pub(crate) column: ColumnRef,
    pub(crate) field: DataField,
}
impl ColumnWithField {
    pub fn new(column: ColumnRef, field: DataField) -> Self {
        Self { column, field }
    }
    pub fn column(&self) -> &ColumnRef {
        &self.column
    }
    pub fn field(&self) -> &DataField {
        &self.field
    }
    pub fn data_type(&self) -> &DataTypeImpl {
        self.field.data_type()
    }
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            column: self.column.slice(offset, length),
            field: self.field.to_owned(),
        }
    }
}

pub type ColumnsWithField = [ColumnWithField];
