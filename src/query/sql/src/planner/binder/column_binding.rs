// Copyright 2021 Datafuse Labs
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

use databend_common_expression::types::DataType;
use databend_common_expression::ColumnIndex;

use crate::IndexType;
use crate::Visibility;

// Please use `ColumnBindingBuilder` to construct a new `ColumnBinding`
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ColumnBinding {
    /// Database name of this `ColumnBinding` in current context
    pub database_name: Option<String>,
    pub table_name: Option<String>,
    /// Column Position of this `ColumnBinding` in current context
    pub column_position: Option<usize>,
    /// Table index of this `ColumnBinding` in current context
    pub table_index: Option<IndexType>,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    /// Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: Box<DataType>,

    pub visibility: Visibility,

    pub virtual_computed_expr: Option<String>,
}

const DUMMY_INDEX: usize = usize::MAX;
impl ColumnBinding {
    pub fn new_dummy_column(name: String, data_type: Box<DataType>) -> Self {
        ColumnBinding {
            database_name: None,
            table_name: None,
            column_position: None,
            table_index: None,
            column_name: name,
            index: DUMMY_INDEX,
            data_type,
            visibility: Visibility::Visible,
            virtual_computed_expr: None,
        }
    }
}

// we only care about the index
// if index is same, thus the column binding is same
impl Eq for ColumnBinding {}
impl PartialEq for ColumnBinding {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.data_type == other.data_type
            && ((self.index == DUMMY_INDEX && self.column_name == other.column_name)
                || self.index != DUMMY_INDEX)
    }
}

impl std::hash::Hash for ColumnBinding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.data_type.hash(state);

        if self.index == DUMMY_INDEX {
            self.column_name.hash(state);
        }
    }
}

impl ColumnIndex for ColumnBinding {}

pub struct ColumnBindingBuilder {
    /// Database name of this `ColumnBinding` in current context
    pub database_name: Option<String>,
    /// Table name of this `ColumnBinding` in current context
    pub table_name: Option<String>,
    /// Column Position of this `ColumnBinding` in current context
    pub column_position: Option<usize>,
    /// Table index of this `ColumnBinding` in current context
    pub table_index: Option<IndexType>,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    /// Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: Box<DataType>,

    pub visibility: Visibility,

    pub virtual_computed_expr: Option<String>,
}

impl ColumnBindingBuilder {
    pub fn new(
        column_name: String,
        index: IndexType,
        data_type: Box<DataType>,
        visibility: Visibility,
    ) -> ColumnBindingBuilder {
        ColumnBindingBuilder {
            database_name: None,
            table_name: None,
            column_position: None,
            table_index: None,
            column_name,
            index,
            data_type,
            visibility,
            virtual_computed_expr: None,
        }
    }

    pub fn database_name(mut self, name: Option<String>) -> ColumnBindingBuilder {
        self.database_name = name;
        self
    }

    pub fn table_name(mut self, name: Option<String>) -> ColumnBindingBuilder {
        self.table_name = name;
        self
    }

    pub fn column_position(mut self, pos: Option<usize>) -> ColumnBindingBuilder {
        self.column_position = pos;
        self
    }

    pub fn table_index(mut self, index: Option<IndexType>) -> ColumnBindingBuilder {
        self.table_index = index;
        self
    }

    pub fn virtual_computed_expr(mut self, vir: Option<String>) -> ColumnBindingBuilder {
        self.virtual_computed_expr = vir;
        self
    }

    pub fn build(self) -> ColumnBinding {
        ColumnBinding {
            database_name: self.database_name,
            table_name: self.table_name,
            column_position: self.column_position,
            table_index: self.table_index,
            column_name: self.column_name,
            index: self.index,
            data_type: self.data_type,
            visibility: self.visibility,
            virtual_computed_expr: self.virtual_computed_expr,
        }
    }
}
