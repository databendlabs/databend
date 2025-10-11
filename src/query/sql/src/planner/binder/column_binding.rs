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
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq, Hash)]
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
    // Optional virtual expr, used by virtual computed column and variant virtual column,
    // `virtual_expr` will be parsed and bind to a `ScalarExpr`.
    pub virtual_expr: Option<String>,
    // A flag indicates the column binding from srf or not
    pub is_srf: bool,
}

const DUMMY_INDEX: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum DummyColumnType {
    WindowFunction = 1,
    AggregateFunction = 2,
    Subquery = 3,
    UDF = 4,
    AsyncFunction = 5,
    Other = 6,
}

impl DummyColumnType {
    fn type_identifier(&self) -> usize {
        DUMMY_INDEX - (*self) as usize
    }
}

impl ColumnBinding {
    pub fn new_dummy_column(
        name: String,
        data_type: Box<DataType>,
        dummy_type: DummyColumnType,
    ) -> Self {
        let index = dummy_type.type_identifier();
        ColumnBinding {
            database_name: None,
            table_name: None,
            column_position: None,
            table_index: None,
            column_name: name,
            index,
            data_type,
            visibility: Visibility::Visible,
            virtual_expr: None,
            is_srf: false,
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.index >= DummyColumnType::Other.type_identifier()
    }
}

impl ColumnIndex for ColumnBinding {
    fn unique_name<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.index)
    }
}

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

    pub virtual_expr: Option<String>,
    pub is_srf: bool,
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
            virtual_expr: None,
            is_srf: false,
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

    pub fn virtual_expr(mut self, virtual_expr: Option<String>) -> ColumnBindingBuilder {
        self.virtual_expr = virtual_expr;
        self
    }

    pub fn is_srf(mut self, is_srf: bool) -> ColumnBindingBuilder {
        self.is_srf = is_srf;
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
            virtual_expr: self.virtual_expr,
            is_srf: self.is_srf,
        }
    }
}
