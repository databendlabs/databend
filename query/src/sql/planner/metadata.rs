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

use common_ast::ast::Expr;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;

use crate::sql::common::IndexType;
use crate::sql::exec::format_field_name;
use crate::storages::Table;

#[derive(Clone)]
pub struct TableEntry {
    pub index: IndexType,
    pub name: String,
    pub catalog: String,
    pub database: String,

    pub table: Arc<dyn Table>,

    pub source: ReadDataSourcePlan,
}

impl TableEntry {
    pub fn new(
        index: IndexType,
        name: String,
        catalog: String,
        database: String,
        table: Arc<dyn Table>,
        source: ReadDataSourcePlan,
    ) -> Self {
        TableEntry {
            index,
            name,
            catalog,
            database,
            table,
            source,
        }
    }
}

#[derive(Clone)]
pub struct ColumnEntry {
    pub column_index: IndexType,
    pub name: String,
    pub data_type: DataTypeImpl,
    pub nullable: bool,

    // Table index of column entry. None if column is derived from a subquery.
    pub table_index: Option<IndexType>,
}

impl ColumnEntry {
    pub fn new(
        name: String,
        data_type: DataTypeImpl,
        nullable: bool,
        column_index: IndexType,
        table_index: Option<IndexType>,
    ) -> Self {
        ColumnEntry {
            column_index,
            name,
            data_type,
            nullable,
            table_index,
        }
    }
}

/// Metadata stores information about columns and tables used in a query.
/// Tables and columns are identified with its unique index, notice that index value of a column can
/// be same with that of a table.
#[derive(Clone, Default)]
pub struct Metadata {
    tables: Vec<TableEntry>,
    columns: Vec<ColumnEntry>,
}

impl Metadata {
    pub fn create() -> Self {
        Self {
            tables: vec![],
            columns: vec![],
        }
    }

    pub fn table(&self, index: IndexType) -> &TableEntry {
        self.tables.get(index).unwrap()
    }

    pub fn column(&self, index: IndexType) -> &ColumnEntry {
        self.columns.get(index).unwrap()
    }

    pub fn columns_by_table_index(&self, index: IndexType) -> Vec<ColumnEntry> {
        let mut result = vec![];
        for col in self.columns.iter() {
            match col.table_index {
                Some(col_index) if col_index == index => {
                    result.push(col.clone());
                }
                _ => {}
            }
        }

        result
    }

    pub fn column_idx_by_column_name(&self, col_name: &str) -> Result<IndexType> {
        for col in self.columns.iter() {
            if col.name == col_name {
                return Ok(col.column_index);
            }
        }
        Err(ErrorCode::LogicalError(format!(
            "Can't find column {col_name} in metadata"
        )))
    }

    fn create_function_display_name(
        &self,
        fun: &str,
        distinct: &bool,
        args: &[Expr],
    ) -> Result<String> {
        let names: Vec<String> = args
            .iter()
            .map(|arg| self.get_expr_display_string(arg, false))
            .collect::<Result<_>>()?;
        Ok(match distinct {
            true => format!("{}({}{})", fun, "distinct ", names.join(",")),
            false => format!("{}({}{})", fun, "", names.join(",")),
        })
    }

    pub fn get_expr_display_string(&self, expr: &Expr, is_first_expr: bool) -> Result<String> {
        match expr {
            Expr::ColumnRef { column, .. } => {
                if is_first_expr {
                    return Ok(column.name.clone());
                }
                let idx = self.column_idx_by_column_name(column.name.as_str())?;
                Ok(format_field_name(column.name.as_str(), idx))
            }
            Expr::FunctionCall {
                name,
                distinct,
                args,
                ..
            } => self.create_function_display_name(name.name.as_str(), distinct, args),
            _ => Err(ErrorCode::LogicalError(
                "{expr} doesn't implement get_expr_display_string",
            )),
        }
    }

    pub fn add_column(
        &mut self,
        name: String,
        data_type: DataTypeImpl,
        nullable: bool,
        table_index: Option<IndexType>,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column_entry = ColumnEntry::new(name, data_type, nullable, column_index, table_index);
        self.columns.push(column_entry);
        column_index
    }

    pub fn add_table(
        &mut self,
        catalog: String,
        database: String,
        table_meta: Arc<dyn Table>,
        source: ReadDataSourcePlan,
    ) -> IndexType {
        let table_name = table_meta.name().to_string();
        let table_index = self.tables.len();
        let table_entry = TableEntry {
            index: table_index,
            name: table_name,
            database,
            catalog,
            table: table_meta.clone(),
            source,
        };
        self.tables.push(table_entry);
        for field in table_meta.schema().fields() {
            self.add_column(
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
                Some(table_index),
            );
        }
        table_index
    }
}
