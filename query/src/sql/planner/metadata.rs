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

use std::io::Write;
use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;

use crate::sql::common::IndexType;
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

    // Table index of column entry. None if column is derived from a subquery.
    pub table_index: Option<IndexType>,
}

impl ColumnEntry {
    pub fn new(
        name: String,
        data_type: DataTypeImpl,
        column_index: IndexType,
        table_index: Option<IndexType>,
    ) -> Self {
        ColumnEntry {
            column_index,
            name,
            data_type,
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

    pub fn get_expr_display_string(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::ColumnRef { column, .. } => Ok(column.name.clone()),

            Expr::Literal { lit, .. } => Ok(format!("{lit}")),

            Expr::CountAll { .. } => Ok("count(*)".to_string()),

            Expr::FunctionCall {
                name,
                distinct,
                args,
                ..
            } => {
                let fun = name.name.as_str();
                let mut names = Vec::new();
                if !optimize_remove_count_args(
                    fun,
                    *distinct,
                    args.iter().collect::<Vec<&Expr>>().as_slice(),
                ) {
                    names = args
                        .iter()
                        .map(|arg| self.get_expr_display_string(arg))
                        .collect::<Result<Vec<String>>>()?;
                }

                Ok(match distinct {
                    true => format!("{}({}{})", fun, "distinct ", names.join(",")),
                    false => format!("{}({}{})", fun, "", names.join(",")),
                })
            }

            Expr::IsNull { expr, not, .. } => Ok(format!(
                "{} IS {}NULL",
                expr,
                if *not { "NOT " } else { "" }
            )),

            Expr::InList {
                expr, list, not, ..
            } => {
                let mut w = vec![];
                write!(&mut w, "{} {}IN (", expr, if *not { "NOT " } else { "" })?;
                for (i, expr) in list.iter().enumerate() {
                    write!(&mut w, "{}", expr)?;
                    if i < list.len() - 1 {
                        write!(&mut w, ", ")?;
                    }
                }
                write!(&mut w, ")")?;

                Ok(String::from_utf8(w)?)
            }

            Expr::Between {
                expr,
                low,
                high,
                not,
                ..
            } => Ok(format!(
                "{} {}BETWEEN {} AND {}",
                expr,
                if *not { "NOT " } else { "" },
                low,
                high
            )),

            Expr::BinaryOp {
                op, left, right, ..
            } => Ok(format!("{} {} {}", left, op, right)),

            Expr::UnaryOp { op, expr, .. } => Ok(format!("{} {}", op, expr)),

            Expr::Cast {
                expr, target_type, ..
            } => Ok(format!("CAST({} AS {})", expr, target_type)),

            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => Ok(format!(
                "SUBSTRING({}{}{})",
                expr,
                if let Some(from) = substring_from {
                    format!("FROM {}", from)
                } else {
                    "".to_string()
                },
                if let Some(for_expr) = substring_for {
                    format!("FOR {}", for_expr)
                } else {
                    "".to_string()
                }
            )),
            _ => Err(ErrorCode::LogicalError(format!(
                "{expr} doesn't implement get_expr_display_string"
            ))),
        }
    }

    pub fn add_column(
        &mut self,
        name: String,
        data_type: DataTypeImpl,
        table_index: Option<IndexType>,
    ) -> IndexType {
        let column_index = self.columns.len();
        let column_entry = ColumnEntry::new(name, data_type, column_index, table_index);
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
                Some(table_index),
            );
        }
        table_index
    }
}

pub fn optimize_remove_count_args(name: &str, distinct: bool, args: &[&Expr]) -> bool {
    name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal{lit,..} if *lit!=Literal::Null))
}
