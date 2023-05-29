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

use std::collections::VecDeque;

use common_ast::ast::CreateVirtualColumnsStmt;
use common_ast::ast::DropVirtualColumnsStmt;
use common_ast::ast::Expr;
use common_ast::ast::GenerateVirtualColumnsStmt;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableDataType;
use common_expression::TableSchemaRef;

use crate::binder::Binder;
use crate::plans::CreateVirtualColumnsPlan;
use crate::plans::DropVirtualColumnsPlan;
use crate::plans::GenerateVirtualColumnsPlan;
use crate::plans::Plan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_virtual_columns(
        &mut self,
        stmt: &CreateVirtualColumnsStmt,
    ) -> Result<Plan> {
        let CreateVirtualColumnsStmt {
            catalog,
            database,
            table,
            virtual_columns,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table_info = self.ctx.get_table(&catalog, &database, &table).await?;
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::SemanticError(
                "Virtual Column only support FUSE engine",
            ));
        }
        let schema = table_info.schema();

        let virtual_columns = self
            .analyze_virtual_columns(virtual_columns, schema)
            .await?;

        Ok(Plan::CreateVirtualColumns(Box::new(
            CreateVirtualColumnsPlan {
                catalog,
                database,
                table,
                virtual_columns,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_virtual_columns(
        &mut self,
        stmt: &DropVirtualColumnsStmt,
    ) -> Result<Plan> {
        let DropVirtualColumnsStmt {
            catalog,
            database,
            table,
            virtual_columns,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table_info = self.ctx.get_table(&catalog, &database, &table).await?;
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::SemanticError(
                "Virtual Column only support FUSE engine",
            ));
        }
        let schema = table_info.schema();

        let virtual_columns = self
            .analyze_virtual_columns(virtual_columns, schema)
            .await?;

        Ok(Plan::DropVirtualColumns(Box::new(DropVirtualColumnsPlan {
            catalog,
            database,
            table,
            virtual_columns,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_generate_virtual_columns(
        &mut self,
        stmt: &GenerateVirtualColumnsStmt,
    ) -> Result<Plan> {
        let GenerateVirtualColumnsStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table_info = self.ctx.get_table(&catalog, &database, &table).await?;
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::SemanticError(
                "Virtual Column only support FUSE engine",
            ));
        }

        Ok(Plan::GenerateVirtualColumns(Box::new(
            GenerateVirtualColumnsPlan {
                catalog,
                database,
                table,
            },
        )))
    }

    #[async_backtrace::framed]
    async fn analyze_virtual_columns(
        &mut self,
        virtual_columns: &[Expr],
        schema: TableSchemaRef,
    ) -> Result<Vec<String>> {
        let mut virtual_names = Vec::with_capacity(virtual_columns.len());
        for virtual_column in virtual_columns.iter() {
            let mut expr = virtual_column;
            let mut paths = VecDeque::new();
            while let Expr::MapAccess {
                expr: inner_expr,
                accessor,
                ..
            } = expr
            {
                expr = &**inner_expr;
                let path = match accessor {
                    MapAccessor::Bracket {
                        key: box Expr::Literal { lit, .. },
                    } => lit.clone(),
                    MapAccessor::Period { key } | MapAccessor::Colon { key } => {
                        Literal::String(key.name.clone())
                    }
                    MapAccessor::PeriodNumber { key } => Literal::UInt64(*key),
                    _ => {
                        return Err(ErrorCode::SemanticError(format!(
                            "Unsupported accessor: {:?}",
                            accessor
                        )));
                    }
                };
                paths.push_front(path);
            }
            if paths.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Virtual Column should be a inner field of Variant Column",
                ));
            }
            if let Expr::ColumnRef { column, .. } = expr {
                if let Ok(field) = schema.field_with_name(&column.name) {
                    if field.data_type().remove_nullable() != TableDataType::Variant {
                        return Err(ErrorCode::SemanticError(
                            "Virtual Column only support Variant data type",
                        ));
                    }
                    let mut virtual_name = String::new();
                    virtual_name.push_str(&column.name);
                    while let Some(path) = paths.pop_front() {
                        match path {
                            Literal::UInt64(idx) => {
                                virtual_name.push('[');
                                virtual_name.push_str(&idx.to_string());
                                virtual_name.push(']');
                            }
                            Literal::String(field) => {
                                virtual_name.push(':');
                                virtual_name.push_str(field.as_ref());
                            }
                            _ => unreachable!(),
                        }
                    }
                    virtual_names.push(virtual_name);
                } else {
                    return Err(ErrorCode::SemanticError(format!(
                        "Column is not exist: {:?}",
                        expr
                    )));
                }
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "Unsupported expr: {:?}",
                    expr
                )));
            }
        }

        Ok(virtual_names)
    }
}
