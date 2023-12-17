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

use std::collections::HashSet;
use std::collections::VecDeque;

use databend_common_ast::ast::AlterVirtualColumnStmt;
use databend_common_ast::ast::CreateVirtualColumnStmt;
use databend_common_ast::ast::DropVirtualColumnStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::RefreshVirtualColumnStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;

use crate::binder::Binder;
use crate::plans::AlterVirtualColumnPlan;
use crate::plans::CreateVirtualColumnPlan;
use crate::plans::DropVirtualColumnPlan;
use crate::plans::Plan;
use crate::plans::RefreshVirtualColumnPlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_virtual_column(
        &mut self,
        stmt: &CreateVirtualColumnStmt,
    ) -> Result<Plan> {
        let CreateVirtualColumnStmt {
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

        Ok(Plan::CreateVirtualColumn(Box::new(
            CreateVirtualColumnPlan {
                catalog,
                database,
                table,
                virtual_columns,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_virtual_column(
        &mut self,
        stmt: &AlterVirtualColumnStmt,
    ) -> Result<Plan> {
        let AlterVirtualColumnStmt {
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

        Ok(Plan::AlterVirtualColumn(Box::new(AlterVirtualColumnPlan {
            catalog,
            database,
            table,
            virtual_columns,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_virtual_column(
        &mut self,
        stmt: &DropVirtualColumnStmt,
    ) -> Result<Plan> {
        let DropVirtualColumnStmt {
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

        Ok(Plan::DropVirtualColumn(Box::new(DropVirtualColumnPlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_virtual_column(
        &mut self,
        stmt: &RefreshVirtualColumnStmt,
    ) -> Result<Plan> {
        let RefreshVirtualColumnStmt {
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

        Ok(Plan::RefreshVirtualColumn(Box::new(
            RefreshVirtualColumnPlan {
                catalog,
                database,
                table,
                segment_locs: None,
            },
        )))
    }

    #[async_backtrace::framed]
    async fn analyze_virtual_columns(
        &mut self,
        virtual_columns: &[Expr],
        schema: TableSchemaRef,
    ) -> Result<Vec<String>> {
        let mut virtual_names = HashSet::with_capacity(virtual_columns.len());
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
                    MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                    MapAccessor::DotNumber { key } => Literal::UInt64(*key),
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
                if let Ok(field) = schema.field_with_name(column.name()) {
                    if field.data_type().remove_nullable() != TableDataType::Variant {
                        return Err(ErrorCode::SemanticError(
                            "Virtual Column only support Variant data type",
                        ));
                    }
                    let mut virtual_name = String::new();
                    virtual_name.push_str(column.name());
                    for path in paths {
                        virtual_name.push('[');
                        match path {
                            Literal::UInt64(idx) => {
                                virtual_name.push_str(&idx.to_string());
                            }
                            Literal::String(field) => {
                                virtual_name.push('\'');
                                virtual_name.push_str(field.as_ref());
                                virtual_name.push('\'');
                            }
                            _ => unreachable!(),
                        }
                        virtual_name.push(']');
                    }
                    virtual_names.insert(virtual_name);
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
        let mut virtual_columns: Vec<_> = virtual_names.into_iter().collect();
        virtual_columns.sort();

        Ok(virtual_columns)
    }
}
