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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use databend_common_ast::ast::AlterVirtualColumnStmt;
use databend_common_ast::ast::CreateVirtualColumnStmt;
use databend_common_ast::ast::DropVirtualColumnStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::RefreshVirtualColumnStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowVirtualColumnsStmt;
use databend_common_ast::ast::VirtualColumn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::get_simple_cast_function;
use databend_common_expression::types::DataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::VirtualField;
use log::debug;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::AlterVirtualColumnPlan;
use crate::plans::CreateVirtualColumnPlan;
use crate::plans::DropVirtualColumnPlan;
use crate::plans::Plan;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RewriteKind;
use crate::resolve_type_name;
use crate::BindContext;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_virtual_column(
        &mut self,
        stmt: &CreateVirtualColumnStmt,
    ) -> Result<Plan> {
        let CreateVirtualColumnStmt {
            create_option,
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
        if table_info.is_temp() {
            return Err(ErrorCode::SemanticError(format!(
                "Table {} is temporary table, creating virtual column not allowed",
                table_info.name()
            )));
        }
        let schema = table_info.schema();

        let virtual_columns = self
            .analyze_virtual_columns(virtual_columns, schema)
            .await?;

        Ok(Plan::CreateVirtualColumn(Box::new(
            CreateVirtualColumnPlan {
                create_option: create_option.clone().into(),
                catalog,
                database,
                table,
                virtual_columns,
                auto_generated: false,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_virtual_column(
        &mut self,
        stmt: &AlterVirtualColumnStmt,
    ) -> Result<Plan> {
        let AlterVirtualColumnStmt {
            if_exists,
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
            if_exists: *if_exists,
            catalog,
            database,
            table,
            virtual_columns,
            auto_generated: false,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_virtual_column(
        &mut self,
        stmt: &DropVirtualColumnStmt,
    ) -> Result<Plan> {
        let DropVirtualColumnStmt {
            if_exists,
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
            if_exists: *if_exists,
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
        virtual_columns: &[VirtualColumn],
        schema: TableSchemaRef,
    ) -> Result<Vec<VirtualField>> {
        let mut alias_name_set = HashSet::new();
        let mut virtual_field_map = BTreeMap::new();
        for virtual_column in virtual_columns.iter() {
            let mut typ = None;
            let mut expr = *virtual_column.expr.clone();
            match expr {
                Expr::Cast {
                    expr: inner_expr,
                    target_type,
                    ..
                } => {
                    expr = *inner_expr.clone();
                    typ = Some(target_type);
                }
                Expr::TryCast {
                    expr: inner_expr,
                    target_type,
                    ..
                } => {
                    expr = *inner_expr.clone();
                    typ = Some(target_type);
                }
                _ => {}
            }
            let mut paths = VecDeque::new();
            while let Expr::MapAccess {
                expr: inner_expr,
                accessor,
                ..
            } = expr
            {
                expr = *inner_expr;
                let path = match accessor {
                    MapAccessor::Bracket {
                        key: box Expr::Literal { value, .. },
                    } => value.clone(),
                    MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                    MapAccessor::DotNumber { key } => Literal::UInt64(key),
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
            if let Expr::ColumnRef { ref column, .. } = expr {
                if let Ok(field) = schema.field_with_name(column.column.name()) {
                    if field.data_type().remove_nullable() != TableDataType::Variant {
                        return Err(ErrorCode::SemanticError(
                            "Virtual Column only support Variant data type",
                        ));
                    }
                    let mut virtual_name = String::new();
                    virtual_name.push_str(column.column.name());
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

                    let data_type = if let Some(typ) = typ {
                        let data_type = resolve_type_name(&typ, false)?;
                        let dest_type = DataType::from(&data_type.remove_nullable());
                        let cast_func_name =
                            get_simple_cast_function(true, &DataType::Variant, &dest_type);
                        if cast_func_name.is_none() {
                            return Err(ErrorCode::SemanticError(format!(
                                "Unsupported cast data type: {:?}",
                                typ
                            )));
                        }
                        data_type.wrap_nullable()
                    } else {
                        TableDataType::Nullable(Box::new(TableDataType::Variant))
                    };

                    if virtual_field_map.contains_key(&virtual_name) {
                        return Err(ErrorCode::SemanticError(format!(
                            "Duplicate virtual column: {}",
                            virtual_name
                        )));
                    }

                    let alias_name = virtual_column
                        .alias
                        .as_ref()
                        .map(|ident| self.normalize_identifier(ident).name.clone());

                    if let Some(alias_name) = &alias_name {
                        if schema.field_with_name(alias_name).is_ok() {
                            return Err(ErrorCode::SemanticError(format!(
                                "Virtual column alias name {} conflict with table field",
                                alias_name
                            )));
                        }
                        if alias_name_set.contains(alias_name) {
                            return Err(ErrorCode::SemanticError(format!(
                                "Virtual column alias name {} duplicate",
                                alias_name
                            )));
                        }
                        alias_name_set.insert(alias_name.clone());
                    }
                    let virtual_field = VirtualField {
                        expr: virtual_name.clone(),
                        data_type,
                        alias_name: alias_name.clone(),
                    };
                    virtual_field_map.insert(virtual_name, virtual_field);
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
        let virtual_fields = virtual_field_map.into_values().collect::<Vec<_>>();
        Ok(virtual_fields)
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_virtual_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowVirtualColumnsStmt,
    ) -> Result<Plan> {
        let ShowVirtualColumnsStmt {
            catalog,
            database,
            table,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog).await?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let database = match database {
            None => self.ctx.get_current_database(),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                catalog
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                database
            }
        };

        let mut select_builder = SelectBuilder::from("default.system.virtual_columns");
        select_builder
            .with_column("database")
            .with_column("table")
            .with_column("source_column")
            .with_column("virtual_column_id")
            .with_column("virtual_column_name")
            .with_column("virtual_column_type");

        select_builder.with_filter(format!("database = '{database}'"));
        if let Some(table) = table {
            let table = normalize_identifier(table, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("table = '{table}'"));
        }

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("virtual_column_name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show virtual columns rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowVirtualColumns)
            .await
    }
}
