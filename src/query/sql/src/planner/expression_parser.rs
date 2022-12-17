//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::Expression;
use common_catalog::table::Table;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_settings::Settings;
use parking_lot::RwLock;

use crate::executor::ExpressionBuilderWithoutRenaming;
use crate::planner::semantic::SyncTypeChecker;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::Visibility;

pub struct ExpressionParser;

impl ExpressionParser {
    pub fn parse_exprs(table_meta: Arc<dyn Table>, sql: &str) -> Result<Vec<Expression>> {
        let sql_dialect = Dialect::MySQL;
        let tokens = tokenize_sql(sql)?;
        let backtrace = Backtrace::new();
        let exprs = parse_comma_separated_exprs(&tokens[1..tokens.len()], sql_dialect, &backtrace)?;

        let settings = Settings::default_settings("", GlobalConfig::instance())?;
        let mut bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let table_index = metadata.write().add_table(
            CATALOG_DEFAULT.to_owned(),
            "default".to_string(),
            table_meta,
            None,
        );

        let columns = metadata.read().columns_by_table_index(table_index);
        let table = metadata.read().table(table_index).clone();
        for column in columns.iter() {
            let column_binding = match column {
                ColumnEntry::BaseTableColumn {
                    column_index,
                    column_name,
                    data_type,
                    path_indices,
                    ..
                } => ColumnBinding {
                    database_name: Some("default".to_string()),
                    table_name: Some(table.name().to_string()),
                    column_name: column_name.clone(),
                    index: *column_index,
                    data_type: Box::new(data_type.clone()),
                    visibility: if path_indices.is_some() {
                        Visibility::InVisible
                    } else {
                        Visibility::Visible
                    },
                },

                _ => {
                    return Err(ErrorCode::Internal("Invalid column entry"));
                }
            };

            bind_context.add_column_binding(column_binding);
        }

        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut type_checker = SyncTypeChecker::new(&bind_context, &name_resolution_ctx, &[]);
        let mut expressions = Vec::with_capacity(exprs.len());

        let builder = ExpressionBuilderWithoutRenaming::create(metadata);

        for expr in exprs.iter() {
            let (scalar, _) = *type_checker.resolve(expr, None)?;
            let expr = builder.build(&scalar)?;
            expressions.push(expr);
        }
        Ok(expressions)
    }
}
