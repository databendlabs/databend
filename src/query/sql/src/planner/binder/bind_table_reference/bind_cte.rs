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

use std::sync::Arc;

use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableAlias;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use indexmap::IndexMap;

use crate::binder::BindContext;
use crate::binder::Binder;
use crate::binder::CteContext;
use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::plans::CTEConsumer;
use crate::plans::RelOperator;

impl Binder {
    pub fn bind_cte_consumer(
        &mut self,
        bind_context: &mut BindContext,
        table_name: &str,
        alias: &Option<TableAlias>,
        cte_info: &CteInfo,
    ) -> Result<(SExpr, BindContext)> {
        let (s_expr, cte_bind_context) = self.bind_cte_definition(
            table_name,
            bind_context.cte_context.cte_map.as_ref(),
            &cte_info.query,
        )?;

        let (table_alias, column_alias) = match alias {
            Some(alias) => {
                let table_alias = normalize_identifier(&alias.name, &self.name_resolution_ctx).name;
                let column_alias = if alias.columns.is_empty() {
                    cte_info.columns_alias.clone()
                } else {
                    alias
                        .columns
                        .iter()
                        .map(|column| normalize_identifier(column, &self.name_resolution_ctx).name)
                        .collect()
                };
                (table_alias, column_alias)
            }
            None => (table_name.to_string(), cte_info.columns_alias.clone()),
        };

        if !column_alias.is_empty() && column_alias.len() != cte_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "The CTE '{}' has {} columns ({:?}), but {} aliases ({:?}) were provided. Ensure the number of aliases matches the number of columns in the CTE.",
                table_name,
                cte_bind_context.columns.len(),
                cte_bind_context.columns.iter().map(|c| &c.column_name).collect::<Vec<_>>(),
                column_alias.len(),
                column_alias,
            )));
        }

        let mut cte_output_columns = cte_bind_context.columns.clone();
        for column in cte_output_columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(table_alias.clone());
        }
        for (index, column_name) in column_alias.iter().enumerate() {
            cte_output_columns[index].column_name = column_name.clone();
        }

        let fields = cte_output_columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.index.to_string(),
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        let cte_schema = DataSchemaRefExt::create(fields);

        let mut new_bind_context = bind_context.clone();
        for column in cte_output_columns {
            new_bind_context.add_column_binding(column);
        }

        let s_expr = SExpr::create_leaf(Arc::new(RelOperator::CTEConsumer(CTEConsumer {
            cte_name: table_name.to_string(),
            cte_schema,
            def: s_expr,
        })));
        Ok((s_expr, new_bind_context))
    }

    fn bind_cte_definition(
        &mut self,
        cte_name: &str,
        cte_map: &IndexMap<String, CteInfo>,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        let mut prev_cte_map = Box::new(IndexMap::new());
        for (name, cte_info) in cte_map.iter() {
            if name == cte_name {
                break;
            }
            prev_cte_map.insert(name.clone(), cte_info.clone());
        }
        let mut cte_bind_context = BindContext {
            cte_context: CteContext {
                cte_name: Some(cte_name.to_string()),
                cte_map: prev_cte_map,
            },
            ..Default::default()
        };
        let (s_expr, cte_bind_context) = self.bind_query(&mut cte_bind_context, query)?;
        Ok((s_expr, cte_bind_context))
    }
}
