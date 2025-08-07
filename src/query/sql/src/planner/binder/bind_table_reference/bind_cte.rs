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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::With;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use indexmap::IndexMap;

use crate::binder::BindContext;
use crate::binder::Binder;
use crate::binder::CteContext;
use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;
use crate::plans::Sequence;

impl Binder {
    pub fn init_cte(&mut self, bind_context: &mut BindContext, with: &Option<With>) -> Result<()> {
        let Some(with) = with else {
            return Ok(());
        };

        for cte in with.ctes.iter() {
            let cte_name = self.normalize_identifier(&cte.alias.name).name;
            if bind_context.cte_context.cte_map.contains_key(&cte_name) {
                return Err(ErrorCode::SemanticError(format!(
                    "Duplicate common table expression: {cte_name}"
                )));
            }

            let (s_expr, cte_bind_context) =
                self.bind_cte_definition(&cte_name, &bind_context.cte_context.cte_map, &cte.query)?;

            let column_name = cte
                .alias
                .columns
                .iter()
                .map(|ident| self.normalize_identifier(ident).name)
                .collect();

            let cte_info = CteInfo {
                columns_alias: column_name,
                query: *cte.query.clone(),
                recursive: with.recursive,
                materialized: cte.materialized,
                columns: vec![],
                bound_s_expr: s_expr,
                bound_context: cte_bind_context,
            };
            bind_context.cte_context.cte_map.insert(cte_name, cte_info);
        }

        Ok(())
    }
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

        let output_columns = cte_output_columns.iter().map(|c| c.index).collect();

        let mut new_bind_context = bind_context.clone();
        for column in cte_output_columns.iter() {
            new_bind_context.add_column_binding(column.clone());
        }

        let mut column_mapping = HashMap::new();
        for (index_in_ref, index_in_def) in cte_output_columns
            .iter()
            .zip(cte_info.bound_context.columns.iter())
        {
            column_mapping.insert(index_in_ref.index, index_in_def.index);
        }

        let s_expr = SExpr::create_leaf(Arc::new(RelOperator::MaterializedCTERef(
            MaterializedCTERef {
                cte_name: table_name.to_string(),
                output_columns,
                def: s_expr,
                column_mapping,
            },
        )));
        Ok((s_expr, new_bind_context))
    }

    pub fn bind_cte_definition(
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

    pub fn bind_materialized_cte(
        &mut self,
        with: &With,
        main_query_expr: SExpr,
        cte_context: CteContext,
    ) -> Result<SExpr> {
        let mut current_expr = main_query_expr;

        for cte in with.ctes.iter().rev() {
            if cte.materialized {
                let cte_name = self.normalize_identifier(&cte.alias.name).name;

                let cte_info = cte_context.cte_map.get(&cte_name).ok_or_else(|| {
                    ErrorCode::Internal(format!("CTE '{}' not found in context", cte_name))
                })?;

                let s_expr = cte_info.bound_s_expr.clone();

                let bind_context = cte_info.bound_context.clone();

                let materialized_cte =
                    MaterializedCTE::new(cte_name, Some(bind_context.columns.clone()), None);
                let materialized_cte = SExpr::create_unary(materialized_cte, s_expr);
                let sequence = Sequence {};
                current_expr = SExpr::create_binary(sequence, materialized_cte, current_expr);
            }
        }

        Ok(current_expr)
    }
}
