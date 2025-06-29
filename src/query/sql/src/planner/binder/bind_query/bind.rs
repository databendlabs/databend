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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::With;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::CteContext;
use crate::plans::BoundColumnRef;
use crate::plans::MaterializedCTE;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::NameResolutionContext;

#[derive(Debug, Default, Visitor)]
#[visitor(TableReference(enter))]
struct CTERefCounter {
    cte_ref_count: HashMap<String, usize>,
    name_resolution_ctx: NameResolutionContext,
}

impl CTERefCounter {
    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        if let TableReference::Table { table, .. } = table_ref {
            let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
            if let Some(count) = self.cte_ref_count.get_mut(&table_name) {
                *count += 1;
            }
        }
    }
}

impl Binder {
    pub(crate) fn bind_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        let mut with = query.with.clone();
        if self.ctx.get_settings().get_enable_auto_materialize_cte()? {
            if let Some(with) = &mut with {
                if !with.recursive {
                    self.auto_materialize_cte(with, query)?;
                }
            }
        }
        // Initialize cte map.
        self.init_cte(bind_context, &with)?;

        // Extract limit and offset from query.
        let (limit, offset) = self.extract_limit_and_offset(query)?;

        // Bind query body.
        let (mut s_expr, mut bind_context) =
            self.bind_set_expr(bind_context, &query.body, &query.order_by, limit, None)?;

        // Bind order by for `SetOperation` and `Values`.
        s_expr = self.bind_query_order_by(&mut bind_context, query, s_expr)?;

        // Bind limit.
        s_expr = self.bind_query_limit(query, s_expr, limit, offset);

        if let Some(with) = &with {
            s_expr = self.bind_materialized_cte(with, s_expr, bind_context.cte_context.clone())?;
        }

        Ok((s_expr, bind_context))
    }

    fn auto_materialize_cte(&mut self, with: &mut With, query: &Query) -> Result<()> {
        // Initialize the count of each CTE to 0
        let mut cte_ref_count: HashMap<String, usize> = HashMap::new();
        for cte in with.ctes.iter() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            cte_ref_count.insert(table_name, 0);
        }

        // Count the number of times each CTE is referenced in the query
        let mut visitor = CTERefCounter {
            cte_ref_count,
            name_resolution_ctx: self.name_resolution_ctx.clone(),
        };
        query.drive(&mut visitor);
        cte_ref_count = visitor.cte_ref_count;

        // Update materialization based on reference count
        for cte in with.ctes.iter_mut() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            if let Some(count) = cte_ref_count.get(&table_name) {
                log::info!("[CTE]cte_ref_count: {table_name} {count}");
                // Materialize if referenced more than once
                cte.materialized |= *count > 1;
            }
        }

        Ok(())
    }

    // Initialize cte map.
    pub(crate) fn init_cte(
        &mut self,
        bind_context: &mut BindContext,
        with: &Option<With>,
    ) -> Result<()> {
        let with = if let Some(with) = with {
            with
        } else {
            return Ok(());
        };

        for cte in with.ctes.iter() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            if bind_context.cte_context.cte_map.contains_key(&table_name) {
                return Err(ErrorCode::SemanticError(format!(
                    "Duplicate common table expression: {table_name}"
                )));
            }
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
                columns: vec![],
                materialized: cte.materialized,
            };
            bind_context
                .cte_context
                .cte_map
                .insert(table_name, cte_info);
        }

        Ok(())
    }

    pub(crate) fn bind_query_order_by(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
        child: SExpr,
    ) -> Result<SExpr> {
        if !matches!(
            query.body,
            SetExpr::SetOperation(_) | SetExpr::Values { .. }
        ) || query.order_by.is_empty()
        {
            return Ok(child);
        }

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let mut order_by_items = Vec::with_capacity(query.order_by.len());

        let default_nulls_first = self.ctx.get_settings().get_nulls_first();

        for order in query.order_by.iter() {
            match order.expr {
                Expr::ColumnRef { .. } => {
                    let scalar = scalar_binder.bind(&order.expr)?.0;
                    match scalar {
                        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) => {
                            let asc = order.asc.unwrap_or(true);
                            let order_by_item = SortItem {
                                index: column.index,
                                asc,
                                nulls_first: order
                                    .nulls_first
                                    .unwrap_or_else(|| default_nulls_first(asc)),
                            };
                            order_by_items.push(order_by_item);
                        }
                        _ => {
                            return Err(ErrorCode::Internal("scalar should be BoundColumnRef")
                                .set_span(order.expr.span()));
                        }
                    }
                }
                _ => {
                    return Err(
                        ErrorCode::SemanticError("can only order by column".to_string())
                            .set_span(order.expr.span()),
                    );
                }
            }
        }
        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
            after_exchange: None,
            pre_projection: None,
            window_partition: None,
        };
        Ok(SExpr::create_unary(
            Arc::new(sort_plan.into()),
            Arc::new(child),
        ))
    }

    fn bind_materialized_cte(
        &mut self,
        with: &With,
        main_query_expr: SExpr,
        mut cte_context: CteContext,
    ) -> Result<SExpr> {
        let mut current_expr = main_query_expr;
        cte_context.is_binding_materialized_cte = true;

        for cte in with.ctes.iter().rev() {
            if cte.materialized {
                let cte_name = self.normalize_identifier(&cte.alias.name).name;

                // Create a new bind context for the CTE definition
                let mut cte_bind_context = BindContext {
                    cte_context: cte_context.clone(),
                    ..Default::default()
                };

                // Bind the CTE definition
                let (cte_definition_expr, _) =
                    self.bind_query(&mut cte_bind_context, &cte.query)?;

                // Create the MaterializedCTE operator
                let materialized_cte = MaterializedCTE::new(cte_name);
                current_expr = SExpr::create_binary(
                    Arc::new(materialized_cte.into()),
                    Arc::new(cte_definition_expr),
                    Arc::new(current_expr),
                );
            }
        }

        Ok(current_expr)
    }
}
