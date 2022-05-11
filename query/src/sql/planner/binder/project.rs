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

use std::collections::HashMap;

use common_ast::ast::Indirection;
use common_ast::ast::SelectTarget;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::planner::binder::ColumnBinding;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::ProjectItem;
use crate::sql::plans::ProjectPlan;
use crate::sql::plans::Scalar;

impl<'a> Binder {
    /// Try to build a `ProjectPlan` to satisfy `output_context`.
    /// If `output_context` can already be satisfied by `input_context`(e.g. `SELECT * FROM t`),
    /// then it won't build a `ProjectPlan`.
    pub(super) fn bind_projection(&mut self, output_context: &mut BindContext) -> Result<()> {
        let mut projections: Vec<ProjectItem> = vec![];
        for column_binding in output_context.all_column_bindings() {
            if let Some(expr) = &column_binding.scalar {
                projections.push(ProjectItem {
                    expr: *expr.clone(),
                    index: column_binding.index,
                });
            }
        }

        if !projections.is_empty() {
            let child = output_context.expression.clone().unwrap();
            let project_plan = ProjectPlan { items: projections };

            let new_expr = SExpr::create_unary(project_plan.into(), child);
            output_context.expression = Some(new_expr);
        }

        Ok(())
    }

    /// Normalize select list into a BindContext.
    /// There are three kinds of select target:
    ///
    ///   * Qualified name, e.g. `SELECT t.a FROM t`
    ///   * Qualified name with wildcard, e.g. `SELECT t.* FROM t, t1`
    ///   * Scalar expression or aggregate expression, e.g. `SELECT COUNT(*)+1 AS count FROM t`
    ///
    /// For qualified names, we just resolve it with the input `BindContext`. If successful, we
    /// will get a `ColumnBinding` and the `expr` field is left `None`.
    ///
    /// The qualified names with wildcard will be expanded into qualified names for resolution.
    /// For example, `SELECT * FROM t` may be expanded into `SELECT t.a, t.b FROM t`.
    ///
    /// For scalar expressions and aggregate expressions, we will register new columns for
    /// them in `Metadata`. And notice that, the semantic of aggregate expressions won't be checked
    /// in this function.
    pub(super) async fn normalize_select_list(
        &mut self,
        select_list: &[SelectTarget<'a>],
        has_order_by: bool,
        input_context: &mut BindContext,
    ) -> Result<BindContext> {
        let mut output_context = BindContext::new();
        if has_order_by {
            output_context.order_by_columns = Some(input_context.columns.clone());
        }
        output_context.expression = input_context.expression.clone();
        let mut origin_group_by = HashMap::new();
        for select_target in select_list {
            match select_target {
                SelectTarget::QualifiedName(names) => {
                    // Handle qualified name as select target
                    if names.len() == 1 {
                        let indirection = &names[0];
                        match indirection {
                            Indirection::Identifier(ident) => {
                                let mut column_binding =
                                    input_context.resolve_column(None, ident)?;
                                column_binding.column_name = ident.name.clone();
                                output_context.add_column_binding(column_binding);
                            }
                            Indirection::Star => {
                                // Expands wildcard star, for example we have a table `t(a INT, b INT)`:
                                // The query `SELECT * FROM t` will be expanded into `SELECT t.a, t.b FROM t`
                                for column_binding in input_context.all_column_bindings() {
                                    output_context.add_column_binding(column_binding.clone());
                                }
                            }
                        }
                    } else {
                        // TODO: Support indirection like `a.b`, `a.*`
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let scalar_binder = ScalarBinder::new(input_context, self.ctx.clone());
                    let (bound_expr, data_type) = scalar_binder.bind_expr(expr).await?;

                    // If alias is not specified, we will generate a name for the scalar expression.
                    let expr_name = match alias {
                        Some(alias) => alias.name.clone(),
                        None => self.metadata.get_expr_display_string(expr)?,
                    };

                    // TODO(leiysky): If expr is a ColumnRef, then it's a pass-through column.
                    // There is no need to generate a new ColumnEntry for it.
                    let index =
                        self.metadata
                            .add_column(expr_name.clone(), data_type.clone(), None);
                    let column_binding = ColumnBinding {
                        table_name: None,
                        column_name: expr_name,
                        index,
                        data_type,
                        scalar: Some(Box::new(bound_expr.clone())),
                    };
                    if alias.is_some() {
                        input_context.columns.push(column_binding.clone());
                        origin_group_by
                            .insert(alias.as_ref().unwrap().name.clone(), bound_expr.clone());
                    }
                    if has_order_by
                        && !matches!(bound_expr, Scalar::BoundColumnRef(BoundColumnRef { .. }))
                    {
                        output_context
                            .order_by_columns
                            .as_mut()
                            .ok_or_else(|| {
                                ErrorCode::SemanticError("Order by should have order by columns")
                            })?
                            .push(column_binding.clone());
                    }
                    output_context.add_column_binding(column_binding);
                }
            }
        }
        if !origin_group_by.is_empty() {
            input_context.origin_group_by = Some(origin_group_by);
        }
        Ok(output_context)
    }
}
