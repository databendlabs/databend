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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::ColumnFilter;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use itertools::Itertools;

use super::AggregateInfo;
use crate::binder::aggregate::find_replaced_aggregate_function;
use crate::binder::select::SelectItem;
use crate::binder::select::SelectList;
use crate::binder::window::find_replaced_window_function;
use crate::binder::window::WindowInfo;
use crate::binder::ExprContext;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::ColumnBinding;
use crate::planner::semantic::compare_table_name;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::GroupingChecker;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::VisitorMut as _;
use crate::IndexType;
use crate::TypeChecker;
use crate::WindowChecker;

#[derive(VisitorMut)]
#[visitor(Identifier(enter))]
struct RemoveIdentifierQuote;

impl RemoveIdentifierQuote {
    fn enter_identifier(&mut self, ident: &mut Identifier) {
        ident.quote = None
    }
}

impl Binder {
    pub fn analyze_projection(
        &mut self,
        agg_info: &AggregateInfo,
        window_info: &WindowInfo,
        select_list: &SelectList,
    ) -> Result<(HashMap<IndexType, ScalarItem>, Vec<ColumnBinding>)> {
        let mut columns = Vec::with_capacity(select_list.items.len());
        let mut scalars = HashMap::new();
        for item in select_list.items.iter() {
            // This item is a grouping sets item, its data type should be nullable.
            let is_grouping_sets_item = agg_info.grouping_sets.is_some()
                && agg_info.group_items_map.contains_key(&item.scalar);

            let mut column_binding = match &item.scalar {
                ScalarExpr::BoundColumnRef(column_ref) => {
                    let mut column_binding = column_ref.column.clone();
                    // We should apply alias for the ColumnBinding, since it comes from table
                    column_binding.column_name = item.alias.clone();
                    column_binding
                }
                ScalarExpr::AggregateFunction(agg) => {
                    // Replace to bound column to reduce duplicate derived column bindings.
                    debug_assert!(!is_grouping_sets_item);
                    find_replaced_aggregate_function(agg_info, agg, &item.alias).unwrap()
                }
                ScalarExpr::WindowFunction(win) => {
                    find_replaced_window_function(window_info, win, &item.alias).unwrap()
                }
                ScalarExpr::AsyncFunctionCall(async_func) => self.create_derived_column_binding(
                    async_func.display_name.clone(),
                    async_func.return_type.as_ref().clone(),
                    Some(item.scalar.clone()),
                ),
                _ => self.create_derived_column_binding(
                    item.alias.clone(),
                    item.scalar.data_type()?,
                    Some(item.scalar.clone()),
                ),
            };

            if is_grouping_sets_item {
                column_binding.data_type = Box::new(column_binding.data_type.wrap_nullable());
            }
            let scalar = if let ScalarExpr::SubqueryExpr(SubqueryExpr {
                span,
                typ,
                subquery,
                child_expr,
                compare_op,
                data_type,
                outer_columns,
                output_column,
                ..
            }) = item.scalar.clone()
            {
                if typ == SubqueryType::Any || typ == SubqueryType::Exists {
                    ScalarExpr::SubqueryExpr(SubqueryExpr {
                        span,
                        typ,
                        subquery,
                        child_expr,
                        compare_op,
                        output_column,
                        projection_index: Some(column_binding.index),
                        data_type,
                        outer_columns,
                        contain_agg: None,
                    })
                } else {
                    item.scalar.clone()
                }
            } else {
                item.scalar.clone()
            };
            scalars.insert(column_binding.index, ScalarItem {
                scalar,
                index: column_binding.index,
            });
            columns.push(column_binding);
        }

        Ok((scalars, columns))
    }

    pub fn bind_projection(
        &mut self,
        bind_context: &mut BindContext,
        columns: &[ColumnBinding],
        scalars: &HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        bind_context.set_expr_context(ExprContext::SelectClause);
        let mut scalars = scalars
            .iter()
            .map(|(_, item)| {
                if bind_context.in_grouping {
                    let mut scalar = item.scalar.clone();
                    let mut grouping_checker = GroupingChecker::new(bind_context);
                    grouping_checker.visit(&mut scalar)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                } else {
                    let mut scalar = item.scalar.clone();
                    let mut window_checker = WindowChecker::new(bind_context);
                    window_checker.visit(&mut scalar)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        scalars.sort_by_key(|s| s.index);
        let eval_scalar = EvalScalar { items: scalars };

        let new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(child));

        // Set output columns
        bind_context.columns = columns.to_vec();

        Ok(new_expr)
    }

    /// Normalize select list into a BindContext.
    /// There are three kinds of select target:
    ///
    ///   * Qualified name, e.g. `SELECT t.a FROM t`
    ///   * Qualified name with wildcard, e.g. `SELECT t.* FROM t, t1`
    ///   * Qualified name with exclude, e.g. `SELECT t.* EXCLUDE (c1, c2) FROM t, t1`
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
    pub fn normalize_select_list<'a>(
        &mut self,
        input_context: &mut BindContext,
        select_list: &'a [SelectTarget],
    ) -> Result<SelectList<'a>> {
        input_context.set_expr_context(ExprContext::SelectClause);

        let mut output = SelectList::default();
        let mut prev_aliases = Vec::new();

        for select_target in select_list {
            match select_target {
                SelectTarget::StarColumns {
                    qualified: names,
                    column_filter,
                } => {
                    if names.len() > 3 || names.is_empty() {
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }

                    let span = match names.last() {
                        Some(Indirection::Star(span)) => *span,
                        _ => None,
                    };

                    self.resolve_star_columns(
                        span,
                        input_context,
                        select_target,
                        names.as_slice(),
                        column_filter,
                        &mut output,
                    )?;
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let mut scalar_binder = ScalarBinder::new(
                        input_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &prev_aliases,
                        self.m_cte_bound_ctx.clone(),
                        self.ctes_map.clone(),
                    );
                    let (bound_expr, _) = scalar_binder.bind(expr)?;

                    // If alias is not specified, we will generate a name for the scalar expression.
                    let expr_name = match (expr.as_ref(), alias) {
                        (
                            Expr::ColumnRef {
                                column:
                                    ColumnRef {
                                        column: ColumnID::Name(column),
                                        ..
                                    },
                                ..
                            },
                            None,
                        ) => normalize_identifier(column, &self.name_resolution_ctx).name,
                        (_, Some(alias)) => {
                            normalize_identifier(alias, &self.name_resolution_ctx).name
                        }
                        _ => {
                            let mut expr = expr.clone();
                            let mut remove_quote_visitor = RemoveIdentifierQuote;
                            expr.drive_mut(&mut remove_quote_visitor);
                            format!("{:#}", expr).to_lowercase()
                        }
                    };

                    prev_aliases.push((expr_name.clone(), bound_expr.clone()));

                    output.items.push(SelectItem {
                        select_target,
                        scalar: bound_expr,
                        alias: expr_name,
                    });
                }
            }
        }
        Ok(output)
    }

    fn build_select_item<'a>(
        &self,
        span: Span,
        input_context: &BindContext,
        select_target: &'a SelectTarget,
        column_binding: ColumnBinding,
    ) -> Result<SelectItem<'a>> {
        let scalar = match column_binding.virtual_computed_expr {
            Some(virtual_computed_expr) => {
                let mut input_context = input_context.clone();
                let mut scalar_binder = ScalarBinder::new(
                    &mut input_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    self.m_cte_bound_ctx.clone(),
                    self.ctes_map.clone(),
                );
                let sql_tokens = tokenize_sql(virtual_computed_expr.as_str())?;
                let expr = parse_expr(&sql_tokens, self.dialect)?;

                let (scalar, _) = scalar_binder.bind(&expr)?;
                scalar
            }
            None => ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: column_binding.clone(),
            }),
        };

        Ok(SelectItem {
            select_target,
            scalar,
            alias: column_binding.column_name.clone(),
        })
    }

    fn resolve_star_columns<'a>(
        &self,
        span: Span,
        input_context: &BindContext,
        select_target: &'a SelectTarget,
        names: &[Indirection],
        column_filter: &Option<ColumnFilter>,
        output: &mut SelectList<'a>,
    ) -> Result<()> {
        let excludes = column_filter.as_ref().and_then(|c| c.get_excludes());
        let mut to_exclude_columns = HashSet::new();
        if let Some(excludes) = excludes {
            for ex in excludes.iter() {
                let exclude = normalize_identifier(ex, &self.name_resolution_ctx).name;
                if to_exclude_columns.contains(&exclude) {
                    return Err(ErrorCode::SemanticError(format!(
                        "Duplicate entry `{exclude}` in EXCLUDE list"
                    )));
                }
                to_exclude_columns.insert(exclude);
            }
        }

        let mut excluded_columns = HashSet::new();

        let lambda = column_filter.as_ref().and_then(|c| c.get_lambda());

        let mut database = None;
        let mut table = None;
        if names.len() == 2 {
            if let Indirection::Identifier(ident) = &names[0] {
                table = Some(normalize_identifier(ident, &self.name_resolution_ctx).name);
            }
        } else if names.len() == 3 {
            if let Indirection::Identifier(ident) = &names[0] {
                database = Some(normalize_identifier(ident, &self.name_resolution_ctx).name);
            }
            if let Indirection::Identifier(ident) = &names[1] {
                table = Some(normalize_identifier(ident, &self.name_resolution_ctx).name);
            }
        }

        let mut match_database = false;
        let mut match_table = false;
        let star = table.is_none();
        let mut column_ids = Vec::new();
        let mut column_names = Vec::new();

        let mut adds = 0;

        for column_binding in input_context.all_column_bindings() {
            if column_binding.visibility != Visibility::Visible {
                continue;
            }

            match (&database, &column_binding.database_name) {
                (Some(t1), Some(t2)) if t1 != t2 => {
                    continue;
                }
                (Some(_), None) => continue,
                _ => {}
            }

            match_database = true;

            match (&table, &column_binding.table_name) {
                (Some(t1), Some(t2)) if !compare_table_name(t1, t2, &self.name_resolution_ctx) => {
                    continue;
                }
                (Some(_), None) => continue,
                _ => {}
            }

            match_table = true;

            if to_exclude_columns.contains(&column_binding.column_name) {
                excluded_columns.insert(column_binding.column_name.clone());
                continue;
            }

            // TODO: yangxiufeng refactor it with InVisible
            if star
                && column_binding.column_name.starts_with("_$")
                && column_binding.database_name == Some("system".to_string())
                && column_binding.table_name == Some("stage".to_string())
            {
                return Err(ErrorCode::SemanticError(
                    "select * from file only support Parquet format",
                ));
            }

            if lambda.is_some() {
                column_ids.push(column_binding.index);
                column_names.push(column_binding.column_name.clone())
            } else {
                let item = self.build_select_item(
                    span,
                    input_context,
                    select_target,
                    column_binding.clone(),
                )?;
                output.items.push(item);
                adds += 1;
            }
        }

        for exclude in to_exclude_columns {
            if !excluded_columns.contains(&exclude) {
                return Err(ErrorCode::SemanticError(format!(
                    "Column `{exclude}` in EXCLUDE list not found in FROM clause"
                )));
            }
        }

        if let Some(database) = database {
            if !match_database {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database `{}` from bind context",
                    database,
                ))
                .set_span(span));
            }
        }

        if let Some(table) = &table {
            if !match_table {
                return Err(ErrorCode::UnknownTable(format!(
                    "Unknown table `{}` from bind context",
                    table,
                ))
                .set_span(span));
            }
        }

        // apply lambda expression
        if lambda.is_some() {
            let input_array = Expr::Array {
                span,
                exprs: column_names
                    .into_iter()
                    .map(|x| Expr::Literal {
                        span,
                        value: Literal::String(x),
                    })
                    .collect_vec(),
            };

            let expr = Expr::FunctionCall {
                span,
                func: FunctionCall {
                    name: Identifier::from_name(span, "array_apply"),
                    args: vec![input_array],
                    lambda: lambda.cloned(),
                    distinct: false,
                    params: vec![],
                    window: None,
                },
            };

            let mut temp_ctx = BindContext::new();
            let mut type_checker = TypeChecker::try_create(
                &mut temp_ctx,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                &[],
                true,
            )?;
            let (scalar, _) = *type_checker.resolve(&expr)?;
            let expr = scalar.as_expr()?;
            let (new_expr, _) =
                ConstantFolder::fold(&expr, &self.ctx.get_function_context()?, &BUILTIN_FUNCTIONS);

            match new_expr {
                databend_common_expression::Expr::Constant {
                    scalar: Scalar::Array(Column::Boolean(bitmap)),
                    ..
                } => {
                    let mut new_column_idx = Vec::new();
                    for (index, val) in bitmap.iter().enumerate() {
                        if val {
                            new_column_idx.push(column_ids[index]);
                        }
                    }

                    adds += new_column_idx.len();

                    for column_binding in input_context
                        .all_column_bindings()
                        .iter()
                        .filter(|x| new_column_idx.contains(&x.index))
                    {
                        let item = self.build_select_item(
                            span,
                            input_context,
                            select_target,
                            column_binding.clone(),
                        )?;
                        output.items.push(item);
                    }
                }
                _ => {
                    return Err(ErrorCode::SemanticError(format!(
                        "Column lambda expression must be constant folded: {:?}",
                        new_expr
                    ))
                    .set_span(span));
                }
            }
        }

        if adds == 0 {
            return Err(ErrorCode::SemanticError("SELECT with no columns"));
        }

        Ok(())
    }
}
