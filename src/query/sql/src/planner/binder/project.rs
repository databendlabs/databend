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

use common_ast::ast::Identifier;
use common_ast::ast::Indirection;
use common_ast::ast::QualifiedName;
use common_ast::ast::SelectTarget;
use common_ast::parser::parse_expr;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use super::AggregateInfo;
use crate::binder::select::SelectItem;
use crate::binder::select::SelectList;
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
use crate::IndexType;
use crate::WindowChecker;

impl Binder {
    pub fn analyze_projection(
        &mut self,
        agg_info: &AggregateInfo,
        select_list: &SelectList,
    ) -> Result<(HashMap<IndexType, ScalarItem>, Vec<ColumnBinding>)> {
        let mut columns = Vec::with_capacity(select_list.items.len());
        let mut scalars = HashMap::new();
        for item in select_list.items.iter() {
            // This item is a grouping sets item, its data type should be nullable.
            let is_grouping_sets_item = agg_info.grouping_id_column.is_some()
                && agg_info.group_items_map.contains_key(&item.scalar);
            let mut column_binding = if let ScalarExpr::BoundColumnRef(ref column_ref) = item.scalar
            {
                let mut column_binding = column_ref.column.clone();
                // We should apply alias for the ColumnBinding, since it comes from table
                column_binding.column_name = item.alias.clone();
                column_binding
            } else {
                self.create_derived_column_binding(item.alias.clone(), item.scalar.data_type()?)
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
                    let grouping_checker = GroupingChecker::new(bind_context);
                    let scalar = grouping_checker.resolve(&item.scalar, None)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                } else {
                    let window_checker = WindowChecker::new(bind_context);
                    let scalar = window_checker.resolve(&item.scalar)?;
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
    #[async_backtrace::framed]
    pub async fn normalize_select_list<'a>(
        &mut self,
        input_context: &mut BindContext,
        select_list: &'a [SelectTarget],
    ) -> Result<SelectList<'a>> {
        input_context.set_expr_context(ExprContext::SelectClause);

        let mut output = SelectList::default();
        let mut prev_aliases = Vec::new();
        for select_target in select_list {
            match select_target {
                SelectTarget::QualifiedName {
                    qualified: names,
                    exclude,
                } => {
                    // Handle qualified name as select target
                    let mut exclude_cols: HashSet<String> = HashSet::new();
                    if let Some(cols) = exclude {
                        let is_unquoted_ident_case_sensitive =
                            self.name_resolution_ctx.unquoted_ident_case_sensitive;
                        for col in cols {
                            let name = is_unquoted_ident_case_sensitive
                                .then(|| col.name().to_owned())
                                .unwrap_or_else(|| col.name().to_lowercase());
                            exclude_cols.insert(name);
                        }
                        if exclude_cols.len() < cols.len() {
                            // * except (id, id)
                            return Err(ErrorCode::SemanticError("duplicate column name"));
                        }
                    }
                    let span = match names.last() {
                        Some(Indirection::Star(span)) => *span,
                        _ => None,
                    };
                    match names.len() {
                        1 | 2 => {
                            self.resolve_qualified_name_without_database_name(
                                span,
                                input_context,
                                names,
                                exclude_cols,
                                select_target,
                                &mut output,
                            )
                            .await?
                        }
                        3 => {
                            self.resolve_qualified_name_with_database_name(
                                span,
                                input_context,
                                names,
                                exclude_cols,
                                select_target,
                                &mut output,
                            )
                            .await?
                        }
                        _ => return Err(ErrorCode::SemanticError("Unsupported indirection type")),
                    };

                    if let Some(last) = output.items.last() {
                        prev_aliases.push((last.alias.clone(), last.scalar.clone()));
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let mut scalar_binder = ScalarBinder::new(
                        input_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &prev_aliases,
                        self.m_cte_bound_ctx.clone(),
                    );
                    scalar_binder.allow_pushdown();
                    let (bound_expr, _) = scalar_binder.bind(expr).await?;

                    // If alias is not specified, we will generate a name for the scalar expression.
                    let expr_name = match alias {
                        Some(alias) => normalize_identifier(alias, &self.name_resolution_ctx).name,
                        None => format!("{:#}", expr).to_lowercase(),
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

    #[async_backtrace::framed]
    async fn build_select_item<'a>(
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
                );
                let sql_tokens = tokenize_sql(virtual_computed_expr.as_str())?;
                let expr = parse_expr(&sql_tokens, Dialect::PostgreSQL)?;

                let (scalar, _) = scalar_binder.bind(&expr).await?;
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

    #[async_backtrace::framed]
    async fn resolve_qualified_name_without_database_name<'a>(
        &self,
        span: Span,
        input_context: &BindContext,
        names: &QualifiedName,
        exclude_cols: HashSet<String>,
        select_target: &'a SelectTarget,
        output: &mut SelectList<'a>,
    ) -> Result<()> {
        let mut match_table = false;
        let empty_exclude = exclude_cols.is_empty();
        let table_name = match &names[0] {
            Indirection::Star(_) => None,
            Indirection::Identifier(table_name) => Some(table_name),
        };
        let star = table_name.is_none();
        if !empty_exclude {
            precheck_exclude_cols(input_context, &exclude_cols, None, table_name)?;
        }
        for column_binding in input_context.all_column_bindings() {
            if column_binding.visibility != Visibility::Visible {
                continue;
            }
            let push_item =
                empty_exclude || exclude_cols.get(&column_binding.column_name).is_none();
            if star {
                if column_binding.column_name.starts_with('_')
                    && column_binding.database_name == Some("system".to_string())
                {
                    return Err(ErrorCode::SemanticError(
                        "can not select * from csv/ndjson file",
                    ));
                }
                // Expands wildcard star, for example we have a table `t(a INT, b INT)`:
                // The query `SELECT * FROM t` will be expanded into `SELECT t.a, t.b FROM t`
                if push_item {
                    let item = self
                        .build_select_item(
                            span,
                            input_context,
                            select_target,
                            column_binding.clone(),
                        )
                        .await?;
                    output.items.push(item);
                }
            } else if let Some(name) = &column_binding.table_name {
                if push_item
                    && compare_table_name(
                        name,
                        &table_name.unwrap().name,
                        &self.name_resolution_ctx,
                    )
                {
                    if column_binding.column_name.starts_with('_')
                        && column_binding.database_name == Some("system".to_string())
                    {
                        return Err(ErrorCode::SemanticError(
                            "can not select * from csv/ndjson file",
                        ));
                    }
                    match_table = true;
                    let item = self
                        .build_select_item(
                            span,
                            input_context,
                            select_target,
                            column_binding.clone(),
                        )
                        .await?;
                    output.items.push(item);
                }
            }
        }
        if !star && !match_table {
            return Err(ErrorCode::UnknownTable(format!(
                "Unknown table '{}'",
                table_name.unwrap().name
            ))
            .set_span(span));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn resolve_qualified_name_with_database_name<'a>(
        &self,
        span: Span,
        input_context: &BindContext,
        names: &QualifiedName,
        exclude_cols: HashSet<String>,
        select_target: &'a SelectTarget,
        output: &mut SelectList<'a>,
    ) -> Result<()> {
        let mut match_table = false;
        let empty_exclude = exclude_cols.is_empty();
        // db.table.*
        let db_name = &names[0];
        let tab_name = &names[1];

        match (db_name, tab_name) {
            (Indirection::Identifier(db_name), Indirection::Identifier(table_name)) => {
                if !empty_exclude {
                    precheck_exclude_cols(
                        input_context,
                        &exclude_cols,
                        Some(db_name),
                        Some(table_name),
                    )?;
                }
                for column_binding in input_context.all_column_bindings() {
                    if column_binding.visibility != Visibility::Visible {
                        continue;
                    }
                    let match_table_with_db =
                        match (&column_binding.database_name, &column_binding.table_name) {
                            (Some(d_name), Some(t_name)) => {
                                d_name == &db_name.name
                                    && compare_table_name(
                                        t_name,
                                        &table_name.name,
                                        &self.name_resolution_ctx,
                                    )
                            }
                            _ => false,
                        };
                    if match_table_with_db
                        && (exclude_cols.is_empty()
                            || exclude_cols.get(&column_binding.column_name).is_none())
                    {
                        match_table = true;
                        let item = self
                            .build_select_item(
                                span,
                                input_context,
                                select_target,
                                column_binding.clone(),
                            )
                            .await?;
                        output.items.push(item);
                    }
                }
                if !match_table {
                    return Err(ErrorCode::UnknownTable(format!(
                        "Unknown table '{}'.'{}'",
                        db_name.name.clone(),
                        table_name.name.clone()
                    ))
                    .set_span(span));
                }
            }
            _ => {
                return Err(ErrorCode::SemanticError("Unsupported indirection type"));
            }
        }
        Ok(())
    }
}

// Pre-check exclude_col is legal
fn precheck_exclude_cols(
    input_context: &BindContext,
    exclude_cols: &HashSet<String>,
    db_name: Option<&Identifier>,
    table_name: Option<&Identifier>,
) -> Result<()> {
    let all_columns_bind = input_context.all_column_bindings();
    let mut qualified_cols_name: HashSet<String> = HashSet::new();

    fn fill_qualified_cols(
        qualified_cols_name: &mut HashSet<String>,
        column_bind: &ColumnBinding,
    ) -> Result<()> {
        let col_name = column_bind.column_name.clone();
        if qualified_cols_name.contains(col_name.as_str()) {
            return Err(ErrorCode::SemanticError(format!(
                "ambiguous column name '{col_name}'"
            )));
        } else {
            qualified_cols_name.insert(col_name);
        }
        Ok(())
    }

    match (db_name, table_name) {
        (None, None) => {
            for column_bind in all_columns_bind {
                if column_bind.visibility != Visibility::Visible {
                    continue;
                }
                fill_qualified_cols(&mut qualified_cols_name, column_bind)?;
            }
        }
        (None, Some(table_name)) => {
            for column_bind in all_columns_bind {
                if column_bind.visibility != Visibility::Visible {
                    continue;
                }
                if column_bind.table_name == Some(table_name.name.clone()) {
                    fill_qualified_cols(&mut qualified_cols_name, column_bind)?;
                }
            }
        }
        (Some(db_name), Some(table_name)) => {
            for column_bind in all_columns_bind {
                if column_bind.visibility != Visibility::Visible {
                    continue;
                }
                if column_bind.table_name == Some(table_name.name.clone())
                    && column_bind.database_name == Some(db_name.name.clone())
                {
                    fill_qualified_cols(&mut qualified_cols_name, column_bind)?;
                }
            }
        }
        (Some(_), None) => {}
    }

    for exclude_col in exclude_cols {
        if qualified_cols_name.get(exclude_col).is_none() {
            return Err(ErrorCode::SemanticError(format!(
                "column '{exclude_col}' doesn't exist"
            )));
        }
    }
    if exclude_cols.len() == qualified_cols_name.len() {
        return Err(ErrorCode::SemanticError("SELECT with no columns"));
    }
    Ok(())
}
