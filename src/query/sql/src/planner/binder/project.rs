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
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
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
use crate::optimizer::ir::SExpr;
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
use crate::NameResolutionContext;
use crate::TypeChecker;
use crate::WindowChecker;

#[derive(VisitorMut)]
#[visitor(Identifier(enter))]
struct RemoveIdentifierQuote;

impl RemoveIdentifierQuote {
    fn enter_identifier(&mut self, ident: &mut Identifier) {
        if !ident.is_hole() && !ident.is_variable() {
            ident.quote = None;
            ident.name = ident.name.to_lowercase();
        }
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
                    find_replaced_aggregate_function(
                        agg_info,
                        &agg.display_name,
                        &agg.return_type,
                        &item.alias,
                    )
                    .unwrap()
                }
                ScalarExpr::WindowFunction(win) => {
                    find_replaced_window_function(window_info, win, &item.alias).unwrap()
                }
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
                if typ == SubqueryType::Any {
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
        let mut columns = columns.to_vec();
        let mut scalars = scalars
            .iter()
            .map(|(_, item)| {
                if bind_context.in_grouping {
                    let mut scalar = item.scalar.clone();
                    let mut grouping_checker = GroupingChecker::new(bind_context);
                    grouping_checker.visit(&mut scalar)?;

                    if let Some(x) = columns.iter_mut().find(|x| x.index == item.index) {
                        x.data_type = Box::new(scalar.data_type()?);
                    }

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
        bind_context.columns = columns;
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
                    // Masking policy is now handled uniformly in TypeChecker::resolve()
                    let mut scalar_binder = ScalarBinder::new(
                        input_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &prev_aliases,
                    );
                    let (bound_expr, _) = scalar_binder.bind(expr)?;

                    fn get_expr_name(
                        expr: &Expr,
                        alias: &Option<Identifier>,
                        name_resolution_ctx: &NameResolutionContext,
                    ) -> String {
                        match (expr, alias) {
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
                            ) => normalize_identifier(column, name_resolution_ctx).name,
                            (
                                Expr::Cast {
                                    expr: internal_expr,
                                    ..
                                },
                                None,
                            ) => get_expr_name(internal_expr.as_ref(), &None, name_resolution_ctx),
                            (_, Some(alias)) => {
                                normalize_identifier(alias, name_resolution_ctx).name
                            }
                            _ => {
                                let mut expr = expr.clone();
                                let mut remove_quote_visitor = RemoveIdentifierQuote;
                                expr.drive_mut(&mut remove_quote_visitor);
                                format!("{:#}", expr)
                            }
                        }
                    }

                    // If alias is not specified, we will generate a name for the scalar expression.
                    let expr_name = get_expr_name(expr.as_ref(), alias, &self.name_resolution_ctx);

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
        let scalar = match column_binding.virtual_expr {
            Some(virtual_expr) => {
                let mut input_context = input_context.clone();
                input_context.allow_virtual_column = false;
                let mut scalar_binder = ScalarBinder::new(
                    &mut input_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                let sql_tokens = tokenize_sql(virtual_expr.as_str())?;
                let expr = parse_expr(&sql_tokens, self.dialect)?;

                let (scalar, _) = scalar_binder.bind(&expr)?;
                scalar
            }
            None => {
                // Check if column has masking policy
                // Use the new direct masking policy application method to avoid re-binding issues
                if LicenseManagerSwitch::instance()
                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
                    .is_ok()
                {
                    if let Some(masked_scalar) =
                        self.apply_masking_policy_directly(&column_binding, input_context)?
                    {
                        // Column has masking policy and it was successfully applied
                        masked_scalar
                    } else {
                        // No masking policy or failed to apply - return simple BoundColumnRef
                        ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span,
                            column: column_binding.clone(),
                        })
                    }
                } else {
                    // License check failed - no masking policy support
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span,
                        column: column_binding.clone(),
                    })
                }
            }
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
                    order_by: vec![],
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
                databend_common_expression::Expr::Constant(Constant {
                    scalar: Scalar::Array(Column::Boolean(bitmap)),
                    ..
                }) => {
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

    /// Apply masking policy to a column binding directly without re-binding by name
    /// This method uses the column_binding's table_index and metadata to construct
    /// masked expressions, avoiding name resolution issues in complex queries (PIVOT, Stream, etc.)
    fn apply_masking_policy_directly(
        &self,
        column_binding: &ColumnBinding,
        bind_context: &BindContext,
    ) -> Result<Option<ScalarExpr>> {
        use databend_common_users::UserApiProvider;
        use databend_enterprise_data_mask_feature::get_datamask_handler;

        // Only proceed if license check passes
        if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
            .is_err()
        {
            return Ok(None);
        }

        // Need table_index to access metadata
        let table_index = match column_binding.table_index {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // Extract masking policy information from metadata
        let policy_data = {
            let metadata = self.metadata.read();
            let table_entry = metadata.table(table_index);
            let table_ref = table_entry.table();
            let table_info_ref = table_ref.get_table_info();

            // Find the field by name to get column_id
            let field = table_info_ref
                .meta
                .schema
                .fields()
                .iter()
                .find(|f| f.name == column_binding.column_name);

            match field {
                Some(field) => table_info_ref
                    .meta
                    .column_mask_policy_columns_ids
                    .get(&field.column_id)
                    .map(|policy_info| {
                        (
                            policy_info.policy_id,
                            policy_info.columns_ids.clone(),
                            table_info_ref.meta.schema.clone(),
                            column_binding.database_name.clone(),
                            column_binding.table_name.clone(),
                        )
                    }),
                None => None,
            }
        };

        // If no masking policy, return None
        let (policy_id, using_columns, table_schema, database, table) = match policy_data {
            Some(data) => data,
            None => return Ok(None),
        };

        // Fetch the masking policy asynchronously
        let tenant = self.ctx.get_tenant();
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();

        let policy = databend_common_base::runtime::block_on(async {
            handler
                .get_data_mask_by_id(meta_api.clone(), &tenant, policy_id)
                .await
        });

        let policy = match policy {
            Ok(p) => p.data,
            Err(err) => {
                log::warn!(
                    "Failed to load masking policy (id: {}) for column '{}': {}. \
                     Column will be returned unmasked.",
                    policy_id,
                    column_binding.column_name,
                    err
                );
                return Ok(None);
            }
        };

        // Parse the policy body
        let tokens = tokenize_sql(&policy.body)?;
        let settings = self.ctx.get_settings();
        let ast_expr = parse_expr(&tokens, settings.get_sql_dialect()?)?;

        // Create parameter mapping: each parameter maps to a column reference
        // The key insight: use Expr::ColumnRef with full path (database.table.column)
        // This ensures TypeChecker can resolve the columns even in complex queries
        let args_map: HashMap<_, _> = policy
            .args
            .iter()
            .enumerate()
            .map(|(param_idx, (param_name, _))| {
                let column_id = using_columns.get(param_idx).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Masking policy metadata is corrupted: policy requires {} parameters, \
                         but only {} columns are configured in USING clause.",
                        policy.args.len(),
                        using_columns.len()
                    ))
                })?;

                let field_name = table_schema
                    .fields()
                    .iter()
                    .find(|f| f.column_id == *column_id)
                    .map(|f| f.name.clone())
                    .unwrap_or_else(|| format!("column_{}", column_id));

                let column_ref = Expr::ColumnRef {
                    span: None,
                    column: ColumnRef {
                        database: database
                            .as_ref()
                            .map(|d| Identifier::from_name(None, d.clone())),
                        table: table
                            .as_ref()
                            .map(|t| Identifier::from_name(None, t.clone())),
                        column: ColumnID::Name(Identifier::from_name(None, field_name)),
                    },
                };

                Ok((param_name.as_str(), column_ref))
            })
            .collect::<Result<_>>()?;

        // Replace parameters in the masking policy expression
        let replaced_expr = TypeChecker::clone_expr_with_replacement(&ast_expr, |nest_expr| {
            if let Expr::ColumnRef { column, .. } = nest_expr {
                // Parameter names are already normalized to lowercase at policy creation
                if let Some(arg) = args_map.get(column.column.name().to_lowercase().as_str()) {
                    return Ok(Some(arg.clone()));
                }
            }
            Ok(None)
        })?;

        // Now resolve the replaced expression using TypeChecker
        // IMPORTANT: Use the provided bind_context which has all the necessary column information
        let mut bind_ctx = bind_context.clone();
        let mut type_checker = TypeChecker::try_create(
            &mut bind_ctx,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false, // forbid_udf
        )?;

        let (scalar, _data_type) = *type_checker.resolve(&replaced_expr)?;
        Ok(Some(scalar))
    }
}
