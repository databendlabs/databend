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

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SubqueryModifier;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnIndex;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionKind;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_SEARCH_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::StageInfo;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use simsearch::SimSearch;
use smallvec::SmallVec;
use unicase::Ascii;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use super::resolve_type_name;
use crate::BindContext;
use crate::MetadataRef;
use crate::binder::NameResolutionResult;
use crate::binder::bind_values;
use crate::optimizer::ir::RelExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;

const DEFAULT_DECIMAL_PRECISION: i64 = 38;
const DEFAULT_DECIMAL_SCALE: i64 = 0;

mod aggregate;
mod async_functions;
mod core_expr;
mod date;
mod lambda;
mod like;
mod literal;
mod search;
mod set_returning;
mod string;
mod subquery;
mod sugar;
mod udf;
mod variant;
mod vector;
mod window;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StageLocationParam {
    pub param_name: String,
    pub relative_path: String,
    pub stage_info: StageInfo,
}

/// A helper for type checking.
///
/// `TypeChecker::resolve` first lowers an AST `Expr` into a core expression tree,
/// then resolves the lowered tree into a typed expression `Scalar`. At the same
/// time, name resolution will be performed, which check validity of unbound
/// `ColumnRef` and try to replace it with qualified `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a mut BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    func_ctx: FunctionContext,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,

    aliases: &'a [(String, ScalarExpr)],

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,

    // true if current expr is inside a window function.
    // This is used to allow aggregation function in window's aggregate function.
    in_window_function: bool,
    forbid_udf: bool,

    // true if currently resolving a masking policy expression.
    // This prevents infinite recursion when a masking policy references the masked column itself.
    in_masking_policy: bool,

    // Skip sequence existence checks when resolving `nextval`.
    skip_sequence_check: bool,
}

impl<'a> TypeChecker<'a> {
    pub fn try_create(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
        forbid_udf: bool,
    ) -> Result<Self> {
        let func_ctx = ctx.get_function_context()?;
        let dialect = ctx.get_settings().get_sql_dialect()?;
        Ok(Self {
            bind_context,
            ctx,
            dialect,
            func_ctx,
            name_resolution_ctx,
            metadata,
            aliases,
            in_aggregate_function: false,
            in_window_function: false,
            forbid_udf,
            in_masking_policy: false,
            skip_sequence_check: false,
        })
    }

    pub fn set_skip_sequence_check(&mut self, skip: bool) {
        self.skip_sequence_check = skip;
    }

    fn core_expr_arena(&self) -> core_expr::CoreExprArena<'a> {
        core_expr::CoreExprArena::new(self.func_ctx.week_start as u64)
    }

    #[recursive::recursive]
    pub fn resolve(&mut self, expr: &Expr) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut arena = self.core_expr_arena();
        let root = arena.lower_ast_expr(expr)?;
        self.resolve_core(&arena, root)
    }

    pub(super) fn resolve_legacy_ast(
        &mut self,
        expr: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match expr {
            Expr::ColumnRef {
                span,
                column:
                    ColumnRef {
                        database,
                        table,
                        column: ident,
                    },
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let table = table
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let result = match ident {
                    ColumnID::Name(ident) => {
                        let column = normalize_identifier(ident, self.name_resolution_ctx);
                        self.bind_context.resolve_name(
                            database.as_deref(),
                            table.as_deref(),
                            &column,
                            self.aliases,
                            self.name_resolution_ctx,
                        )?
                    }
                    ColumnID::Position(pos) => self.bind_context.search_column_position(
                        pos.span,
                        database.as_deref(),
                        table.as_deref(),
                        pos.pos,
                    )?,
                };

                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        if let Some(virtual_expr) = column.virtual_expr {
                            let sql_tokens = tokenize_sql(virtual_expr.as_str())?;
                            let expr = parse_expr(&sql_tokens, self.dialect)?;
                            return self.resolve(&expr);
                        } else {
                            // Fast path: Check if table has any masking policies at all before doing expensive async work
                            // BUT: skip masking policy application if we're already resolving a masking policy expression
                            // to prevent infinite recursion (e.g., policy references the masked column itself)
                            let has_masking_policy = !self.in_masking_policy
                                // First check: is DataMask feature enabled? (cheapest check)
                                && LicenseManagerSwitch::instance()
                                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
                                    .is_ok()
                                // Second check: does this column reference a table with masking policy?
                                && column
                                    .table_index
                                    .and_then(|table_index| {
                                        // IMPORTANT: Extract all needed data before releasing the lock
                                        // to avoid holding the lock during fallback resolution
                                        let (table_entry_opt, db_name, tbl_name) = {
                                            let metadata = self.metadata.read();
                                            let entry = metadata.tables().get(table_index);
                                            (
                                                entry.is_some(),
                                                column.database_name.clone(),
                                                column.table_name.clone(),
                                            )
                                        }; // metadata lock is released here

                                        // Now handle the fallback case without holding the lock
                                        let final_table_index = if table_entry_opt {
                                            Some(table_index)
                                        } else {
                                            // table_index invalid - try fallback by name
                                            // This can happen in complex queries (e.g., REPLACE INTO with source columns)
                                            // where metadata context differs between binding phases
                                            if let (Some(db), Some(tbl)) = (db_name.as_ref(), tbl_name.as_ref()) {
                                                // Re-acquire lock for lookup
                                                let metadata = self.metadata.read();
                                                metadata.get_table_index(Some(db), tbl)
                                            } else {
                                                None
                                            }
                                        };

                                        // Re-acquire lock to get table info
                                        final_table_index.and_then(|idx| {
                                            let metadata = self.metadata.read();
                                            let table_entry = metadata.tables().get(idx)?;
                                            let table_ref = table_entry.table();
                                            let table_info = table_ref.get_table_info();
                                            let table_schema = table_ref.schema();

                                            if table_info.meta.column_mask_policy_columns_ids.is_empty()
                                            {
                                                return None;
                                            }
                                            table_schema
                                                .fields()
                                                .iter()
                                                .find(|f| f.name == column.column_name)
                                                .and_then(|field| {
                                                    table_info
                                                        .meta
                                                        .column_mask_policy_columns_ids
                                                        .contains_key(&field.column_id)
                                                        .then_some(())
                                                })
                                        })
                                    })
                                    .is_some();

                            if has_masking_policy {
                                // Only do expensive async work if we know there's a policy
                                let mask_expr = databend_common_base::runtime::block_on(async {
                                    self.get_masking_policy_expr_for_column(
                                        &column,
                                        database.as_deref(),
                                        table.as_deref(),
                                    )
                                    .await
                                })?;

                                if let Some(mask_expr) = mask_expr {
                                    // Set flag to prevent recursive masking policy application
                                    let old_in_masking_policy = self.in_masking_policy;
                                    self.in_masking_policy = true;

                                    // Recursively resolve the masking policy expression
                                    let result = self.resolve(&mask_expr);

                                    // Restore flag
                                    self.in_masking_policy = old_in_masking_policy;

                                    return result;
                                }
                            }

                            let data_type = *column.data_type.clone();
                            (
                                BoundColumnRef {
                                    span: *span,
                                    column,
                                }
                                .into(),
                                data_type,
                            )
                        }
                    }
                    NameResolutionResult::InternalColumn(column) => {
                        // add internal column binding into `BindContext`
                        let column = self.bind_context.add_internal_column_binding(
                            &column,
                            self.metadata.clone(),
                            None,
                            true,
                        )?;
                        let data_type = *column.data_type.clone();
                        (
                            BoundColumnRef {
                                span: *span,
                                column,
                            }
                            .into(),
                            data_type,
                        )
                    }
                    NameResolutionResult::Alias { scalar, .. } => {
                        (scalar.clone(), scalar.data_type()?)
                    }
                };

                Ok(Box::new((scalar, data_type)))
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                if list.len() >= self.ctx.get_settings().get_inlist_to_join_threshold()? {
                    if *not {
                        return self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::InList {
                            span: *span,
                            expr: expr.clone(),
                            list: list.clone(),
                            not: false,
                        });
                    }
                    return self.convert_inlist_to_subquery(expr, list);
                }

                let get_max_inlist_to_or = self.ctx.get_settings().get_max_inlist_to_or()? as usize;
                if list.len() > get_max_inlist_to_or && list.iter().all(like::satisfy_contain_func)
                {
                    let array_expr = Expr::Array {
                        span: *span,
                        exprs: list.clone(),
                    };
                    // Deduplicate the array.
                    let array_expr = Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(*span, "array_distinct"),
                            args: vec![array_expr],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                            distinct: false,
                        },
                    };
                    let args = vec![&array_expr, expr.as_ref()];
                    if *not {
                        self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::FunctionCall {
                            span: *span,
                            func: ASTFunctionCall {
                                distinct: false,
                                name: Identifier::from_name(*span, "contains"),
                                args: args.iter().copied().cloned().collect(),
                                params: vec![],
                                order_by: vec![],
                                window: None,
                                lambda: None,
                            },
                        })
                    } else {
                        self.resolve_function(*span, "contains", vec![], &args)
                    }
                } else {
                    let mut predicate_levels =
                        Vec::with_capacity(list.len().max(1).ilog2() as usize + 1);

                    for item in list {
                        let (predicate, _) = *self.resolve_binary_op_or_subquery(
                            span,
                            &BinaryOperator::Eq,
                            expr.as_ref(),
                            item,
                        )?;
                        self.merge_or_level(*span, &mut predicate_levels, predicate)?;
                    }

                    let result = self
                        .fold_or_levels(*span, predicate_levels)?
                        .expect("IN list should not be empty");
                    let data_type = result.data_type()?;

                    if *not {
                        self.resolve_scalar_function_call(*span, "not", vec![], vec![result])
                    } else {
                        Ok(Box::new((result, data_type)))
                    }
                }
            }

            Expr::BinaryOp {
                span,
                op,
                left,
                right,
                ..
            } => self.resolve_binary_op_or_subquery(span, op, left, right),

            Expr::UnaryOp { span, op, expr, .. } => self.resolve_unary_op(*span, op, expr.as_ref()),

            Expr::FunctionCall {
                span,
                func:
                    ASTFunctionCall {
                        distinct: _,
                        name,
                        args,
                        params,
                        order_by,
                        window,
                        lambda,
                    },
            } => {
                let normalized_func_name;
                let func_name = if name.name.chars().any(char::is_uppercase) {
                    normalized_func_name = name.name.to_lowercase();
                    normalized_func_name.as_str()
                } else {
                    name.name.as_str()
                };
                let uni_case_func_name = Ascii::new(func_name);
                if !is_builtin_function(func_name)
                    && !Self::all_sugar_functions().contains(&uni_case_func_name)
                {
                    let udf_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
                    if let Some(udf) = self.resolve_udf(*span, &udf_name, args)? {
                        return Ok(udf);
                    }

                    // Function not found, try to find and suggest similar function name.
                    let all_funcs = BUILTIN_FUNCTIONS
                        .all_function_names()
                        .into_iter()
                        .chain(AggregateFunctionFactory::instance().registered_names())
                        .chain(
                            GENERAL_WINDOW_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            GENERAL_LAMBDA_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            GENERAL_SEARCH_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            ASYNC_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            Self::all_sugar_functions()
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        );
                    let mut engine: SimSearch<String> = SimSearch::new();
                    for func_name in all_funcs {
                        engine.insert(func_name.clone(), &func_name);
                    }
                    let possible_funcs = engine
                        .search(func_name)
                        .iter()
                        .map(|name| format!("'{name}'"))
                        .collect::<Vec<_>>();
                    if possible_funcs.is_empty() {
                        return Err(ErrorCode::UnknownFunction(format!(
                            "no function matches the given name: {func_name}"
                        ))
                        .set_span(*span));
                    } else {
                        return Err(ErrorCode::UnknownFunction(format!(
                            "no function matches the given name: '{func_name}', do you mean {}?",
                            possible_funcs.join(", ")
                        ))
                        .set_span(*span));
                    }
                }

                // check within group legal
                if !order_by.is_empty()
                    && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&uni_case_func_name)
                {
                    return Err(ErrorCode::SemanticError(
                        "only aggregate functions allowed in within group syntax",
                    )
                    .set_span(*span));
                }
                // check window function legal
                if window.is_some()
                    && !AggregateFunctionFactory::instance().contains(func_name)
                    && !GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name)
                {
                    return Err(ErrorCode::SemanticError(
                        "only window and aggregate functions allowed in window syntax",
                    )
                    .set_span(*span));
                }
                // check lambda function legal
                if lambda.is_some() && !GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
                    return Err(ErrorCode::SemanticError(
                        "only lambda functions allowed in lambda syntax",
                    )
                    .set_span(*span));
                }

                let args = args.iter().collect::<SmallVec<[&Expr; 4]>>();

                if GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
                    if lambda.is_none() {
                        return Err(ErrorCode::SemanticError(format!(
                            "function {func_name} must have a lambda expression",
                        ))
                        .set_span(*span));
                    }
                    let lambda = lambda.as_ref().unwrap();
                    self.resolve_lambda_function(*span, func_name, &args, lambda)
                } else if GENERAL_SEARCH_FUNCTIONS.contains(&uni_case_func_name) {
                    match func_name.to_lowercase().as_str() {
                        "score" => self.resolve_score_search_function(*span, func_name, &args),
                        "match" => self.resolve_match_search_function(*span, func_name, &args),
                        "query" => self.resolve_query_search_function(*span, func_name, &args),
                        _ => Err(ErrorCode::SemanticError(format!(
                            "cannot find search function {}",
                            func_name
                        ))
                        .set_span(*span)),
                    }
                } else if ASYNC_FUNCTIONS.contains(&uni_case_func_name) {
                    self.resolve_async_function(*span, func_name, &args)
                } else if BUILTIN_FUNCTIONS
                    .get_property(func_name)
                    .map(|property| property.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    // Set returning function
                    self.resolve_set_returning_function(*span, func_name, &args)
                } else {
                    // Scalar function
                    let mut new_params: Vec<Scalar> = Vec::with_capacity(params.len());
                    for param in params {
                        let box (scalar, _) = self.resolve(param)?;
                        let expr = scalar.as_expr()?;
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let constant = expr
                            .into_constant()
                            .map_err(|_| {
                                ErrorCode::SemanticError(format!(
                                    "invalid parameter {param} for scalar function, expected constant",
                                ))
                                .set_span(*span)
                            })?
                            .scalar;
                        new_params.push(constant);
                    }
                    self.resolve_function(*span, func_name, new_params, &args)
                }
            }

            Expr::Exists { subquery, not, .. } => self.resolve_subquery(
                if !*not {
                    SubqueryType::Exists
                } else {
                    SubqueryType::NotExists
                },
                subquery,
                None,
                None,
            ),

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, None, None)
            }

            Expr::InSubquery {
                subquery,
                not,
                expr,
                span,
            } => {
                // Not in subquery will be transformed to not(Expr = Any(...))
                if *not {
                    return self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::InSubquery {
                        subquery: subquery.clone(),
                        not: false,
                        expr: expr.clone(),
                        span: *span,
                    });
                }
                // InSubquery will be transformed to Expr = Any(...)
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    Some(*expr.clone()),
                    Some(SubqueryComparisonOp::Equal),
                )
            }

            Expr::LikeSubquery {
                subquery,
                expr,
                span,
                modifier,
                escape,
            } => self.resolve_scalar_subquery(
                subquery,
                expr,
                span,
                span,
                modifier,
                &BinaryOperator::Like(escape.clone()),
            ),

            Expr::LikeAnyWithEscape {
                span,
                left,
                right,
                escape,
            } => self.resolve_binary_op_or_subquery(
                span,
                &BinaryOperator::LikeAny(Some(escape.clone())),
                left,
                right,
            ),

            Expr::LikeWithEscape {
                span,
                left,
                right,
                is_not,
                escape,
            } => {
                let like_op = if *is_not {
                    BinaryOperator::NotLike(Some(escape.clone()))
                } else {
                    BinaryOperator::Like(Some(escape.clone()))
                };

                self.resolve_binary_op_or_subquery(span, &like_op, left, right)
            }

            Expr::Extract {
                span, kind, expr, ..
            } => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_extract_expr(*span, kind, expr)?;
                self.resolve_core(&arena, root)
            }

            Expr::DatePart {
                span, kind, expr, ..
            } => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_extract_expr(*span, kind, expr)?;
                self.resolve_core(&arena, root)
            }

            Expr::DateTrunc {
                span, unit, date, ..
            } => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_date_trunc_expr(
                    *span,
                    unit,
                    date,
                    self.func_ctx.week_start as u64,
                )?;
                self.resolve_core(&arena, root)
            }

            Expr::TimeSlice {
                span,
                unit,
                date,
                slice_length,
                start_or_end,
            } => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_time_slice_expr(
                    *span,
                    date,
                    *slice_length,
                    unit,
                    start_or_end.to_string(),
                )?;
                self.resolve_core(&arena, root)
            }

            Expr::LastDay {
                span, unit, date, ..
            } => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_last_day_expr(*span, date, unit)?;
                self.resolve_core(&arena, root)
            }

            Expr::Hole { span, .. } | Expr::Placeholder { span } => Err(ErrorCode::SemanticError(
                "Hole or Placeholder expression is impossible in trivial query".to_string(),
            )
            .set_span(*span)),
            Expr::StageLocation { span, location } => self.resolve_stage_location(*span, location),
            _ => Err(ErrorCode::Internal(
                "expression should have been lowered into CoreExpr",
            )),
        }
    }

    fn resolve_binary_op_or_subquery(
        &mut self,
        span: &Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if let Expr::Subquery {
            subquery,
            modifier: Some(subquery_modifier),
            ..
        } = right
        {
            self.resolve_scalar_subquery(subquery, left, span, &right.span(), subquery_modifier, op)
        } else {
            self.resolve_binary_op(*span, op, left, right)
        }
    }

    fn merge_or_level(
        &mut self,
        span: Span,
        predicate_levels: &mut Vec<Option<ScalarExpr>>,
        mut predicate: ScalarExpr,
    ) -> Result<()> {
        let mut level = 0;

        loop {
            if predicate_levels.len() == level {
                predicate_levels.push(Some(predicate));
                return Ok(());
            }

            if let Some(left) = predicate_levels[level].take() {
                let (or_predicate, _) =
                    *self
                        .resolve_scalar_function_call(span, "or", vec![], vec![left, predicate])?;
                predicate = or_predicate;
                level += 1;
            } else {
                predicate_levels[level] = Some(predicate);
                return Ok(());
            }
        }
    }

    fn fold_or_levels(
        &mut self,
        span: Span,
        predicate_levels: Vec<Option<ScalarExpr>>,
    ) -> Result<Option<ScalarExpr>> {
        let mut result = None;

        for predicate in predicate_levels.into_iter().rev().flatten() {
            result = Some(match result {
                None => predicate,
                Some(acc) => {
                    let (or_predicate, _) =
                        *self.resolve_scalar_function_call(span, "or", vec![], vec![
                            acc, predicate,
                        ])?;
                    or_predicate
                }
            });
        }

        Ok(result)
    }

    fn resolve_scalar_subquery(
        &mut self,
        subquery: &Query,
        expr: &Expr,
        span: &Span,
        right_span: &Span,
        modifier: &SubqueryModifier,
        op: &BinaryOperator,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        Ok(match modifier {
            SubqueryModifier::Any | SubqueryModifier::Some => {
                let comparison_op = SubqueryComparisonOp::try_from(op)?;
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    Some(expr.clone()),
                    Some(comparison_op),
                )?
            }
            SubqueryModifier::All => {
                let contrary_op = op.to_contrary()?;
                let rewritten_subquery = Expr::Subquery {
                    span: *right_span,
                    modifier: Some(SubqueryModifier::Any),
                    subquery: Box::new(subquery.clone()),
                };
                self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::BinaryOp {
                    span: *span,
                    op: contrary_op,
                    left: Box::new(expr.clone()),
                    right: Box::new(rewritten_subquery),
                })?
            }
        })
    }

    // TODO: remove this function
    fn rewrite_substring(args: &mut [ScalarExpr]) {
        if let ScalarExpr::ConstantExpr(expr) = &args[1] {
            if let Scalar::Number(NumberScalar::UInt8(0)) = expr.value {
                args[1] = ConstantExpr {
                    span: expr.span,
                    value: Scalar::Number(1i64.into()),
                }
                .into();
            }
        }
    }

    pub(super) fn can_lower_core_scalar_function(func_name: &str) -> bool {
        if Self::all_sugar_functions().contains(&Ascii::new(func_name)) {
            return false;
        }
        BUILTIN_FUNCTIONS
            .get_property(func_name)
            .map(|property| property.kind != FunctionKind::SRF)
            .unwrap_or(false)
    }

    pub(super) fn resolve_cast_expr(
        &mut self,
        span: Span,
        scalar: ScalarExpr,
        data_type: DataType,
        target_type: &TypeName,
        is_try: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if target_type == &TypeName::Variant {
            if let Some(result) = self.resolve_cast_to_variant(span, &data_type, &scalar, is_try) {
                return result;
            }
        }

        let raw_expr = RawExpr::Cast {
            span,
            is_try,
            expr: Box::new(scalar.as_raw_expr()),
            dest_type: DataType::from(&resolve_type_name(target_type, true)?),
        };
        let checked_expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;

        if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
            return Ok(constant);
        }

        // cast variant to other type should nest wrap nullable,
        // as we cast JSON null to SQL NULL.
        let target_type = if data_type.remove_nullable() == DataType::Variant {
            checked_expr.data_type().nest_wrap_nullable()
        // if the source type is nullable, cast target type should also be nullable.
        } else if !is_try && data_type.is_nullable_or_null() {
            checked_expr.data_type().wrap_nullable()
        } else {
            checked_expr.data_type().clone()
        };

        Ok(Box::new((
            CastExpr {
                span,
                is_try,
                argument: Box::new(scalar),
                target_type: Box::new(target_type.clone()),
            }
            .into(),
            target_type,
        )))
    }

    pub(super) fn rewrite_variant_compare_constant(
        &self,
        scalar: ScalarExpr,
        data_type: DataType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let ScalarExpr::FunctionCall(ref func) = scalar else {
            return Ok(Box::new((scalar, data_type)));
        };
        if func.arguments.len() != 2 {
            return Ok(Box::new((scalar, data_type)));
        }
        let arg0 = &func.arguments[0];
        let arg1 = &func.arguments[1];
        let (constant_arg_index, constant_arg) = match (arg0, arg1) {
            (ScalarExpr::ConstantExpr(_), _)
                if arg1.data_type()?.remove_nullable() == DataType::Variant
                    && !arg1.used_columns().is_empty()
                    && arg0.data_type()? == DataType::String =>
            {
                (0, arg0)
            }
            (_, ScalarExpr::ConstantExpr(_))
                if arg0.data_type()?.remove_nullable() == DataType::Variant
                    && !arg0.used_columns().is_empty()
                    && arg1.data_type()? == DataType::String =>
            {
                (1, arg1)
            }
            _ => {
                return Ok(Box::new((scalar, data_type)));
            }
        };

        let wrap_new_arg = ScalarExpr::FunctionCall(FunctionCall {
            span: func.span,
            func_name: "to_variant".to_string(),
            params: vec![],
            arguments: vec![constant_arg.clone()],
        });
        let mut new_arguments = func.arguments.clone();
        new_arguments[constant_arg_index] = wrap_new_arg;

        let new_func = ScalarExpr::FunctionCall(FunctionCall {
            span: func.span,
            func_name: func.func_name.clone(),
            params: func.params.clone(),
            arguments: new_arguments,
        });

        Ok(Box::new((new_func, data_type)))
    }

    /// Resolve function call.
    pub fn resolve_function(
        &mut self,
        span: Span,
        func_name: &str,
        params: Vec<Scalar>,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Check if current function is a virtual function, e.g. `database`, `version`
        if Self::all_sugar_functions().contains(&Ascii::new(func_name)) {
            if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
                self.try_rewrite_sugar_function(span, func_name, arguments),
            ) {
                return rewritten_func_result;
            }
        }

        let mut args = Vec::with_capacity(arguments.len());

        for argument in arguments {
            let box (arg, _) = self.resolve(argument)?;
            args.push(arg);
        }

        if self.should_try_rewrite_variant_function(func_name) {
            let mut arg_types = Vec::with_capacity(args.len());
            for arg in &args {
                let mut arg_type = arg.data_type()?;
                if let ScalarExpr::SubqueryExpr(subquery) = arg
                    && subquery.typ == SubqueryType::Scalar
                    && !arg_type.is_nullable()
                {
                    arg_type = arg_type.wrap_nullable();
                }
                arg_types.push(arg_type);
            }
            if let Some(rewritten_variant_expr) =
                self.try_rewrite_variant_function(span, func_name, &args, &arg_types)
            {
                return rewritten_variant_expr;
            }
        }
        if Self::is_vector_function(func_name) {
            if let Some(rewritten_vector_expr) =
                self.try_rewrite_vector_function(span, func_name, &args)
            {
                return rewritten_vector_expr;
            }
        }

        self.resolve_scalar_function_call(span, func_name, params, args)
    }

    pub fn resolve_scalar_function_call(
        &self,
        span: Span,
        func_name: &str,
        mut params: Vec<Scalar>,
        mut args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // rewrite substr('xx', 0, xx) -> substr('xx', 1, xx)
        if (func_name == "substr" || func_name == "substring")
            && self
                .ctx
                .get_settings()
                .get_sql_dialect()
                .unwrap()
                .substr_index_zero_literal_as_one()
        {
            Self::rewrite_substring(&mut args);
        }

        self.adjust_date_interval_function_args(func_name, &mut args)?;

        // Type check
        let mut arguments = args.iter().map(|v| v.as_raw_expr()).collect::<Vec<_>>();
        // inject the params
        if ["round", "truncate"].contains(&func_name)
            && !args.is_empty()
            && params.is_empty()
            && args[0].data_type()?.remove_nullable().is_decimal()
        {
            let scale = if args.len() == 2 {
                let scalar_expr = &arguments[1];
                let expr = type_check::check(scalar_expr, &BUILTIN_FUNCTIONS)?;

                let scale: i64 = check_number(
                    expr.span(),
                    &FunctionContext::default(),
                    &expr,
                    &BUILTIN_FUNCTIONS,
                )?;
                scale.clamp(-76, 76)
            } else {
                0
            };
            params.push(Scalar::Number(NumberScalar::Int64(scale)));
        } else if func_name.eq_ignore_ascii_case("as_decimal") {
            // Convert the precision and scale argument of `as_decimal` to params
            if !params.is_empty() {
                if params.len() > 2 || arguments.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                        params.len(),
                        arguments.len()
                    )));
                }
            } else {
                if arguments.is_empty() || arguments.len() > 3 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}` require 1, 2 or 3 arguments, but got {} arguments",
                        arguments.len()
                    )));
                }
                let param_args = arguments.split_off(1);
                for arg in param_args.into_iter() {
                    let expr = type_check::check(&arg, &BUILTIN_FUNCTIONS)?;
                    let param: u8 = check_number(
                        expr.span(),
                        &FunctionContext::default(),
                        &expr,
                        &BUILTIN_FUNCTIONS,
                    )?;
                    params.push(Scalar::Number(NumberScalar::UInt8(param)));
                }
            }
            if !params.is_empty() {
                let Some(precision) = params[0].get_i64() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{}` for `{func_name}` precision parameter",
                        params[0]
                    )));
                };
                if precision < 0 || precision > i256::MAX_PRECISION as i64 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{precision}` for `{func_name}` precision parameter"
                    )));
                }
                if params.len() == 2 {
                    let Some(scale) = params[1].get_i64() else {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{}` for `{func_name}` scale parameter",
                            params[1]
                        )));
                    };
                    if scale < 0 || scale > precision {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{scale}` for `{func_name}` scale parameter"
                        )));
                    }
                }
            }
        } else if (func_name.eq_ignore_ascii_case("to_number")
            || func_name.eq_ignore_ascii_case("to_numeric")
            || func_name.eq_ignore_ascii_case("to_decimal")
            || func_name.eq_ignore_ascii_case("try_to_number")
            || func_name.eq_ignore_ascii_case("try_to_numeric")
            || func_name.eq_ignore_ascii_case("try_to_decimal"))
            && params.is_empty()
        {
            if args.is_empty() || args.len() > 4 {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                    params.len(),
                    arguments.len()
                )));
            }
            let func_ctx = self.ctx.get_function_context()?;
            let arg_fn = |args: &[ScalarExpr],
                          index: usize,
                          arg_name: &str,
                          default: i64|
             -> Result<i64> {
                Ok(args.get(index).map(|arg| {
                    match ConstantFolder::fold(&arg.as_expr()?, &func_ctx, &BUILTIN_FUNCTIONS).0 {
                        EExpr::Constant(Constant {
                            scalar,
                            ..
                        }) => Ok(scalar.get_i64()),
                        _ => Err(ErrorCode::SemanticError(format!("Invalid arguments for `{func_name}`, {arg_name} is only allowed to be a constant"))),
                    }
                }).transpose()?.flatten().unwrap_or(default))
            };

            let (precision_index, scale_index) =
                if args.len() > 1 && args[1].data_type()?.remove_nullable().is_string() {
                    (2, 3)
                } else {
                    (1, 2)
                };
            let precision = arg_fn(
                &args,
                precision_index,
                "precision",
                DEFAULT_DECIMAL_PRECISION,
            )?;
            let scale = arg_fn(&args, scale_index, "scale", DEFAULT_DECIMAL_SCALE)?;

            if let Err(err) = DecimalSize::new(precision as u8, scale as u8) {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, {}",
                    err,
                )));
            }

            params.push(Scalar::Number(NumberScalar::Int64(precision as _)));
            params.push(Scalar::Number(NumberScalar::Int64(scale as _)));
        }

        let raw_expr = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.clone(),
            args: arguments,
        };

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        let expr = type_check::rewrite_function_to_cast(expr);

        // Run constant folding for arguments of the scalar function.
        // This will be helpful to simplify some constant expressions, especially
        // the implicitly casted literal values, e.g. `timestamp > '2001-01-01'`
        // will be folded from `timestamp > to_timestamp('2001-01-01')` to `timestamp > 978307200000000`
        // Note: check function may reorder the args

        let mut folded_args = match &expr {
            expr::Expr::FunctionCall(expr::FunctionCall {
                function,
                args: checked_args,
                ..
            }) => checked_args
                .iter()
                .zip(
                    function
                        .signature
                        .args_type
                        .iter()
                        .map(DataType::is_generic),
                )
                .zip(args)
                .map(|((checked_arg, is_generic), arg)| {
                    if !arg.evaluable() {
                        return arg;
                    }
                    match self.try_fold_constant(checked_arg, !is_generic) {
                        Some(box (constant, _)) => constant,
                        _ => arg,
                    }
                })
                .collect(),
            _ => args,
        };

        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            self.ctx.result_cache_state().set_cacheable(false);
        }

        if let Some(constant) = self.try_fold_constant(&expr, true) {
            return Ok(constant);
        }

        if let expr::Expr::Cast(expr::Cast {
            span,
            is_try,
            dest_type,
            ..
        }) = expr
        {
            assert_eq!(folded_args.len(), 1);
            return Ok(Box::new((
                CastExpr {
                    span,
                    is_try,
                    argument: Box::new(folded_args.pop().unwrap()),
                    target_type: Box::new(dest_type.clone()),
                }
                .into(),
                dest_type,
            )));
        }

        // reorder
        if func_name == "eq"
            && folded_args.len() == 2
            && matches!(folded_args[0], ScalarExpr::ConstantExpr(_))
            && !matches!(folded_args[1], ScalarExpr::ConstantExpr(_))
        {
            folded_args.swap(0, 1);
        }

        Ok(Box::new((
            FunctionCall {
                span,
                params,
                arguments: folded_args,
                func_name: func_name.to_string(),
            }
            .into(),
            expr.data_type().clone(),
        )))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub fn resolve_binary_op(
        &mut self,
        span: Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            BinaryOperator::NotLike(_) | BinaryOperator::NotRegexp | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike(escape) => BinaryOperator::Like(escape.clone()),
                    BinaryOperator::NotRegexp => BinaryOperator::Regexp,
                    BinaryOperator::NotRLike => BinaryOperator::RLike,
                    _ => unreachable!(),
                };
                let (positive, _) = *self.resolve_binary_op(span, &positive_op, left, right)?;
                self.resolve_scalar_function_call(span, "not", vec![], vec![positive])
            }
            BinaryOperator::SoundsLike => {
                // rewrite "expr1 SOUNDS LIKE expr2" to "SOUNDEX(expr1) = SOUNDEX(expr2)"
                let box (left, _) = self.resolve(left)?;
                let box (right, _) = self.resolve(right)?;

                let (left, _) =
                    *self.resolve_scalar_function_call(span, "soundex", vec![], vec![left])?;
                let (right, _) =
                    *self.resolve_scalar_function_call(span, "soundex", vec![], vec![right])?;

                self.resolve_scalar_function_call(span, "eq", vec![], vec![left, right])
            }
            BinaryOperator::Like(escape) => {
                // Convert `Like` to compare function , such as `p_type like PROMO%` will be converted to `p_type >= PROMO and p_type < PROMP`
                if let Expr::Literal {
                    value: Literal::String(str),
                    ..
                } = right
                {
                    return self.resolve_like(op, span, left, right, str, escape);
                }
                self.resolve_like_escape(op, span, left, right, escape)
            }
            BinaryOperator::LikeAny(escape) => {
                self.resolve_like_escape(op, span, left, right, escape)
            }
            BinaryOperator::Eq | BinaryOperator::NotEq => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_call_expr(
                    span,
                    core_expr::binary_op_core_function(op).unwrap(),
                    [left, right],
                )?;
                self.resolve_core(&arena, root)
            }
            BinaryOperator::Plus | BinaryOperator::Minus => {
                let (mut left_expr, left_type) = *self.resolve(left)?;
                let (mut right_expr, right_type) = *self.resolve(right)?;
                self.adjust_date_interval_operands(
                    op,
                    &mut left_expr,
                    &left_type,
                    &mut right_expr,
                    &right_type,
                )?;
                self.resolve_scalar_function_call(
                    span,
                    core_expr::binary_op_core_function(op).unwrap(),
                    vec![],
                    vec![left_expr, right_expr],
                )
            }
            other => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_call_expr(
                    span,
                    core_expr::binary_op_core_function(other).unwrap(),
                    [left, right],
                )?;
                self.resolve_core(&arena, root)
            }
        }
    }

    /// Resolve unary expressions.
    pub fn resolve_unary_op(
        &mut self,
        span: Span,
        op: &UnaryOperator,
        child: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            UnaryOperator::Plus => {
                // Omit unary + operator
                self.resolve(child)
            }
            UnaryOperator::Minus => {
                if let Expr::Literal { value, .. } = child {
                    let box (value, data_type) = self.resolve_minus_literal_scalar(span, value)?;
                    let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
                    return Ok(Box::new((scalar_expr, data_type)));
                }
                let mut arena = self.core_expr_arena();
                let root = arena.lower_call_expr(
                    span,
                    core_expr::unary_op_core_function(op).unwrap(),
                    [child],
                )?;
                self.resolve_core(&arena, root)
            }
            other => {
                let mut arena = self.core_expr_arena();
                let root = arena.lower_call_expr(
                    span,
                    core_expr::unary_op_core_function(other).unwrap(),
                    [child],
                )?;
                self.resolve_core(&arena, root)
            }
        }
    }

    fn convert_inlist_to_subquery(
        &mut self,
        expr: &Expr,
        list: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
        let mut values = Vec::with_capacity(list.len());
        for val in list.iter() {
            values.push(vec![val.clone()])
        }
        let (const_scan, ctx) = bind_values(
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &mut bind_context,
            None,
            &values,
            None,
        )?;

        assert_eq!(ctx.columns.len(), 1);
        // Wrap group by on `const_scan` to deduplicate values
        let distinct_const_scan = const_scan.build_unary(Aggregate {
            mode: AggregateMode::Initial,
            group_items: vec![ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ctx.columns[0].clone(),
                }),
                index: ctx.columns[0].index,
            }],
            ..Default::default()
        });

        let box mut data_type = ctx.columns[0].data_type.clone();
        let rel_expr = RelExpr::with_s_expr(&distinct_const_scan);
        let rel_prop = rel_expr.derive_relational_prop()?;
        let box (scalar, expr_ty) = self.resolve(expr)?;
        // wrap nullable to make sure expr and list values have common type.
        if expr_ty.is_nullable() {
            data_type = data_type.wrap_nullable();
        }
        let child_scalar = Some(Box::new(scalar));
        let subquery_expr = SubqueryExpr {
            span: None,
            subquery: Box::new(distinct_const_scan),
            child_expr: child_scalar,
            compare_op: Some(SubqueryComparisonOp::Equal),
            output_column: ctx.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ: SubqueryType::Any,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg: None,
        };
        let data_type = subquery_expr.output_data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn clone_expr_with_replacement<F>(original_expr: &Expr, replacement_fn: F) -> Result<Expr>
    where F: Fn(&Expr) -> Result<Option<Expr>> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter))]
        struct ReplacerVisitor<F: Fn(&Expr) -> Result<Option<Expr>>>(F);

        impl<F: Fn(&Expr) -> Result<Option<Expr>>> ReplacerVisitor<F> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                let replacement_opt = (self.0)(expr);
                if let Ok(Some(replacement)) = replacement_opt {
                    *expr = replacement;
                }
            }
        }
        let mut visitor = ReplacerVisitor(replacement_fn);
        let mut expr = original_expr.clone();
        expr.drive_mut(&mut visitor);
        Ok(expr)
    }

    fn try_fold_constant<Index: ColumnIndex>(
        &self,
        expr: &EExpr<Index>,
        enable_shrink: bool,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if expr.is_deterministic(&BUILTIN_FUNCTIONS) && enable_shrink {
            if let (EExpr::Constant(expr::Constant { scalar, .. }), _) =
                ConstantFolder::fold(expr, &self.func_ctx, &BUILTIN_FUNCTIONS)
            {
                let scalar = if enable_shrink {
                    shrink_scalar(scalar)
                } else {
                    scalar
                };
                let ty = scalar.as_ref().infer_data_type();
                return Some(Box::new((
                    ConstantExpr {
                        span: expr.span(),
                        value: scalar,
                    }
                    .into(),
                    ty,
                )));
            }
        }

        None
    }
}

impl<'a> TypeChecker<'a> {
    /// Get masking policy expression for a column reference
    /// This is the ONLY place where masking policy is applied - unifying all paths (SELECT/WHERE/HAVING)
    async fn get_masking_policy_expr_for_column(
        &self,
        column_binding: &crate::ColumnBinding,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Option<Expr>> {
        use databend_common_ast::ast;
        use databend_common_license::license::Feature::DataMask;
        use databend_common_license::license_manager::LicenseManagerSwitch;
        use databend_common_users::UserApiProvider;
        use databend_common_users::security_policy_cache::PolicyType;
        use databend_common_users::security_policy_cache::RawPolicyDef;
        use databend_common_users::security_policy_cache::SecurityPolicyCacheManager;
        use databend_enterprise_data_mask_feature::get_datamask_handler;

        // Check if this column has a masking policy
        if let Some(table_index) = column_binding.table_index {
            // Extract all needed data before the await point to avoid holding the mutex lock
            let policy_data = {
                let metadata = self.metadata.read();
                let table_entry = metadata.table(table_index);
                let table_ref = table_entry.table();
                let table_info_ref = table_ref.get_table_info();
                let table_schema = table_ref.schema();

                // Find the field by name to get column_id
                if let Some(field) = table_schema
                    .fields()
                    .iter()
                    .find(|f| f.name == column_binding.column_name)
                {
                    if let Some(policy_info) = table_info_ref
                        .meta
                        .column_mask_policy_columns_ids
                        .get(&field.column_id)
                    {
                        // Check license
                        if LicenseManagerSwitch::instance()
                            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)
                            .is_err()
                        {
                            return Ok(None);
                        }

                        // Extract data needed after await
                        Some((
                            policy_info.policy_id,
                            policy_info.columns_ids.clone(),
                            table_schema,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }; // metadata lock is released here

            if let Some((policy_id, using_columns, table_schema)) = policy_data {
                let tenant = self.ctx.get_tenant();
                let cache = SecurityPolicyCacheManager::instance();
                let meta_api = UserApiProvider::instance().get_meta_store_client();
                let tenant_clone = tenant.clone();

                let cached = cache
                    .get_or_load(
                        PolicyType::DataMask,
                        &tenant,
                        policy_id,
                        || async move {
                            let handler = get_datamask_handler();
                            let seq_v = handler
                                .get_data_mask_by_id(meta_api, &tenant_clone, policy_id)
                                .await?;
                            let meta = seq_v.data;
                            Ok(RawPolicyDef {
                                body: meta.body,
                                args: meta.args,
                            })
                        },
                    )
                    .await
                    .map_err(|err| {
                        ErrorCode::UnknownMaskPolicy(format!(
                            "Failed to load masking policy (id: {}) for column '{}': {}. Query denied to prevent potential data leakage. Please verify the policy still exists and meta service is available",
                            policy_id, column_binding.column_name, err
                        ))
                    })?;

                let args = &cached.args;

                // Create arguments based on USING clause
                let arguments: Result<Vec<Expr>> = args
                            .iter()
                            .enumerate()
                            .map(|(param_idx, _)| {
                                let column_id = using_columns.get(param_idx).ok_or_else(|| {
                                    ErrorCode::Internal(format!(
                                        "Masking policy metadata is corrupted: policy requires {} parameters, \
                                         but only {} columns are configured in USING clause. \
                                         Please drop and recreate the masking policy attachment.",
                                        args.len(),
                                        using_columns.len()
                                    ))
                                })?;

                                let field_name = table_schema
                                    .fields()
                                    .iter()
                                    .find(|f| f.column_id == *column_id)
                                    .map(|f| f.name.clone())
                                    .unwrap_or_else(|| format!("column_{}", column_id));

                                Ok(Expr::ColumnRef {
                                    span: None,
                                    column: ast::ColumnRef {
                                        database: database.map(|d| Identifier::from_name(None, d.to_string())),
                                        table: table.map(|t| Identifier::from_name(None, t.to_string())),
                                        column: ast::ColumnID::Name(Identifier::from_name(
                                            None, field_name,
                                        )),
                                    },
                                })
                            })
                            .collect();
                let arguments = arguments?;

                // Create parameter mapping
                // Since parameter names are normalized to lowercase at policy creation time (see data_mask.rs),
                // we use them directly as keys.
                let args_map: HashMap<_, _> = args
                    .iter()
                    .map(|(param_name, _)| param_name.as_str())
                    .zip(arguments.iter().cloned())
                    .collect();

                // Replace parameters in the expression
                let expr = Self::clone_expr_with_replacement(&cached.expr, |nest_expr| {
                    if let Expr::ColumnRef { column, .. } = nest_expr {
                        // Parameter names are already lowercase in args_map (normalized at creation).
                        // Lookup also needs to be lowercase for consistent matching.
                        if let Some(arg) =
                            args_map.get(column.column.name().to_lowercase().as_str())
                        {
                            return Ok(Some(arg.clone()));
                        }
                    }
                    Ok(None)
                })?;

                return Ok(Some(expr));
            }
        }
        Ok(None)
    }
}
