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
use std::collections::VecDeque;
use std::sync::Arc;
use std::vec;

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::IntervalKind as ASTIntervalKind;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_ast::ast::Query;
use common_ast::ast::SubqueryModifier;
use common_ast::ast::TrimWhere;
use common_ast::ast::TypeName;
use common_ast::ast::UnaryOperator;
use common_ast::parser::parse_expr;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_expression::infer_schema_type;
use common_expression::type_check;
use common_expression::type_check::check_literal;
use common_expression::type_check::common_super_type;
use common_expression::types::decimal::DecimalDataType;
use common_expression::types::decimal::DecimalSize;
use common_expression::types::number::F64;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::RawExpr;
use common_expression::TableDataType;
use common_functions::aggregates::AggregateCountFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::is_builtin_function;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_users::UserApiProvider;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use crate::binder::Binder;
use crate::binder::NameResolutionResult;
use crate::optimizer::RelExpr;
use crate::planner::binder::wrap_cast_if_needed;
use crate::planner::metadata::optimize_remove_count_args;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::MetadataRef;

/// A helper for type checking.
///
/// `TypeChecker::resolve` will resolve types of `Expr` and transform `Expr` into
/// a typed expression `Scalar`. At the same time, name resolution will be performed,
/// which check validity of unbound `ColumnRef` and try to replace it with qualified
/// `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a BindContext,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,

    aliases: &'a [(String, ScalarExpr)],

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,
}

impl<'a> TypeChecker<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        Self {
            bind_context,
            ctx,
            name_resolution_ctx,
            metadata,
            aliases,
            in_aggregate_function: false,
        }
    }

    fn post_resolve(
        &mut self,
        scalar: &ScalarExpr,
        data_type: &DataType,
    ) -> Result<(ScalarExpr, DataType)> {
        // TODO(leiysky): constant folding with new expression
        //
        // if let Ok((value, value_type)) = Evaluator::eval_scalar(scalar).and_then(|evaluator| {
        //     let func_ctx = self.ctx.try_get_function_context()?;
        //     if scalar.is_deterministic() {
        //         evaluator.try_eval_const(&func_ctx)
        //     } else {
        //         Err(ErrorCode::Internal(
        //             "Constant folding requires the function deterministic",
        //         ))
        //     }
        // }) {
        //     Ok((
        //         ConstantExpr {
        //             value,
        //             data_type: Box::new(value_type),
        //         }
        //         .into(),
        //         data_type.clone(),
        //     ))
        // } else {
        //     Ok((scalar.clone(), data_type.clone()))
        // }

        Ok((scalar.clone(), data_type.clone()))
    }

    #[async_recursion::async_recursion]
    pub async fn resolve(
        &mut self,
        expr: &Expr,
        // TODO(andylokandy): remove me
        required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (scalar, data_type): Box<(ScalarExpr, DataType)> = match expr {
            Expr::ColumnRef {
                database,
                table,
                column: ident,
                ..
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let table = table
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let column = normalize_identifier(ident, self.name_resolution_ctx).name;
                let result = self.bind_context.resolve_name(
                    database.as_deref(),
                    table.as_deref(),
                    column.as_str(),
                    ident.span,
                    self.aliases,
                )?;
                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        let data_type = *column.data_type.clone();
                        (BoundColumnRef { column }.into(), data_type)
                    }
                    NameResolutionResult::Alias { scalar, .. } => {
                        (scalar.clone(), scalar.data_type())
                    }
                };

                Box::new((scalar, data_type))
            }

            Expr::IsNull {
                span, expr, not, ..
            } => {
                let args = &[expr.as_ref()];
                if *not {
                    self.resolve_function(*span, "is_not_null", vec![], args, required_type)
                        .await?
                } else {
                    self.resolve_function(*span, "is_null", vec![], args, required_type)
                        .await?
                }
            }

            Expr::IsDistinctFrom {
                span,
                left,
                right,
                not,
            } => {
                let left_null_expr = Box::new(Expr::IsNull {
                    span: *span,
                    expr: left.clone(),
                    not: false,
                });
                let right_null_expr = Box::new(Expr::IsNull {
                    span: *span,
                    expr: right.clone(),
                    not: false,
                });
                let op = if *not {
                    BinaryOperator::Eq
                } else {
                    BinaryOperator::NotEq
                };
                let (scalar, _) = *self
                    .resolve_function(
                        *span,
                        "multi_if",
                        vec![],
                        &[
                            &Expr::BinaryOp {
                                span: *span,
                                op: BinaryOperator::And,
                                left: left_null_expr.clone(),
                                right: right_null_expr.clone(),
                            },
                            &Expr::Literal {
                                span: *span,
                                lit: Literal::Boolean(*not),
                            },
                            &Expr::BinaryOp {
                                span: *span,
                                op: BinaryOperator::Or,
                                left: left_null_expr.clone(),
                                right: right_null_expr.clone(),
                            },
                            &Expr::Literal {
                                span: *span,
                                lit: Literal::Boolean(!*not),
                            },
                            &Expr::BinaryOp {
                                span: *span,
                                op,
                                left: left.clone(),
                                right: right.clone(),
                            },
                        ],
                        None,
                    )
                    .await?;
                self.resolve_scalar_function_call(
                    *span,
                    "assume_not_null",
                    vec![],
                    vec![scalar],
                    required_type,
                )
                .await?
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                let get_max_inlist_to_or = self.ctx.get_settings().get_max_inlist_to_or()? as usize;
                if list.len() > get_max_inlist_to_or
                    && list
                        .iter()
                        .all(|e| matches!(e, Expr::Literal { lit, .. } if lit != &Literal::Null))
                {
                    let array_expr = Expr::Array {
                        span: *span,
                        exprs: list.clone(),
                    };
                    let args = vec![&array_expr, expr.as_ref()];
                    if *not {
                        self.resolve_unary_op(
                            *span,
                            &UnaryOperator::Not,
                            &Expr::FunctionCall {
                                span: *span,
                                distinct: false,
                                name: Identifier {
                                    name: "contains".to_string(),
                                    quote: None,
                                    span: *span,
                                },
                                args: args.iter().copied().cloned().collect(),
                                params: vec![],
                            },
                            None,
                        )
                        .await?
                    } else {
                        self.resolve_function(*span, "contains", vec![], &args, required_type)
                            .await?
                    }
                } else {
                    let mut result = list
                        .iter()
                        .map(|e| Expr::BinaryOp {
                            span: *span,
                            op: BinaryOperator::Eq,
                            left: expr.clone(),
                            right: Box::new(e.clone()),
                        })
                        .fold(None, |mut acc, e| {
                            match acc.as_mut() {
                                None => acc = Some(e),
                                Some(acc) => {
                                    *acc = Expr::BinaryOp {
                                        span: *span,
                                        op: BinaryOperator::Or,
                                        left: Box::new(acc.clone()),
                                        right: Box::new(e),
                                    }
                                }
                            }
                            acc
                        })
                        .unwrap();

                    if *not {
                        result = Expr::UnaryOp {
                            span: *span,
                            op: UnaryOperator::Not,
                            expr: Box::new(result),
                        };
                    }
                    self.resolve(&result, required_type).await?
                }
            }

            Expr::Between {
                span,
                expr,
                low,
                high,
                not,
                ..
            } => {
                if !*not {
                    // Rewrite `expr BETWEEN low AND high`
                    // into `expr >= low AND expr <= high`
                    let (ge_func, _left_type) = *self
                        .resolve_binary_op(
                            *span,
                            &BinaryOperator::Gte,
                            expr.as_ref(),
                            low.as_ref(),
                            None,
                        )
                        .await?;
                    let (le_func, _right_type) = *self
                        .resolve_binary_op(
                            *span,
                            &BinaryOperator::Lte,
                            expr.as_ref(),
                            high.as_ref(),
                            None,
                        )
                        .await?;

                    let (_, data_type) = *self
                        .resolve_scalar_function_call(
                            *span,
                            "and",
                            vec![],
                            vec![ge_func.clone(), le_func.clone()],
                            required_type,
                        )
                        .await?;

                    Box::new((
                        AndExpr {
                            left: Box::new(ge_func),
                            right: Box::new(le_func),
                            return_type: Box::new(data_type.clone()),
                        }
                        .into(),
                        data_type,
                    ))
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let (lt_func, _left_type) = *self
                        .resolve_binary_op(
                            *span,
                            &BinaryOperator::Lt,
                            expr.as_ref(),
                            low.as_ref(),
                            None,
                        )
                        .await?;
                    let (gt_func, _right_type) = *self
                        .resolve_binary_op(
                            *span,
                            &BinaryOperator::Gt,
                            expr.as_ref(),
                            high.as_ref(),
                            None,
                        )
                        .await?;

                    let (_, data_type) = *self
                        .resolve_scalar_function_call(
                            *span,
                            "or",
                            vec![],
                            vec![lt_func.clone(), gt_func.clone()],
                            required_type,
                        )
                        .await?;

                    Box::new((
                        OrExpr {
                            left: Box::new(lt_func),
                            right: Box::new(gt_func),
                            return_type: Box::new(data_type.clone()),
                        }
                        .into(),
                        data_type,
                    ))
                }
            }

            Expr::BinaryOp {
                span,
                op,
                left,
                right,
                ..
            } => {
                if let Expr::Subquery {
                    subquery, modifier, ..
                } = &**right
                {
                    if let Some(subquery_modifier) = modifier {
                        match subquery_modifier {
                            SubqueryModifier::Any | SubqueryModifier::Some => {
                                let comparison_op = ComparisonOp::try_from(op)?;
                                self.resolve_subquery(
                                    SubqueryType::Any,
                                    subquery,
                                    true,
                                    Some(*left.clone()),
                                    Some(comparison_op),
                                    None,
                                )
                                .await?
                            }
                            SubqueryModifier::All => {
                                let contrary_op = op.to_contrary()?;
                                let rewritten_subquery = Expr::Subquery {
                                    span: right.span(),
                                    modifier: Some(SubqueryModifier::Any),
                                    subquery: (*subquery).clone(),
                                };
                                self.resolve_unary_op(
                                    *span,
                                    &UnaryOperator::Not,
                                    &Expr::BinaryOp {
                                        span: *span,
                                        op: contrary_op,
                                        left: (*left).clone(),
                                        right: Box::new(rewritten_subquery),
                                    },
                                    None,
                                )
                                .await?
                            }
                        }
                    } else {
                        self.resolve_binary_op(
                            *span,
                            op,
                            left.as_ref(),
                            right.as_ref(),
                            required_type,
                        )
                        .await?
                    }
                } else {
                    self.resolve_binary_op(*span, op, left.as_ref(), right.as_ref(), required_type)
                        .await?
                }
            }

            Expr::UnaryOp { span, op, expr, .. } => {
                self.resolve_unary_op(*span, op, expr.as_ref(), required_type)
                    .await?
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type).await?;
                let raw_expr = RawExpr::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(scalar.as_raw_expr_with_col_name()),
                    dest_type: DataType::from(&Self::resolve_type_name(target_type)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let expr = type_check::check(&raw_expr, registry)?;
                Box::new((
                    CastExpr {
                        is_try: false,
                        argument: Box::new(scalar),
                        from_type: Box::new(data_type),
                        target_type: Box::new(expr.data_type().clone()),
                    }
                    .into(),
                    expr.data_type().clone(),
                ))
            }

            Expr::TryCast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type).await?;
                let raw_expr = RawExpr::Cast {
                    span: None,
                    is_try: true,
                    expr: Box::new(scalar.as_raw_expr_with_col_name()),
                    dest_type: DataType::from(&Self::resolve_type_name(target_type)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let expr = type_check::check(&raw_expr, registry)?;
                Box::new((
                    CastExpr {
                        is_try: true,
                        argument: Box::new(scalar),
                        from_type: Box::new(data_type),
                        target_type: Box::new(expr.data_type().clone()),
                    }
                    .into(),
                    expr.data_type().clone(),
                ))
            }

            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut arguments = Vec::with_capacity(conditions.len() * 2 + 1);
                for (c, r) in conditions.iter().zip(results.iter()) {
                    match operand {
                        Some(operand) => {
                            // compare case operand with each conditions until one of them is equal
                            let equal_expr = Expr::FunctionCall {
                                span: *span,
                                distinct: false,
                                name: Identifier {
                                    name: "eq".to_string(),
                                    quote: None,
                                    span: *span,
                                },
                                args: vec![*operand.clone(), c.clone()],
                                params: vec![],
                            };
                            arguments.push(equal_expr)
                        }
                        None => arguments.push(c.clone()),
                    }
                    arguments.push(r.clone());
                }
                let null_arg = Expr::Literal {
                    span: None,
                    lit: Literal::Null,
                };

                if let Some(expr) = else_result {
                    arguments.push(*expr.clone());
                } else {
                    arguments.push(null_arg)
                }
                let args_ref: Vec<&Expr> = arguments.iter().collect();

                match args_ref.len() {
                    // faster path
                    3 => {
                        self.resolve_function(*span, "if", vec![], &args_ref, required_type)
                            .await?
                    }
                    _ => {
                        self.resolve_function(*span, "multi_if", vec![], &args_ref, required_type)
                            .await?
                    }
                }
            }

            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut arguments = vec![expr.as_ref(), substring_from.as_ref()];
                if let Some(substring_for) = substring_for {
                    arguments.push(substring_for.as_ref());
                }
                self.resolve_function(*span, "substring", vec![], &arguments, required_type)
                    .await?
            }

            Expr::Literal { lit, .. } => {
                let box (value, data_type) = self.resolve_literal(lit, required_type)?;
                Box::new((
                    ConstantExpr {
                        value,
                        data_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                ))
            }

            Expr::FunctionCall {
                span,
                distinct,
                name,
                args,
                params,
                ..
            } => {
                let func_name = name.name.to_lowercase();
                let func_name = func_name.as_str();
                if !is_builtin_function(func_name)
                    && !Self::all_rewritable_scalar_function().contains(&func_name)
                {
                    return self.resolve_udf(*span, func_name, args).await;
                }

                let args: Vec<&Expr> = args.iter().collect();

                if AggregateFunctionFactory::instance().contains(func_name) {
                    if self.in_aggregate_function {
                        // Reset the state
                        self.in_aggregate_function = false;
                        return Err(ErrorCode::SemanticError(
                            "aggregate function calls cannot be nested".to_string(),
                        )
                        .set_span(expr.span()));
                    }

                    // Check aggregate function
                    let params = params
                        .iter()
                        .map(|literal| {
                            self.resolve_literal(literal, None)
                                .map(|box (value, _)| value)
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let scalar_params = params
                        .iter()
                        .map(|param| check_literal(param).0)
                        .collect::<Vec<_>>();

                    self.in_aggregate_function = true;
                    let mut arguments = vec![];
                    let mut arg_types = vec![];
                    for arg in args.iter() {
                        let box (argument, arg_type) = self.resolve(arg, None).await?;
                        arguments.push(argument);
                        arg_types.push(arg_type);
                    }
                    self.in_aggregate_function = false;

                    // Rewrite `xxx(distinct)` to `xxx_distinct(...)`
                    let (func_name, distinct) =
                        if func_name.eq_ignore_ascii_case("count") && *distinct {
                            ("count_distinct", false)
                        } else {
                            (func_name, *distinct)
                        };

                    let func_name = if distinct {
                        format!("{}_distinct", func_name)
                    } else {
                        func_name.to_string()
                    };

                    let agg_func = AggregateFunctionFactory::instance()
                        .get(&func_name, scalar_params, arg_types)
                        .map_err(|e| e.set_span(*span))?;

                    let args = if optimize_remove_count_args(&func_name, distinct, args.as_slice())
                    {
                        vec![]
                    } else {
                        arguments
                    };

                    Box::new((
                        AggregateFunction {
                            display_name: format!("{:#}", expr),
                            func_name,
                            distinct: false,
                            params,
                            args,
                            return_type: Box::new(agg_func.return_type()?),
                        }
                        .into(),
                        agg_func.return_type()?,
                    ))
                } else {
                    // Scalar function
                    let params = params
                        .iter()
                        .map(|literal| match literal {
                            Literal::Integer(n) => Ok(*n as usize),
                            lit => Err(ErrorCode::SemanticError(format!(
                                "Invalid parameter {lit} for scalar function"
                            ))
                            .set_span(*span)),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    self.resolve_function(*span, func_name, params, &args, required_type)
                        .await?
                }
            }

            Expr::CountAll { .. } => {
                let agg_func = AggregateCountFunction::try_create("", vec![], vec![])?;

                Box::new((
                    AggregateFunction {
                        display_name: format!("{:#}", expr),
                        func_name: "count".to_string(),
                        distinct: false,
                        params: vec![],
                        args: vec![],
                        return_type: Box::new(agg_func.return_type()?),
                    }
                    .into(),
                    agg_func.return_type()?,
                ))
            }

            Expr::Exists { subquery, not, .. } => {
                self.resolve_subquery(
                    if !*not {
                        SubqueryType::Exists
                    } else {
                        SubqueryType::NotExists
                    },
                    subquery,
                    true,
                    None,
                    None,
                    None,
                )
                .await?
            }

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, false, None, None, None)
                    .await?
            }

            Expr::InSubquery {
                subquery,
                not,
                expr,
                span,
            } => {
                // Not in subquery will be transformed to not(Expr = Any(...))
                if *not {
                    return self
                        .resolve_unary_op(
                            *span,
                            &UnaryOperator::Not,
                            &Expr::InSubquery {
                                subquery: subquery.clone(),
                                not: false,
                                expr: expr.clone(),
                                span: *span,
                            },
                            required_type,
                        )
                        .await;
                }
                // InSubquery will be transformed to Expr = Any(...)
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    true,
                    Some(*expr.clone()),
                    Some(ComparisonOp::Equal),
                    None,
                )
                .await?
            }

            expr @ Expr::MapAccess { span, .. } => {
                let mut expr = expr;
                let mut paths = VecDeque::new();
                while let Expr::MapAccess {
                    expr: inner_expr,
                    accessor,
                    ..
                } = expr
                {
                    expr = &**inner_expr;
                    let path = match accessor {
                        MapAccessor::Bracket {
                            key: box Expr::Literal { lit, .. },
                        } => lit.clone(),
                        MapAccessor::Period { key } | MapAccessor::Colon { key } => {
                            Literal::String(key.name.clone())
                        }
                        MapAccessor::PeriodNumber { key } => Literal::Integer(*key),
                        _ => {
                            return Err(ErrorCode::SemanticError(format!(
                                "Unsupported accessor: {:?}",
                                accessor
                            ))
                            .set_span(*span));
                        }
                    };
                    paths.push_front(path);
                }
                self.resolve_map_access(expr, paths).await?
            }

            Expr::Extract {
                span, kind, expr, ..
            } => {
                self.resolve_extract_expr(*span, kind, expr, required_type)
                    .await?
            }

            Expr::Interval { span, .. } => {
                return Err(ErrorCode::SemanticError(
                    "Unsupport interval expression yet".to_string(),
                )
                .set_span(*span));
            }
            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
                ..
            } => {
                self.resolve_date_add(*span, unit, interval, date, required_type)
                    .await?
            }
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
                ..
            } => {
                self.resolve_date_add(
                    *span,
                    unit,
                    &Expr::UnaryOp {
                        span: *span,
                        op: UnaryOperator::Minus,
                        expr: interval.clone(),
                    },
                    date,
                    required_type,
                )
                .await?
            }
            Expr::DateTrunc {
                span, unit, date, ..
            } => {
                self.resolve_date_trunc(*span, date, unit, required_type)
                    .await?
            }
            Expr::Trim {
                span,
                expr,
                trim_where,
                ..
            } => self.resolve_trim_function(*span, expr, trim_where).await?,

            Expr::Array { span, exprs, .. } => self.resolve_array(*span, exprs).await?,

            Expr::Position {
                substr_expr,
                str_expr,
                span,
                ..
            } => {
                self.resolve_function(
                    *span,
                    "locate",
                    vec![],
                    &[substr_expr.as_ref(), str_expr.as_ref()],
                    None,
                )
                .await?
            }

            Expr::Tuple { span, exprs, .. } => self.resolve_tuple(*span, exprs).await?,
        };

        Ok(Box::new(self.post_resolve(&scalar, &data_type)?))
    }

    // TODO: remove this function
    fn rewrite_substring(args: &mut [ScalarExpr]) {
        if let ScalarExpr::ConstantExpr(expr) = &args[1] {
            if let common_expression::Literal::UInt8(0) = expr.value {
                args[1] = ConstantExpr {
                    value: common_expression::Literal::Int64(1),
                    data_type: Box::new(DataType::Number(NumberDataType::Int64)),
                }
                .into();
            }
        }
    }

    /// Resolve function call.
    #[async_recursion::async_recursion]
    pub async fn resolve_function(
        &mut self,
        span: Span,
        func_name: &str,
        params: Vec<usize>,
        arguments: &[&Expr],
        required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Check if current function is a virtual function, e.g. `database`, `version`
        if let Some(rewriten_func_result) = self
            .try_rewrite_scalar_function(span, func_name, arguments)
            .await
        {
            return rewriten_func_result;
        }

        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let box (arg, mut arg_type) = self.resolve(argument, None).await?;
            if let ScalarExpr::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type().is_nullable() {
                    arg_type = arg_type.wrap_nullable();
                }
            }
            args.push(arg);
            arg_types.push(arg_type);
        }

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

        // rewrite_collation
        let func_name = if self.function_need_collation(func_name, &args)
            && self.ctx.get_settings().get_collation()? == "utf8"
        {
            format!("{func_name}_utf8")
        } else {
            func_name.to_owned()
        };

        self.resolve_scalar_function_call(span, &func_name, params, args, required_type)
            .await
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_scalar_function_call(
        &mut self,
        _span: Span,
        func_name: &str,
        params: Vec<usize>,
        args: Vec<ScalarExpr>,
        _required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Type check
        let arguments = args
            .iter()
            .map(|v| v.as_raw_expr_with_col_name())
            .collect::<Vec<_>>();
        let raw_expr = RawExpr::FunctionCall {
            span: None,
            name: func_name.to_string(),
            params: vec![],
            args: arguments,
        };
        let registry = &BUILTIN_FUNCTIONS;
        let expr = type_check::check(&raw_expr, registry)?;

        Ok(Box::new((
            FunctionCall {
                params,
                arguments: args,
                func_name: func_name.to_string(),
                return_type: Box::new(expr.data_type().clone()),
            }
            .into(),
            expr.data_type().clone(),
        )))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    #[async_recursion::async_recursion]
    pub async fn resolve_binary_op(
        &mut self,
        span: Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
        required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            BinaryOperator::NotLike | BinaryOperator::NotRegexp | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike => BinaryOperator::Like,
                    BinaryOperator::NotRegexp => BinaryOperator::Regexp,
                    BinaryOperator::NotRLike => BinaryOperator::RLike,
                    _ => unreachable!(),
                };
                let (positive, data_type) = *self
                    .resolve_binary_op(span, &positive_op, left, right, required_type)
                    .await?;
                let return_type = Box::new(data_type.clone());
                let scalar = ScalarExpr::NotExpr(NotExpr {
                    argument: Box::new(positive),
                    return_type,
                });
                Ok(Box::new((scalar, data_type)))
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let op = ComparisonOp::try_from(op)?;
                let box (left, _) = self.resolve(left, None).await?;
                let box (right, _) = self.resolve(right, None).await?;

                let (_, data_type) = *self
                    .resolve_scalar_function_call(
                        span,
                        op.to_func_name(),
                        vec![],
                        vec![left.clone(), right.clone()],
                        required_type,
                    )
                    .await?;

                Ok(Box::new((
                    ComparisonExpr {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                )))
            }
            BinaryOperator::And => {
                let box (left, _) = self.resolve(left, None).await?;
                let box (right, _) = self.resolve(right, None).await?;

                let (_, data_type) = *self
                    .resolve_scalar_function_call(
                        span,
                        "and",
                        vec![],
                        vec![left.clone(), right.clone()],
                        required_type,
                    )
                    .await?;

                Ok(Box::new((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                )))
            }
            BinaryOperator::Or => {
                let box (left, _) = self.resolve(left, None).await?;
                let box (right, _) = self.resolve(right, None).await?;

                let (_, data_type) = *self
                    .resolve_scalar_function_call(
                        span,
                        "or",
                        vec![],
                        vec![left.clone(), right.clone()],
                        required_type,
                    )
                    .await?;

                Ok(Box::new((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                )))
            }
            other => {
                let name = other.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[left, right], required_type)
                    .await
            }
        }
    }

    /// Resolve unary expressions.
    #[async_recursion::async_recursion]
    pub async fn resolve_unary_op(
        &mut self,
        span: Span,
        op: &UnaryOperator,
        child: &Expr,
        required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            UnaryOperator::Plus => {
                // Omit unary + operator
                self.resolve(child, required_type).await
            }

            UnaryOperator::Minus => {
                self.resolve_function(span, "minus", vec![], &[child], required_type)
                    .await
            }

            UnaryOperator::Not => {
                let (argument, _) = *self.resolve(child, None).await?;

                let (_, data_type) = *self
                    .resolve_scalar_function_call(
                        span,
                        "not",
                        vec![],
                        vec![argument.clone()],
                        required_type,
                    )
                    .await?;

                Ok(Box::new((
                    NotExpr {
                        argument: Box::new(argument),
                        return_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                )))
            }
        }
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_extract_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        arg: &Expr,
        _required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match interval_kind {
            ASTIntervalKind::Year => {
                self.resolve_function(span, "to_year", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(span, "to_quarter", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Month => {
                self.resolve_function(span, "to_month", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Day => {
                self.resolve_function(span, "to_day_of_month", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(span, "to_hour", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(span, "to_minute", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Second => {
                self.resolve_function(span, "to_second", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Doy => {
                self.resolve_function(span, "to_day_of_year", vec![], &[arg], None)
                    .await
            }
            ASTIntervalKind::Dow => {
                self.resolve_function(span, "to_day_of_week", vec![], &[arg], None)
                    .await
            }
        }
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_date_add(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        interval: &Expr,
        date: &Expr,
        required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = format!("add_{}s", interval_kind.to_string().to_lowercase());

        let mut args = vec![];
        let mut arg_types = vec![];

        let (date, date_type) = *self.resolve(date, None).await?;
        args.push(date);
        arg_types.push(date_type);

        let (interval, interval_type) = *self.resolve(interval, None).await?;

        args.push(interval);
        arg_types.push(interval_type);

        self.resolve_scalar_function_call(span, &func_name, vec![], args, required_type)
            .await
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_date_trunc(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
        _required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(
                    span,
                    "to_start_of_quarter",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            ASTIntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second",vec![], 
                    &[date],
                    None,
                )
                    .await
            }
            _ => Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, month, day, hour, minute, second]".to_string()).set_span(span)),
        }
    }

    pub async fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query,
        allow_multi_rows: bool,
        child_expr: Option<Expr>,
        compare_op: Option<ComparisonOp>,
        _required_type: Option<DataType>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let bind_context = BindContext::with_parent(Box::new(self.bind_context.clone()));
        let (s_expr, output_context) = binder.bind_query(&bind_context, subquery).await?;

        if (typ == SubqueryType::Scalar || typ == SubqueryType::Any)
            && output_context.columns.len() > 1
        {
            return Err(ErrorCode::SemanticError(format!(
                "Subquery must return only one column, but got {} columns",
                output_context.columns.len()
            )));
        }

        let mut data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        let mut child_scalar = None;
        if let Some(expr) = child_expr {
            assert_eq!(output_context.columns.len(), 1);
            let box (mut scalar, scalar_data_type) = self.resolve(&expr, None).await?;
            if scalar_data_type != *data_type {
                // Make comparison scalar type keep consistent
                let coercion_type = common_super_type(scalar_data_type, *data_type.clone())
                    .ok_or_else(|| ErrorCode::Internal("Cannot find common super type"))?;
                scalar = wrap_cast_if_needed(&scalar, &coercion_type);
                data_type = Box::new(coercion_type);
            }
            child_scalar = Some(Box::new(scalar));
        }

        if typ.eq(&SubqueryType::Scalar) {
            data_type = Box::new(data_type.wrap_nullable());
        }

        let subquery_expr = SubqueryExpr {
            subquery: Box::new(s_expr),
            child_expr: child_scalar,
            compare_op,
            output_column: output_context.columns[0].index,
            projection_index: None,
            data_type: data_type.clone(),
            allow_multi_rows,
            typ,
            outer_columns: rel_prop.outer_columns,
        };

        let data_type = subquery_expr.data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    pub fn all_rewritable_scalar_function() -> &'static [&'static str] {
        &[
            "database",
            "currentdatabase",
            "current_database",
            "version",
            "user",
            "currentuser",
            "current_user",
            "current_role",
            "connection_id",
            "timezone",
            "nullif",
            "ifnull",
            "is_null",
            "coalesce",
        ]
    }

    #[async_recursion::async_recursion]
    async fn try_rewrite_scalar_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        match (func_name.to_lowercase().as_str(), args) {
            ("database" | "currentdatabase" | "current_database", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_current_database()),
                    },
                    None,
                )
                .await,
            ),
            ("version", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_fuse_version()),
                    },
                    None,
                )
                .await,
            ),
            ("user" | "currentuser" | "current_user", &[]) => match self.ctx.get_current_user() {
                Ok(user) => Some(
                    self.resolve(
                        &Expr::Literal {
                            span,
                            lit: Literal::String(user.identity().to_string()),
                        },
                        None,
                    )
                    .await,
                ),
                Err(e) => Some(Err(e)),
            },
            ("current_role", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(
                            self.ctx
                                .get_current_role()
                                .map(|r| r.name)
                                .unwrap_or_else(|| "".to_string()),
                        ),
                    },
                    None,
                )
                .await,
            ),
            ("connection_id", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_connection_id()),
                    },
                    None,
                )
                .await,
            ),
            ("timezone", &[]) => {
                let tz = self.ctx.get_settings().get_timezone().unwrap();
                Some(
                    self.resolve(
                        &Expr::Literal {
                            span,
                            lit: Literal::String(tz),
                        },
                        None,
                    )
                    .await,
                )
            }
            ("nullif", &[arg_x, arg_y]) => {
                // Rewrite nullif(x, y) to if(x = y, null, x)
                Some(
                    self.resolve_function(
                        span,
                        "if",
                        vec![],
                        &[
                            &Expr::BinaryOp {
                                span,
                                op: BinaryOperator::Eq,
                                left: Box::new(arg_x.clone()),
                                right: Box::new(arg_y.clone()),
                            },
                            &Expr::Literal {
                                span,
                                lit: Literal::Null,
                            },
                            arg_x,
                        ],
                        None,
                    )
                    .await,
                )
            }
            ("ifnull", &[arg_x, arg_y]) => {
                // Rewrite ifnull(x, y) to if(is_null(x), y, x)
                Some(
                    self.resolve_function(
                        span,
                        "if",
                        vec![],
                        &[
                            &Expr::IsNull {
                                span,
                                expr: Box::new(arg_x.clone()),
                                not: false,
                            },
                            arg_y,
                            arg_x,
                        ],
                        None,
                    )
                    .await,
                )
            }
            ("is_null", &[arg_x]) => {
                // Rewrite is_null(x) to not(is_not_null(x))
                Some(
                    self.resolve_unary_op(
                        span,
                        &UnaryOperator::Not,
                        &Expr::FunctionCall {
                            span,
                            distinct: false,
                            name: Identifier {
                                name: "is_not_null".to_string(),
                                quote: None,
                                span,
                            },
                            args: vec![arg_x.clone()],
                            params: vec![],
                        },
                        None,
                    )
                    .await,
                )
            }
            ("coalesce", args) => {
                // coalesce(arg0, arg1, ..., argN) is essentially
                // multi_if(is_not_null(arg0), assume_not_null(arg0), is_not_null(arg1), assume_not_null(arg1), ..., argN)
                // with constant Literal::Null arguments removed.
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);

                for arg in args.iter() {
                    if let Expr::Literal {
                        span: _,
                        lit: Literal::Null,
                    } = arg
                    {
                        continue;
                    }

                    let is_not_null_expr = Expr::IsNull {
                        span,
                        expr: Box::new((*arg).clone()),
                        not: true,
                    };

                    let assume_not_null_expr = Expr::FunctionCall {
                        span,
                        distinct: false,
                        name: Identifier {
                            name: "assume_not_null".to_string(),
                            quote: None,
                            span,
                        },
                        args: vec![(*arg).clone()],
                        params: vec![],
                    };

                    new_args.push(is_not_null_expr);
                    new_args.push(assume_not_null_expr);
                }
                new_args.push(Expr::Literal {
                    span,
                    lit: Literal::Null,
                });
                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(
                    self.resolve_function(span, "multi_if", vec![], &args_ref, None)
                        .await,
                )
            }

            _ => None,
        }
    }

    #[async_recursion::async_recursion]
    async fn resolve_trim_function(
        &mut self,
        span: Span,
        expr: &Expr,
        trim_where: &Option<(TrimWhere, Box<Expr>)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (func_name, trim_scalar, _trim_type) = if let Some((trim_type, trim_expr)) = trim_where
        {
            let func_name = match trim_type {
                TrimWhere::Leading => "trim_leading",
                TrimWhere::Trailing => "trim_trailing",
                TrimWhere::Both => "trim_both",
            };

            let box (trim_scalar, trim_type) = self.resolve(trim_expr, None).await?;
            (func_name, trim_scalar, trim_type)
        } else {
            let trim_scalar = ConstantExpr {
                value: common_expression::Literal::String(" ".as_bytes().to_vec()),
                data_type: Box::new(DataType::String),
            }
            .into();
            ("trim_both", trim_scalar, DataType::String)
        };

        let box (trim_source, _source_type) = self.resolve(expr, None).await?;
        let args = vec![trim_source, trim_scalar];

        self.resolve_scalar_function_call(span, func_name, vec![], args, None)
            .await
    }

    /// Resolve literal values.
    pub fn resolve_literal(
        &self,
        literal: &common_ast::ast::Literal,
        required_type: Option<DataType>,
    ) -> Result<Box<(common_expression::Literal, DataType)>> {
        let value = match literal {
            Literal::Integer(uint) => {
                // how to use match range?
                if *uint <= u8::MAX as u64 {
                    common_expression::Literal::UInt8(*uint as u8)
                } else if *uint <= u16::MAX as u64 {
                    common_expression::Literal::UInt16(*uint as u16)
                } else if *uint <= u32::MAX as u64 {
                    common_expression::Literal::UInt32(*uint as u32)
                } else {
                    common_expression::Literal::UInt64(*uint)
                }
            }
            Literal::Float(float) => common_expression::Literal::Float64(F64::from(*float)),
            Literal::String(string) => {
                common_expression::Literal::String(string.as_bytes().to_vec())
            }
            Literal::Boolean(boolean) => common_expression::Literal::Boolean(*boolean),
            Literal::Null => common_expression::Literal::Null,
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported literal value: {literal}"
            )))?,
        };
        let (scalar, data_type) = check_literal(&value);
        if let Some(required_type) = required_type {
            if required_type != data_type {
                let block = DataBlock::empty();
                let func_ctx = self.ctx.get_function_context()?;
                let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
                let src_expr = common_expression::Expr::Constant {
                    span: None,
                    scalar,
                    data_type,
                };
                let cast_expr = common_expression::Expr::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(src_expr),
                    dest_type: required_type.clone(),
                };
                let val = evaluator.run(&cast_expr).map_err(|_| {
                    ErrorCode::SemanticError(format!(
                        "Failed to cast to data type: {}",
                        required_type
                    ))
                })?;
                let required_val =
                    common_expression::Literal::try_from(val.into_scalar().unwrap())?;
                return Ok(Box::new((required_val, required_type)));
            }
        }

        Ok(Box::new((value, data_type)))
    }

    // TODO(leiysky): use an array builder function instead, since we should allow declaring
    // an array with variable as element.
    #[async_recursion::async_recursion]
    async fn resolve_array(
        &mut self,
        span: Span,
        exprs: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve(expr, None).await?;
            elems.push(arg);
        }

        self.resolve_scalar_function_call(span, "array", vec![], elems, None)
            .await
    }

    #[async_recursion::async_recursion]
    async fn resolve_tuple(
        &mut self,
        span: Span,
        exprs: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut args = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve(expr, None).await?;
            args.push(arg);
        }

        self.resolve_scalar_function_call(span, "tuple", vec![], args, None)
            .await
    }

    #[async_recursion::async_recursion]
    async fn resolve_udf(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let udf = UserApiProvider::instance()
            .get_udf(self.ctx.get_tenant().as_str(), func_name)
            .await;
        if let Ok(udf) = udf {
            let parameters = udf.parameters;
            if parameters.len() != arguments.len() {
                return Err(ErrorCode::SyntaxException(format!(
                    "Require {} parameters, but got: {}",
                    parameters.len(),
                    arguments.len()
                ))
                .set_span(span));
            }
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let backtrace = Backtrace::new();
            let sql_tokens = tokenize_sql(udf.definition.as_str())?;
            let expr = parse_expr(&sql_tokens, sql_dialect, &backtrace)?;
            let mut args_map = HashMap::new();
            arguments.iter().enumerate().for_each(|(idx, argument)| {
                if let Some(parameter) = parameters.get(idx) {
                    args_map.insert(parameter, (*argument).clone());
                }
            });
            let udf_expr = self
                .clone_expr_with_replacement(&expr, &|nest_expr| {
                    if let Expr::ColumnRef { column, .. } = nest_expr {
                        if let Some(arg) = args_map.get(&column.name) {
                            return Ok(Some(arg.clone()));
                        }
                    }
                    Ok(None)
                })
                .map_err(|e| e.set_span(span))?;
            self.resolve(&udf_expr, None).await
        } else {
            Err(ErrorCode::UnknownFunction(format!(
                "No function matches the given name: {func_name}"
            ))
            .set_span(span))
        }
    }

    #[async_recursion::async_recursion]
    async fn resolve_map_access(
        &mut self,
        expr: &Expr,
        mut paths: VecDeque<Literal>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (mut scalar, data_type) = self.resolve(expr, None).await?;
        let mut table_data_type = infer_schema_type(&data_type)?;
        // If it's map accessors to a tuple column, pushdown the map accessors to storage.
        if let Expr::ColumnRef { column: ident, .. } = expr {
            if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column }) = scalar {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn { data_type, .. } = column_entry {
                    table_data_type = data_type;
                    if let TableDataType::Tuple { .. } = table_data_type {
                        let box (inner_scalar, _inner_data_type) = self
                            .resolve_tuple_map_access_pushdown(
                                ident.span,
                                column.clone(),
                                &mut table_data_type,
                                &mut paths,
                            )
                            .await?;
                        scalar = inner_scalar;
                    }
                }
            }
        }

        // Otherwise, desugar it into a `get` function.
        while let Some(path_lit) = paths.pop_front() {
            table_data_type = table_data_type.remove_nullable();
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type
            {
                let idx = match path_lit {
                    Literal::Integer(idx) => {
                        if idx == 0 {
                            return Err(ErrorCode::SemanticError(
                                "tuple index is starting from 1, but 0 is found".to_string(),
                            ));
                        }
                        if idx as usize > fields_type.len() {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple index {} is out of bounds for length {}",
                                idx,
                                fields_type.len()
                            )));
                        }
                        table_data_type = fields_type.get(idx as usize - 1).unwrap().clone();
                        idx as usize
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => {
                            table_data_type = fields_type.get(idx).unwrap().clone();
                            idx + 1
                        }
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            )));
                        }
                    },
                    _ => unreachable!(),
                };
                scalar = FunctionCall {
                    params: vec![idx],
                    arguments: vec![scalar.clone()],
                    func_name: "get".to_string(),
                    return_type: Box::new(DataType::from(&table_data_type)),
                }
                .into();
                continue;
            }
            let box (path_value, path_data_type) = self.resolve_literal(&path_lit, None)?;
            let path_scalar: ScalarExpr = ConstantExpr {
                value: path_value,
                data_type: Box::new(path_data_type.clone()),
            }
            .into();
            if let TableDataType::Array(inner_type) = table_data_type {
                table_data_type = *inner_type;
            }
            table_data_type = TableDataType::wrap_nullable(&table_data_type);
            scalar = FunctionCall {
                params: vec![],
                arguments: vec![scalar.clone(), path_scalar],
                func_name: "get".to_string(),
                return_type: Box::new(DataType::from(&table_data_type)),
            }
            .into();
        }
        Ok(Box::new((scalar, DataType::from(&table_data_type))))
    }

    #[async_recursion::async_recursion]
    async fn resolve_tuple_map_access_pushdown(
        &mut self,
        span: Span,
        column: ColumnBinding,
        table_data_type: &mut TableDataType,
        paths: &mut VecDeque<Literal>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut names = Vec::new();
        names.push(column.column_name.clone());
        let mut index_with_types = VecDeque::with_capacity(paths.len());
        while paths.front().is_some() {
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type
            {
                let path = paths.pop_front().unwrap();
                match path {
                    Literal::Integer(idx) => {
                        if idx == 0 {
                            return Err(ErrorCode::SemanticError(
                                "tuple index is starting from 1, but 0 is found".to_string(),
                            ));
                        }
                        if idx as usize > fields_type.len() {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple index {} is out of bounds for length {}",
                                idx,
                                fields_type.len()
                            )));
                        }
                        let inner_name = fields_name.get(idx as usize - 1).unwrap();
                        let inner_type = fields_type.get(idx as usize - 1).unwrap();
                        names.push(inner_name.clone());
                        index_with_types.push_back((idx as usize, inner_type.clone()));
                        *table_data_type = inner_type.clone();
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => {
                            let inner_name = fields_name.get(idx).unwrap();
                            let inner_type = fields_type.get(idx).unwrap();
                            names.push(inner_name.clone());
                            index_with_types.push_back((idx + 1, inner_type.clone()));
                            *table_data_type = inner_type.clone();
                        }
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            )));
                        }
                    },
                    _ => unreachable!(),
                }
            } else {
                // other data types use `get` function.
                break;
            };
        }

        let inner_column_name = names.join(":");
        match self.bind_context.resolve_name(
            column.database_name.as_deref(),
            column.table_name.as_deref(),
            inner_column_name.as_str(),
            span,
            self.aliases,
        ) {
            Ok(result) => {
                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        let data_type = *column.data_type.clone();
                        (BoundColumnRef { column }.into(), data_type)
                    }
                    NameResolutionResult::Alias { scalar, .. } => {
                        (scalar.clone(), scalar.data_type())
                    }
                };
                Ok(Box::new((scalar, data_type)))
            }
            Err(_) => {
                // inner column is not exist in view, desugar it into a `get` function.
                let mut scalar: ScalarExpr = BoundColumnRef { column }.into();
                while let Some((idx, table_data_type)) = index_with_types.pop_front() {
                    scalar = FunctionCall {
                        params: vec![idx],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                        return_type: Box::new(DataType::from(&table_data_type)),
                    }
                    .into();
                }
                let return_type = scalar.data_type();
                Ok(Box::new((scalar, return_type)))
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn clone_expr_with_replacement<F>(
        &self,
        original_expr: &Expr,
        replacement_fn: &F,
    ) -> Result<Expr>
    where
        F: Fn(&Expr) -> Result<Option<Expr>>,
    {
        let replacement_opt = replacement_fn(original_expr)?;
        match replacement_opt {
            Some(replacement) => Ok(replacement),
            None => match original_expr {
                Expr::IsNull { span, expr, not } => Ok(Expr::IsNull {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    not: *not,
                }),
                Expr::InList {
                    span,
                    expr,
                    list,
                    not,
                } => Ok(Expr::InList {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    list: list
                        .iter()
                        .map(|item| self.clone_expr_with_replacement(item, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    not: *not,
                }),
                Expr::Between {
                    span,
                    expr,
                    low,
                    high,
                    not,
                } => Ok(Expr::Between {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    low: Box::new(self.clone_expr_with_replacement(low.as_ref(), replacement_fn)?),
                    high: Box::new(
                        self.clone_expr_with_replacement(high.as_ref(), replacement_fn)?,
                    ),
                    not: *not,
                }),
                Expr::BinaryOp {
                    span,
                    op,
                    left,
                    right,
                } => Ok(Expr::BinaryOp {
                    span: *span,
                    op: op.clone(),
                    left: Box::new(
                        self.clone_expr_with_replacement(left.as_ref(), replacement_fn)?,
                    ),
                    right: Box::new(
                        self.clone_expr_with_replacement(right.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::UnaryOp { span, op, expr } => Ok(Expr::UnaryOp {
                    span: *span,
                    op: op.clone(),
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Cast {
                    span,
                    expr,
                    target_type,
                    pg_style,
                } => Ok(Expr::Cast {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    target_type: target_type.clone(),
                    pg_style: *pg_style,
                }),
                Expr::TryCast {
                    span,
                    expr,
                    target_type,
                } => Ok(Expr::TryCast {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    target_type: target_type.clone(),
                }),
                Expr::Extract { span, kind, expr } => Ok(Expr::Extract {
                    span: *span,
                    kind: *kind,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Position {
                    span,
                    substr_expr,
                    str_expr,
                } => Ok(Expr::Position {
                    span: *span,
                    substr_expr: Box::new(
                        self.clone_expr_with_replacement(substr_expr.as_ref(), replacement_fn)?,
                    ),
                    str_expr: Box::new(
                        self.clone_expr_with_replacement(str_expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Substring {
                    span,
                    expr,
                    substring_from,
                    substring_for,
                } => Ok(Expr::Substring {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    substring_from: Box::new(
                        self.clone_expr_with_replacement(substring_from.as_ref(), replacement_fn)?,
                    ),
                    substring_for: if let Some(substring_for_expr) = substring_for {
                        Some(Box::new(self.clone_expr_with_replacement(
                            substring_for_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                Expr::Trim {
                    span,
                    expr,
                    trim_where,
                } => {
                    Ok(Expr::Trim {
                        span: *span,
                        expr: Box::new(
                            self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                        ),
                        trim_where: if let Some((trim, trim_expr)) = trim_where {
                            Some((
                                trim.clone(),
                                Box::new(self.clone_expr_with_replacement(
                                    trim_expr.as_ref(),
                                    replacement_fn,
                                )?),
                            ))
                        } else {
                            None
                        },
                    })
                }
                Expr::Tuple { span, exprs } => Ok(Expr::Tuple {
                    span: *span,
                    exprs: exprs
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::FunctionCall {
                    span,
                    distinct,
                    name,
                    args,
                    params,
                } => Ok(Expr::FunctionCall {
                    span: *span,
                    distinct: *distinct,
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|arg| self.clone_expr_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    params: params.clone(),
                }),
                Expr::Case {
                    span,
                    operand,
                    conditions,
                    results,
                    else_result,
                } => Ok(Expr::Case {
                    span: *span,
                    operand: if let Some(operand_expr) = operand {
                        Some(Box::new(self.clone_expr_with_replacement(
                            operand_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    conditions: conditions
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    results: results
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    else_result: if let Some(else_result_expr) = else_result {
                        Some(Box::new(self.clone_expr_with_replacement(
                            else_result_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                Expr::MapAccess {
                    span,
                    expr,
                    accessor,
                } => Ok(Expr::MapAccess {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    accessor: accessor.clone(),
                }),
                Expr::Array { span, exprs } => Ok(Expr::Array {
                    span: *span,
                    exprs: exprs
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::Interval { span, expr, unit } => Ok(Expr::Interval {
                    span: *span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    unit: *unit,
                }),
                Expr::DateAdd {
                    span,
                    unit,
                    interval,
                    date,
                } => Ok(Expr::DateAdd {
                    span: *span,
                    unit: *unit,
                    interval: Box::new(
                        self.clone_expr_with_replacement(interval.as_ref(), replacement_fn)?,
                    ),
                    date: Box::new(
                        self.clone_expr_with_replacement(date.as_ref(), replacement_fn)?,
                    ),
                }),
                _ => Ok(original_expr.clone()),
            },
        }
    }

    fn function_need_collation(&self, name: &str, args: &[ScalarExpr]) -> bool {
        let names = vec!["substr", "substring", "length"];
        !args.is_empty()
            && matches!(args[0].data_type().remove_nullable(), DataType::String)
            && self.ctx.get_settings().get_collation().unwrap() != "binary"
            && names.contains(&name)
    }

    pub fn resolve_type_name(type_name: &TypeName) -> Result<TableDataType> {
        let data_type = match type_name {
            TypeName::Boolean => TableDataType::Boolean,
            TypeName::UInt8 => TableDataType::Number(NumberDataType::UInt8),
            TypeName::UInt16 => TableDataType::Number(NumberDataType::UInt16),
            TypeName::UInt32 => TableDataType::Number(NumberDataType::UInt32),
            TypeName::UInt64 => TableDataType::Number(NumberDataType::UInt64),
            TypeName::Int8 => TableDataType::Number(NumberDataType::Int8),
            TypeName::Int16 => TableDataType::Number(NumberDataType::Int16),
            TypeName::Int32 => TableDataType::Number(NumberDataType::Int32),
            TypeName::Int64 => TableDataType::Number(NumberDataType::Int64),
            TypeName::Float32 => TableDataType::Number(NumberDataType::Float32),
            TypeName::Float64 => TableDataType::Number(NumberDataType::Float64),
            TypeName::Decimal { precision, scale } => {
                TableDataType::Decimal(DecimalDataType::from_size(DecimalSize {
                    precision: *precision,
                    scale: *scale,
                })?)
            }
            TypeName::String => TableDataType::String,
            TypeName::Timestamp => TableDataType::Timestamp,
            TypeName::Date => TableDataType::Date,
            TypeName::Array {
                item_type: Some(item_type),
            } => TableDataType::Array(Box::new(Self::resolve_type_name(item_type)?)),
            TypeName::Tuple {
                fields_type,
                fields_name,
            } => TableDataType::Tuple {
                fields_name: match fields_name {
                    None => (0..fields_type.len())
                        .into_iter()
                        .map(|i| i.to_string())
                        .collect(),
                    Some(names) => names.clone(),
                },
                fields_type: fields_type
                    .iter()
                    .map(Self::resolve_type_name)
                    .collect::<Result<Vec<_>>>()?,
            },
            TypeName::Nullable(inner_type) => {
                TableDataType::Nullable(Box::new(Self::resolve_type_name(inner_type)?))
            }
            TypeName::Variant => TableDataType::Variant,
            name => {
                return Err(ErrorCode::Internal(format!(
                    "Invalid type name \'{:?}\'",
                    name
                )));
            }
        };

        Ok(data_type)
    }
}

pub fn validate_function_arg(
    name: &str,
    args_len: usize,
    variadic_arguments: Option<(usize, usize)>,
    num_arguments: usize,
) -> Result<()> {
    match variadic_arguments {
        Some((start, end)) => {
            if args_len < start || args_len > end {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have [{}, {}] arguments, but got {}",
                    name, start, end, args_len
                )))
            } else {
                Ok(())
            }
        }
        None => {
            if num_arguments != args_len {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have {} arguments, but got {}",
                    name, num_arguments, args_len
                )))
            } else {
                Ok(())
            }
        }
    }
}
