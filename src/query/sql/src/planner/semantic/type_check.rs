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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::vec;

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IntervalKind as ASTIntervalKind;
use databend_common_ast::ast::Lambda;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::SubqueryModifier;
use databend_common_ast::ast::TrimWhere;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::ast::Weekday as ASTWeekday;
use databend_common_ast::ast::Window;
use databend_common_ast::ast::WindowFrame;
use databend_common_ast::ast::WindowFrameBound;
use databend_common_ast::ast::WindowFrameUnits;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::Span;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::InvertedIndexOption;
use databend_common_catalog::table_context::TableContext;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::display::display_tuple_field_name;
use databend_common_expression::infer_schema_type;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::decimal::DecimalDataType;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::decimal::MAX_DECIMAL128_PRECISION;
use databend_common_expression::types::decimal::MAX_DECIMAL256_PRECISION;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::F32;
use databend_common_expression::ColumnIndex;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionKind;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_SEARCH_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::RANK_WINDOW_FUNCTIONS;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_storage::init_stage_operator;
use databend_common_users::UserApiProvider;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use itertools::Itertools;
use jsonb::keypath::KeyPath;
use jsonb::keypath::KeyPaths;
use simsearch::SimSearch;
use unicase::Ascii;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use crate::binder::bind_values;
use crate::binder::resolve_file_location;
use crate::binder::wrap_cast;
use crate::binder::Binder;
use crate::binder::ExprContext;
use crate::binder::InternalColumnBinding;
use crate::binder::NameResolutionResult;
use crate::field_default_value;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::parse_lambda_expr;
use crate::planner::metadata::optimize_remove_count_args;
use crate::planner::semantic::lowering::TypeCheck;
use crate::planner::udf_validator::UDFValidator;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::AggregateMode;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::DictGetFunctionArgument;
use crate::plans::DictionarySource;
use crate::plans::FunctionCall;
use crate::plans::LagLeadFunction;
use crate::plans::LambdaFunc;
use crate::plans::NthValueFunction;
use crate::plans::NtileFunction;
use crate::plans::RedisSource;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SqlSource;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::UDFField;
use crate::plans::UDFLambdaCall;
use crate::plans::UDFScriptCode;
use crate::plans::UDFType;
use crate::plans::Visitor as ScalarVisitor;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

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
        })
    }

    #[allow(dead_code)]
    fn post_resolve(
        &mut self,
        scalar: &ScalarExpr,
        data_type: &DataType,
    ) -> Result<(ScalarExpr, DataType)> {
        Ok((scalar.clone(), data_type.clone()))
    }

    #[recursive::recursive]
    pub fn resolve(&mut self, expr: &Expr) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (scalar, data_type): Box<(ScalarExpr, DataType)> = match expr {
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

                Box::new((scalar, data_type))
            }

            Expr::IsNull {
                span, expr, not, ..
            } => {
                let args = &[expr.as_ref()];
                if *not {
                    self.resolve_function(*span, "is_not_null", vec![], args)?
                } else {
                    self.resolve_function(*span, "is_null", vec![], args)?
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
                let (scalar, _) = *self.resolve_function(*span, "if", vec![], &[
                    &Expr::BinaryOp {
                        span: *span,
                        op: BinaryOperator::And,
                        left: left_null_expr.clone(),
                        right: right_null_expr.clone(),
                    },
                    &Expr::Literal {
                        span: *span,
                        value: Literal::Boolean(*not),
                    },
                    &Expr::BinaryOp {
                        span: *span,
                        op: BinaryOperator::Or,
                        left: left_null_expr.clone(),
                        right: right_null_expr.clone(),
                    },
                    &Expr::Literal {
                        span: *span,
                        value: Literal::Boolean(!*not),
                    },
                    &Expr::BinaryOp {
                        span: *span,
                        op,
                        left: left.clone(),
                        right: right.clone(),
                    },
                ])?;
                self.resolve_scalar_function_call(*span, "assume_not_null", vec![], vec![scalar])?
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
                if list.len() > get_max_inlist_to_or && list.iter().all(satisfy_contain_func) {
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
                        })?
                    } else {
                        self.resolve_function(*span, "contains", vec![], &args)?
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
                    self.resolve(&result)?
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
                    let (ge_func, _left_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Gte,
                        expr.as_ref(),
                        low.as_ref(),
                    )?;
                    let (le_func, _right_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Lte,
                        expr.as_ref(),
                        high.as_ref(),
                    )?;

                    self.resolve_scalar_function_call(*span, "and", vec![], vec![
                        ge_func.clone(),
                        le_func.clone(),
                    ])?
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let (lt_func, _left_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Lt,
                        expr.as_ref(),
                        low.as_ref(),
                    )?;
                    let (gt_func, _right_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Gt,
                        expr.as_ref(),
                        high.as_ref(),
                    )?;

                    self.resolve_scalar_function_call(*span, "or", vec![], vec![lt_func, gt_func])?
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
                    subquery,
                    modifier: Some(subquery_modifier),
                    ..
                } = &**right
                {
                    match subquery_modifier {
                        SubqueryModifier::Any | SubqueryModifier::Some => {
                            let comparison_op = ComparisonOp::try_from(op)?;
                            self.resolve_subquery(
                                SubqueryType::Any,
                                subquery,
                                Some(*left.clone()),
                                Some(comparison_op),
                            )?
                        }
                        SubqueryModifier::All => {
                            let contrary_op = op.to_contrary()?;
                            let rewritten_subquery = Expr::Subquery {
                                span: right.span(),
                                modifier: Some(SubqueryModifier::Any),
                                subquery: (*subquery).clone(),
                            };
                            self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::BinaryOp {
                                span: *span,
                                op: contrary_op,
                                left: (*left).clone(),
                                right: Box::new(rewritten_subquery),
                            })?
                        }
                    }
                } else {
                    self.resolve_binary_op(*span, op, left.as_ref(), right.as_ref())?
                }
            }

            Expr::JsonOp {
                span,
                op,
                left,
                right,
            } => {
                let func_name = op.to_func_name();
                self.resolve_function(*span, func_name.as_str(), vec![], &[left, right])?
            }

            Expr::UnaryOp { span, op, expr, .. } => {
                self.resolve_unary_op(*span, op, expr.as_ref())?
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr)?;
                if target_type == &TypeName::Variant {
                    if let Some(result) =
                        self.resolve_cast_to_variant(expr.span(), &data_type, &scalar, false)
                    {
                        return result;
                    }
                }

                let raw_expr = RawExpr::Cast {
                    span: expr.span(),
                    is_try: false,
                    expr: Box::new(scalar.as_raw_expr()),
                    dest_type: DataType::from(&resolve_type_name(target_type, true)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let checked_expr = type_check::check(&raw_expr, registry)?;

                if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
                    return Ok(constant);
                }

                // cast variant to other type should nest wrap nullable,
                // as we cast JSON null to SQL NULL.
                let target_type = if data_type.remove_nullable() == DataType::Variant {
                    checked_expr.data_type().nest_wrap_nullable()
                // if the source type is nullable, cast target type should also be nullable.
                } else if data_type.is_nullable_or_null() {
                    checked_expr.data_type().wrap_nullable()
                } else {
                    checked_expr.data_type().clone()
                };

                Box::new((
                    CastExpr {
                        span: expr.span(),
                        is_try: false,
                        argument: Box::new(scalar),
                        target_type: Box::new(target_type.clone()),
                    }
                    .into(),
                    target_type,
                ))
            }

            Expr::TryCast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr)?;
                if target_type == &TypeName::Variant {
                    if let Some(result) =
                        self.resolve_cast_to_variant(expr.span(), &data_type, &scalar, true)
                    {
                        return result;
                    }
                }

                let raw_expr = RawExpr::Cast {
                    span: expr.span(),
                    is_try: true,
                    expr: Box::new(scalar.as_raw_expr()),
                    dest_type: DataType::from(&resolve_type_name(target_type, true)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let checked_expr = type_check::check(&raw_expr, registry)?;

                if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
                    return Ok(constant);
                }

                // cast variant to other type should nest wrap nullable,
                // as we cast JSON null to SQL NULL.
                let target_type = if data_type.remove_nullable() == DataType::Variant {
                    checked_expr.data_type().nest_wrap_nullable()
                } else {
                    checked_expr.data_type().clone()
                };
                Box::new((
                    CastExpr {
                        span: expr.span(),
                        is_try: true,
                        argument: Box::new(scalar),
                        target_type: Box::new(target_type.clone()),
                    }
                    .into(),
                    target_type,
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
                                func: ASTFunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(*span, "eq"),
                                    args: vec![*operand.clone(), c.clone()],
                                    params: vec![],
                                    order_by: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            };
                            arguments.push(equal_expr)
                        }
                        None => arguments.push(c.clone()),
                    }
                    arguments.push(r.clone());
                }
                let null_arg = Expr::Literal {
                    span: None,
                    value: Literal::Null,
                };

                if let Some(expr) = else_result {
                    arguments.push(*expr.clone());
                } else {
                    arguments.push(null_arg)
                }
                let args_ref: Vec<&Expr> = arguments.iter().collect();

                self.resolve_function(*span, "if", vec![], &args_ref)?
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
                self.resolve_function(*span, "substring", vec![], &arguments)?
            }

            Expr::Literal { span, value } => self.resolve_literal(*span, value)?,

            Expr::FunctionCall {
                span,
                func:
                    ASTFunctionCall {
                        distinct,
                        name,
                        args,
                        params,
                        order_by,
                        window,
                        lambda,
                    },
            } => {
                let func_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
                let func_name = func_name.as_str();
                let uni_case_func_name = Ascii::new(func_name);
                if !is_builtin_function(func_name)
                    && !Self::all_sugar_functions().contains(&uni_case_func_name)
                {
                    if let Some(udf) = self.resolve_udf(*span, func_name, args)? {
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

                let args: Vec<&Expr> = args.iter().collect();

                if GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name) {
                    // general window function
                    if window.is_none() {
                        return Err(ErrorCode::SemanticError(format!(
                            "window function {func_name} can only be used in window clause"
                        ))
                        .set_span(*span));
                    }
                    let window = window.as_ref().unwrap();
                    if !RANK_WINDOW_FUNCTIONS.contains(&func_name) && window.ignore_nulls.is_some()
                    {
                        return Err(ErrorCode::SemanticError(format!(
                            "window function {func_name} not support IGNORE/RESPECT NULLS option"
                        ))
                        .set_span(*span));
                    }
                    let func = self.resolve_general_window_function(
                        *span,
                        func_name,
                        &args,
                        &window.ignore_nulls,
                    )?;
                    let display_name = format!("{:#}", expr);
                    self.resolve_window(*span, display_name, &window.window, func)?
                } else if AggregateFunctionFactory::instance().contains(func_name) {
                    let mut new_params = Vec::with_capacity(params.len());
                    for param in params {
                        let box (scalar, _data_type) = self.resolve(param)?;
                        let expr = scalar.as_expr()?;
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let constant = expr
                            .into_constant()
                            .map_err(|_| {
                                ErrorCode::SemanticError(format!(
                                    "invalid parameter {param} for aggregate function, expected constant",
                                ))
                                    .set_span(*span)
                            })?
                            .1;
                        new_params.push(constant);
                    }
                    let in_window = self.in_window_function;
                    self.in_window_function = self.in_window_function || window.is_some();
                    let in_aggregate_function = self.in_aggregate_function;
                    let (new_agg_func, data_type) = self.resolve_aggregate_function(
                        *span, func_name, expr, *distinct, new_params, &args, order_by,
                    )?;
                    self.in_window_function = in_window;
                    self.in_aggregate_function = in_aggregate_function;
                    if let Some(window) = window {
                        // aggregate window function
                        let display_name = format!("{:#}", expr);
                        if window.ignore_nulls.is_some() {
                            return Err(ErrorCode::SemanticError(format!(
                                "window function {func_name} not support IGNORE/RESPECT NULLS option"
                            ))
                                .set_span(*span));
                        }
                        // general window function
                        let func = WindowFuncType::Aggregate(new_agg_func);
                        self.resolve_window(*span, display_name, &window.window, func)?
                    } else {
                        // aggregate function
                        Box::new((new_agg_func.into(), data_type))
                    }
                } else if GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
                    if lambda.is_none() {
                        return Err(ErrorCode::SemanticError(format!(
                            "function {func_name} must have a lambda expression",
                        ))
                        .set_span(*span));
                    }
                    let lambda = lambda.as_ref().unwrap();
                    self.resolve_lambda_function(*span, func_name, &args, lambda)?
                } else if GENERAL_SEARCH_FUNCTIONS.contains(&uni_case_func_name) {
                    match func_name.to_lowercase().as_str() {
                        "score" => self.resolve_score_search_function(*span, func_name, &args)?,
                        "match" => self.resolve_match_search_function(*span, func_name, &args)?,
                        "query" => self.resolve_query_search_function(*span, func_name, &args)?,
                        _ => {
                            return Err(ErrorCode::SemanticError(format!(
                                "cannot find search function {}",
                                func_name
                            ))
                            .set_span(*span));
                        }
                    }
                } else if ASYNC_FUNCTIONS.contains(&uni_case_func_name) {
                    self.resolve_async_function(*span, func_name, &args)?
                } else if BUILTIN_FUNCTIONS
                    .get_property(func_name)
                    .map(|property| property.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    // Set returning function
                    self.resolve_set_returning_function(*span, func_name, &args)?
                } else {
                    // Scalar function
                    let mut new_params: Vec<Scalar> = Vec::with_capacity(params.len());
                    for param in params {
                        let box (scalar, _data_type) = self.resolve(param)?;
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
                            .1;
                        new_params.push(constant);
                    }
                    self.resolve_function(*span, func_name, new_params, &args)?
                }
            }

            Expr::CountAll { span, window } => {
                let (new_agg_func, data_type) =
                    self.resolve_aggregate_function(*span, "count", expr, false, vec![], &[], &[])?;

                if let Some(window) = window {
                    // aggregate window function
                    let display_name = format!("{:#}", expr);
                    let func = WindowFuncType::Aggregate(new_agg_func);
                    self.resolve_window(*span, display_name, window, func)?
                } else {
                    // aggregate function
                    Box::new((new_agg_func.into(), data_type))
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
            )?,

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, None, None)?
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
                    Some(ComparisonOp::Equal),
                )?
            }

            expr @ Expr::MapAccess { .. } => {
                let mut expr = expr;
                let mut paths = VecDeque::new();
                while let Expr::MapAccess {
                    span,
                    expr: inner_expr,
                    accessor,
                } = expr
                {
                    expr = &**inner_expr;
                    let path = match accessor {
                        MapAccessor::Bracket {
                            key: box Expr::Literal { value, .. },
                        } => {
                            if !matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                                return Err(ErrorCode::SemanticError(format!(
                                    "Unsupported accessor: {:?}",
                                    value
                                ))
                                .set_span(*span));
                            }
                            value.clone()
                        }
                        MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                        MapAccessor::DotNumber { key } => Literal::UInt64(*key),
                        _ => {
                            return Err(ErrorCode::SemanticError(format!(
                                "Unsupported accessor: {:?}",
                                accessor
                            ))
                            .set_span(*span));
                        }
                    };
                    paths.push_front((*span, path));
                }
                self.resolve_map_access(expr, paths)?
            }

            Expr::Extract {
                span, kind, expr, ..
            } => self.resolve_extract_expr(*span, kind, expr)?,

            Expr::DatePart {
                span, kind, expr, ..
            } => self.resolve_extract_expr(*span, kind, expr)?,

            Expr::Interval { span, .. } => {
                return Err(ErrorCode::SemanticError(
                    "Unsupported interval expression yet".to_string(),
                )
                .set_span(*span));
            }
            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_arith(*span, unit, interval, date, false)?,
            Expr::DateDiff {
                span,
                unit,
                date_start,
                date_end,
                ..
            } => self.resolve_date_arith(*span, unit, date_start, date_end, true)?,
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_arith(
                *span,
                unit,
                &Expr::UnaryOp {
                    span: *span,
                    op: UnaryOperator::Minus,
                    expr: interval.clone(),
                },
                date,
                false,
            )?,
            Expr::DateTrunc {
                span, unit, date, ..
            } => self.resolve_date_trunc(*span, date, unit)?,
            Expr::LastDay {
                span, unit, date, ..
            } => self.resolve_last_day(*span, date, unit)?,
            Expr::PreviousDay {
                span, unit, date, ..
            } => self.resolve_previous_or_next_day(*span, date, unit, true)?,
            Expr::NextDay {
                span, unit, date, ..
            } => self.resolve_previous_or_next_day(*span, date, unit, false)?,
            Expr::Trim {
                span,
                expr,
                trim_where,
                ..
            } => self.resolve_trim_function(*span, expr, trim_where)?,

            Expr::Array { span, exprs, .. } => self.resolve_array(*span, exprs)?,

            Expr::Position {
                substr_expr,
                str_expr,
                span,
                ..
            } => self.resolve_function(*span, "locate", vec![], &[
                substr_expr.as_ref(),
                str_expr.as_ref(),
            ])?,

            Expr::Map { span, kvs, .. } => self.resolve_map(*span, kvs)?,

            Expr::Tuple { span, exprs, .. } => self.resolve_tuple(*span, exprs)?,

            Expr::Hole { span, .. } | Expr::Placeholder { span } => {
                return Err(ErrorCode::SemanticError(
                    "Hole or Placeholder expression is impossible in trivial query".to_string(),
                )
                .set_span(*span))
            }
        };
        Ok(Box::new((scalar, data_type)))
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

    fn resolve_window(
        &mut self,
        span: Span,
        display_name: String,
        window: &Window,
        func: WindowFuncType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if self.in_aggregate_function {
            // Reset the state
            self.in_aggregate_function = false;
            return Err(ErrorCode::SemanticError(
                "aggregate function calls cannot contain window function calls".to_string(),
            )
            .set_span(span));
        }
        if self.in_window_function {
            // Reset the state
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        let spec = match window {
            Window::WindowSpec(spec) => spec.clone(),
            Window::WindowReference(w) => self
                .bind_context
                .window_definitions
                .get(&w.window_name.name)
                .ok_or_else(|| {
                    ErrorCode::SyntaxException(format!(
                        "Window definition {} not found",
                        w.window_name.name
                    ))
                })?
                .value()
                .clone(),
        };

        self.in_window_function = true;
        let mut partitions = Vec::with_capacity(spec.partition_by.len());
        for p in spec.partition_by.iter() {
            let box (part, _part_type) = self.resolve(p)?;
            partitions.push(part);
        }

        let mut order_by = Vec::with_capacity(spec.order_by.len());
        for o in spec.order_by.iter() {
            let box (order, _) = self.resolve(&o.expr)?;

            if matches!(order, ScalarExpr::ConstantExpr(_)) {
                continue;
            }

            order_by.push(WindowOrderBy {
                expr: order,
                asc: o.asc,
                nulls_first: o.nulls_first,
            })
        }
        self.in_window_function = false;

        let frame =
            self.resolve_window_frame(span, &func, &mut order_by, spec.window_frame.clone())?;

        if matches!(&frame.start_bound, WindowFuncFrameBound::Following(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame start cannot be UNBOUNDED FOLLOWING".to_string(),
            )
            .set_span(span));
        }

        if matches!(&frame.end_bound, WindowFuncFrameBound::Preceding(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame end cannot be UNBOUNDED PRECEDING".to_string(),
            )
            .set_span(span));
        }

        let data_type = func.return_type();
        let window_func = WindowFunc {
            span,
            display_name,
            func,
            partition_by: partitions,
            order_by,
            frame,
        };
        Ok(Box::new((window_func.into(), data_type)))
    }

    // just support integer
    #[inline]
    fn resolve_rows_offset(&self, expr: &Expr) -> Result<Scalar> {
        if let Expr::Literal { value, .. } = expr {
            let box (value, _) = self.resolve_literal_scalar(value)?;
            match value {
                Scalar::Number(NumberScalar::UInt8(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt16(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt32(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt64(_)) => return Ok(value),
                _ => {}
            }
        }

        Err(ErrorCode::SemanticError(
            "Only unsigned numbers are allowed in ROWS offset".to_string(),
        )
        .set_span(expr.span()))
    }

    #[inline]
    fn resolve_literal(
        &self,
        span: Span,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (value, data_type) = self.resolve_literal_scalar(literal)?;

        let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
        Ok(Box::new((scalar_expr, data_type)))
    }

    fn resolve_window_rows_frame(&self, frame: WindowFrame) -> Result<WindowFuncFrame> {
        let units = match frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match frame.start_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Preceding(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Preceding(None)
                }
            }
            WindowFrameBound::Following(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Following(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Following(None)
                }
            }
        };
        let end = match frame.end_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Preceding(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Preceding(None)
                }
            }
            WindowFrameBound::Following(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Following(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Following(None)
                }
            }
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_range_offset(&mut self, bound: &WindowFrameBound) -> Result<Option<Scalar>> {
        match bound {
            WindowFrameBound::Following(Some(box expr))
            | WindowFrameBound::Preceding(Some(box expr)) => {
                let box (expr, _) = self.resolve(expr)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr.as_expr()?, &self.func_ctx, &BUILTIN_FUNCTIONS);
                if let EExpr::Constant { scalar, .. } = expr {
                    Ok(Some(scalar))
                } else {
                    Err(ErrorCode::SemanticError(
                        "Only constant is allowed in RANGE offset".to_string(),
                    )
                    .set_span(expr.span()))
                }
            }
            _ => Ok(None),
        }
    }

    fn resolve_window_range_frame(&mut self, frame: WindowFrame) -> Result<WindowFuncFrame> {
        let start_offset = self.resolve_range_offset(&frame.start_bound)?;
        let end_offset = self.resolve_range_offset(&frame.end_bound)?;

        let units = match frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match frame.start_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(start_offset),
            WindowFrameBound::Following(_) => WindowFuncFrameBound::Following(start_offset),
        };
        let end = match frame.end_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(end_offset),
            WindowFrameBound::Following(_) => WindowFuncFrameBound::Following(end_offset),
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_window_frame(
        &mut self,
        span: Span,
        func: &WindowFuncType,
        order_by: &mut [WindowOrderBy],
        window_frame: Option<WindowFrame>,
    ) -> Result<WindowFuncFrame> {
        match func {
            WindowFuncType::PercentRank => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::LagLead(lag_lead) if lag_lead.is_lag => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::LagLead(lag_lead) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::Ntile(_) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::CumeDist => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Range,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            _ => {}
        }
        if let Some(frame) = window_frame {
            if frame.units.is_range() {
                if order_by.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "The RANGE OFFSET window frame requires exactly one ORDER BY column, {} given.",
                        order_by.len()
                    )).set_span(span));
                }
                self.resolve_window_range_frame(frame)
            } else {
                self.resolve_window_rows_frame(frame)
            }
        } else if order_by.is_empty() {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::Following(None),
            })
        } else {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::CurrentRow,
            })
        }
    }

    /// Resolve general window function call.
    fn resolve_general_window_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
        window_ignore_null: &Option<bool>,
    ) -> Result<WindowFuncType> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "window functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InSetReturningFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "window functions can not be used in set-returning function".to_string(),
            )
            .set_span(span));
        }
        // try to resolve window function without arguments first
        if let Ok(window_func) = WindowFuncType::from_name(func_name) {
            return Ok(window_func);
        }

        if self.in_window_function {
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        self.in_window_function = true;
        let mut arguments = vec![];
        let mut arg_types = vec![];
        for arg in args.iter() {
            let box (argument, arg_type) = self.resolve(arg)?;
            arguments.push(argument);
            arg_types.push(arg_type);
        }
        self.in_window_function = false;

        // If { IGNORE | RESPECT } NULLS is not specified, the default is RESPECT NULLS
        // (i.e. a NULL value will be returned if the expression contains a NULL value, and it is the first value in the expression).
        let ignore_null = if let Some(ignore_null) = window_ignore_null {
            *ignore_null
        } else {
            false
        };

        match func_name {
            "lag" | "lead" => {
                self.resolve_lag_lead_window_function(func_name, &arguments, &arg_types)
            }
            "first_value" | "first" | "last_value" | "last" | "nth_value" => self
                .resolve_nth_value_window_function(func_name, &arguments, &arg_types, ignore_null),
            "ntile" => self.resolve_ntile_window_function(&arguments),
            _ => Err(ErrorCode::UnknownFunction(format!(
                "Unknown window function: {func_name}"
            ))),
        }
    }

    fn resolve_lag_lead_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
    ) -> Result<WindowFuncType> {
        if args.is_empty() || args.len() > 3 {
            return Err(ErrorCode::InvalidArgument(format!(
                "Function {:?} only support 1 to 3 arguments",
                func_name
            )));
        }

        let offset = if args.len() >= 2 {
            let off = args[1].as_expr()?;
            match off {
                EExpr::Constant { .. } => Some(check_number::<_, i64>(
                    off.span(),
                    &self.func_ctx,
                    &off,
                    &BUILTIN_FUNCTIONS,
                )?),
                _ => {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The second argument to the function {:?} must be a constant",
                        func_name
                    )));
                }
            }
        } else {
            None
        };

        let offset = offset.unwrap_or(1);

        let is_lag = match func_name {
            "lag" if offset < 0 => false,
            "lead" if offset < 0 => true,
            "lag" => true,
            "lead" => false,
            _ => unreachable!(),
        };

        let (default, return_type) = if args.len() == 3 {
            (Some(args[2].clone()), arg_types[0].clone())
        } else {
            (None, arg_types[0].wrap_nullable())
        };

        let cast_default = default.map(|d| {
            Box::new(ScalarExpr::CastExpr(CastExpr {
                span: d.span(),
                is_try: false,
                argument: Box::new(d),
                target_type: Box::new(return_type.clone()),
            }))
        });

        Ok(WindowFuncType::LagLead(LagLeadFunction {
            is_lag,
            arg: Box::new(args[0].clone()),
            offset: offset.unsigned_abs(),
            default: cast_default,
            return_type: Box::new(return_type),
        }))
    }

    fn resolve_nth_value_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
        ignore_null: bool,
    ) -> Result<WindowFuncType> {
        Ok(match func_name {
            "first_value" | "first" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(1),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            "last_value" | "last" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: None,
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            _ => {
                // nth_value
                if args.len() != 2 {
                    return Err(ErrorCode::InvalidArgument(
                        "The function nth_value must take two arguments".to_string(),
                    ));
                }
                let return_type = arg_types[0].wrap_nullable();
                let n_expr = args[1].as_expr()?;
                let n = match n_expr {
                    EExpr::Constant { .. } => check_number::<_, u64>(
                        n_expr.span(),
                        &self.func_ctx,
                        &n_expr,
                        &BUILTIN_FUNCTIONS,
                    )?,
                    _ => {
                        return Err(ErrorCode::InvalidArgument(
                            "The count of `nth_value` must be constant positive integer",
                        ));
                    }
                };
                if n == 0 {
                    return Err(ErrorCode::InvalidArgument(
                        "nth_value should count from 1".to_string(),
                    ));
                }

                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(n),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
        })
    }

    fn resolve_ntile_window_function(&mut self, args: &[ScalarExpr]) -> Result<WindowFuncType> {
        if args.len() != 1 {
            return Err(ErrorCode::InvalidArgument(
                "Function ntile can only take one argument".to_string(),
            ));
        }
        let n_expr = args[0].as_expr()?;
        let return_type = DataType::Number(NumberDataType::UInt64);
        let n = match n_expr {
            EExpr::Constant { .. } => {
                check_number::<_, u64>(n_expr.span(), &self.func_ctx, &n_expr, &BUILTIN_FUNCTIONS)?
            }
            _ => {
                return Err(ErrorCode::InvalidArgument(
                    "The argument of `ntile` must be constant".to_string(),
                ));
            }
        };
        if n == 0 {
            return Err(ErrorCode::InvalidArgument(
                "ntile buckets must be greater than 0".to_string(),
            ));
        }

        Ok(WindowFuncType::Ntile(NtileFunction {
            n,
            return_type: Box::new(return_type),
        }))
    }

    /// Resolve aggregation function call.
    fn resolve_aggregate_function(
        &mut self,
        span: Span,
        func_name: &str,
        expr: &Expr,
        distinct: bool,
        params: Vec<Scalar>,
        args: &[&Expr],
        order_by: &[OrderByExpr],
    ) -> Result<(AggregateFunction, DataType)> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "aggregate functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }

        if self.in_aggregate_function {
            if self.in_window_function {
                // The aggregate function can be in window function call,
                // but it cannot be nested.
                // E.g. `select sum(sum(x)) over (partition by y) from t group by y;` is allowed.
                // But `select sum(sum(sum(x))) from t;` is not allowed.
                self.in_window_function = false;
            } else {
                // Reset the state
                self.in_aggregate_function = false;
                return Err(ErrorCode::SemanticError(
                    "aggregate function calls cannot be nested".to_string(),
                )
                .set_span(expr.span()));
            }
        }

        // Check aggregate function
        self.in_aggregate_function = true;
        let mut arguments = vec![];
        let mut arg_types = vec![];
        for arg in args.iter() {
            let box (argument, arg_type) = self.resolve(arg)?;
            arguments.push(argument);
            arg_types.push(arg_type);
        }
        self.in_aggregate_function = false;

        let sort_descs = order_by
            .iter()
            .map(
                |OrderByExpr {
                     expr,
                     asc,
                     nulls_first,
                 }| {
                    let box (scalar_expr, _) = self.resolve(expr)?;

                    Ok(AggregateFunctionScalarSortDesc {
                        expr: scalar_expr,
                        is_reuse_index: false,
                        nulls_first: nulls_first.unwrap_or(false),
                        asc: asc.unwrap_or(true),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        // Convert the delimiter of string_agg to params
        let params = if (func_name.eq_ignore_ascii_case("string_agg")
            || func_name.eq_ignore_ascii_case("listagg")
            || func_name.eq_ignore_ascii_case("group_concat"))
            && arguments.len() == 2
            && params.is_empty()
        {
            let delimiter_value = ConstantExpr::try_from(arguments[1].clone());
            if arg_types[1] != DataType::String || delimiter_value.is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "The delimiter of `{func_name}` must be a constant string"
                )));
            }
            let _ = arguments.pop();
            let _ = arg_types.pop();
            let delimiter = delimiter_value.unwrap();
            vec![delimiter.value]
        } else {
            params
        };

        // Convert the num_buckets of histogram to params
        let params = if func_name.eq_ignore_ascii_case("histogram")
            && arguments.len() == 2
            && params.is_empty()
        {
            let max_num_buckets: u64 = check_number(
                None,
                &FunctionContext::default(),
                &arguments[1].as_expr()?,
                &BUILTIN_FUNCTIONS,
            )?;

            vec![Scalar::Number(NumberScalar::UInt64(max_num_buckets))]
        } else {
            params
        };

        // Rewrite `xxx(distinct)` to `xxx_distinct(...)`
        let (func_name, distinct) = if func_name.eq_ignore_ascii_case("count") && distinct {
            ("count_distinct", false)
        } else {
            (func_name, distinct)
        };

        let func_name = if distinct {
            format!("{}_distinct", func_name)
        } else {
            func_name.to_string()
        };

        let agg_func = AggregateFunctionFactory::instance()
            .get(&func_name, params.clone(), arg_types, vec![])
            .map_err(|e| e.set_span(span))?;

        let args = if optimize_remove_count_args(&func_name, distinct, args) {
            vec![]
        } else {
            arguments
        };

        let display_name = format!("{:#}", expr);
        let new_agg_func = AggregateFunction {
            span,
            display_name,
            func_name,
            distinct: false,
            params,
            args,
            return_type: Box::new(agg_func.return_type()?),
            sort_descs,
        };

        let data_type = agg_func.return_type()?;

        Ok((new_agg_func, data_type))
    }

    fn transform_to_max_type(&self, ty: &DataType) -> Result<DataType> {
        let max_ty = match ty.remove_nullable() {
            DataType::Number(s) => {
                if s.is_float() {
                    DataType::Number(NumberDataType::Float64)
                } else {
                    DataType::Number(NumberDataType::Int64)
                }
            }
            DataType::Decimal(DecimalDataType::Decimal128(s)) => {
                let p = MAX_DECIMAL128_PRECISION;
                let decimal_size = DecimalSize {
                    precision: p,
                    scale: s.scale,
                };
                DataType::Decimal(DecimalDataType::from_size(decimal_size)?)
            }
            DataType::Decimal(DecimalDataType::Decimal256(s)) => {
                let p = MAX_DECIMAL256_PRECISION;
                let decimal_size = DecimalSize {
                    precision: p,
                    scale: s.scale,
                };
                DataType::Decimal(DecimalDataType::from_size(decimal_size)?)
            }
            DataType::Null => DataType::Null,
            DataType::Binary => DataType::Binary,
            DataType::String => DataType::String,
            DataType::Variant => DataType::Variant,
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "array_reduce does not support type '{:?}'",
                    ty
                )));
            }
        };

        if ty.is_nullable() {
            Ok(max_ty.wrap_nullable())
        } else {
            Ok(max_ty)
        }
    }

    fn resolve_lambda_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
        lambda: &Lambda,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "lambda functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }

        if args.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for lambda function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let box (mut arg, mut arg_type) = self.resolve(args[0])?;

        let mut func_name = func_name;
        let mut is_cast_variant = false;
        if arg_type.remove_nullable() == DataType::Variant {
            if func_name.starts_with("json_") {
                func_name = &func_name[5..];
            }
            // Try auto cast the Variant type to Array(Variant) or Map(String, Variant),
            // so that the lambda functions support variant type as argument.
            let mut target_type = if func_name.starts_with("array") {
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Variant))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    DataType::String,
                    DataType::Nullable(Box::new(DataType::Variant)),
                ])))
            };
            if arg_type.is_nullable() {
                target_type = target_type.wrap_nullable();
            }

            arg = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(arg.clone()),
                target_type: Box::new(target_type.clone()),
            });
            arg_type = target_type;

            is_cast_variant = true;
        }

        let params = lambda
            .params
            .iter()
            .map(|param| param.name.to_lowercase())
            .collect::<Vec<_>>();

        self.check_lambda_param_count(func_name, params.len(), span)?;

        let inner_ty = match arg_type.remove_nullable() {
            DataType::Array(box inner_ty) => inner_ty.clone(),
            DataType::Map(box inner_ty) => inner_ty.clone(),
            DataType::Null | DataType::EmptyArray | DataType::EmptyMap => DataType::Null,
            _ => {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for lambda function, argument data type must be an array or map"
                        .to_string(),
                )
                .set_span(span));
            }
        };

        let inner_tys = if func_name == "array_reduce" {
            let max_ty = self.transform_to_max_type(&inner_ty)?;
            vec![max_ty.clone(), max_ty.clone()]
        } else if func_name == "map_filter"
            || func_name == "map_transform_keys"
            || func_name == "map_transform_values"
        {
            match &inner_ty {
                DataType::Null => {
                    vec![DataType::Null, DataType::Null]
                }
                DataType::Tuple(t) => t.clone(),
                _ => unreachable!(),
            }
        } else {
            vec![inner_ty.clone()]
        };

        let lambda_columns = params
            .iter()
            .zip(inner_tys.iter())
            .map(|(col, ty)| (col.clone(), ty.clone()))
            .collect::<Vec<_>>();

        let mut lambda_context = self.bind_context.clone();
        let box (lambda_expr, lambda_type) = parse_lambda_expr(
            self.ctx.clone(),
            &mut lambda_context,
            &lambda_columns,
            &lambda.expr,
        )?;

        let return_type = if func_name == "array_filter" || func_name == "map_filter" {
            if lambda_type.remove_nullable() == DataType::Boolean {
                arg_type.clone()
            } else {
                return Err(ErrorCode::SemanticError(
                    format!("invalid lambda function for `{}`, the result data type of lambda function must be boolean", func_name)
                )
                .set_span(span));
            }
        } else if func_name == "array_reduce" {
            // transform arg type
            let max_ty = inner_tys[0].clone();
            let target_type = if arg_type.is_nullable() {
                Box::new(DataType::Nullable(Box::new(DataType::Array(Box::new(
                    max_ty.clone(),
                )))))
            } else {
                Box::new(DataType::Array(Box::new(max_ty.clone())))
            };
            // we should convert arg to max_ty to avoid overflow in 'ADD'/'SUB',
            // so if arg_type(origin_type) != target_type(max_type), cast arg
            // for example, if arg = [1INT8, 2INT8, 3INT8], after cast it be [1INT64, 2INT64, 3INT64]
            if arg_type != *target_type {
                arg = ScalarExpr::CastExpr(CastExpr {
                    span: arg.span(),
                    is_try: false,
                    argument: Box::new(arg),
                    target_type,
                });
            }
            max_ty.wrap_nullable()
        } else if func_name == "map_transform_keys" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))
            }
        } else if func_name == "map_transform_values" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))
            }
        } else if arg_type.is_nullable() {
            DataType::Nullable(Box::new(DataType::Array(Box::new(lambda_type.clone()))))
        } else {
            DataType::Array(Box::new(lambda_type.clone()))
        };

        let (lambda_func, data_type) = match arg_type.remove_nullable() {
            // Null and Empty array can convert to ConstantExpr
            DataType::Null => (
                ConstantExpr {
                    span,
                    value: Scalar::Null,
                }
                .into(),
                DataType::Null,
            ),
            DataType::EmptyArray => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyArray,
                }
                .into(),
                DataType::EmptyArray,
            ),
            DataType::EmptyMap => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyMap,
                }
                .into(),
                DataType::EmptyMap,
            ),
            _ => {
                struct LambdaVisitor<'a> {
                    bind_context: &'a BindContext,
                    arg_index: HashSet<IndexType>,
                    args: Vec<ScalarExpr>,
                    fields: Vec<DataField>,
                }

                impl<'a> ScalarVisitor<'a> for LambdaVisitor<'a> {
                    fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                        if self.arg_index.contains(&col.column.index) {
                            return Ok(());
                        }
                        self.arg_index.insert(col.column.index);
                        let is_outer_column = self
                            .bind_context
                            .all_column_bindings()
                            .iter()
                            .map(|c| c.index)
                            .contains(&col.column.index);
                        if is_outer_column {
                            let arg = ScalarExpr::BoundColumnRef(col.clone());
                            self.args.push(arg);
                            let field = DataField::new(
                                &format!("{}", col.column.index),
                                *col.column.data_type.clone(),
                            );
                            self.fields.push(field);
                        }
                        Ok(())
                    }
                }

                // Collect outer scope columns as arguments first.
                let mut lambda_visitor = LambdaVisitor {
                    bind_context: self.bind_context,
                    arg_index: HashSet::new(),
                    args: Vec::new(),
                    fields: Vec::new(),
                };
                lambda_visitor.visit(&lambda_expr)?;

                let mut lambda_args = mem::take(&mut lambda_visitor.args);
                lambda_args.push(arg);
                let mut lambda_fields = mem::take(&mut lambda_visitor.fields);
                // Add lambda columns as arguments at end.
                for (lambda_column_name, lambda_column_type) in lambda_columns.into_iter() {
                    for column in lambda_context.all_column_bindings().iter().rev() {
                        if column.column_name == lambda_column_name {
                            let lambda_field =
                                DataField::new(&format!("{}", column.index), lambda_column_type);
                            lambda_fields.push(lambda_field);
                            break;
                        }
                    }
                }
                let lambda_schema = DataSchema::new(lambda_fields);
                let expr = lambda_expr
                    .type_check(&lambda_schema)?
                    .project_column_ref(|index| {
                        lambda_schema.index_of(&index.to_string()).unwrap()
                    });
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let remote_lambda_expr = expr.as_remote_expr();
                let lambda_display = format!("{:?} -> {}", params, expr.sql_display());

                (
                    LambdaFunc {
                        span,
                        func_name: func_name.to_string(),
                        args: lambda_args,
                        lambda_expr: Box::new(remote_lambda_expr),
                        lambda_display,
                        return_type: Box::new(return_type.clone()),
                    }
                    .into(),
                    return_type,
                )
            }
        };

        if is_cast_variant {
            let result_target_type = if data_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Variant))
            } else {
                DataType::Variant
            };
            let result_target_scalar = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(lambda_func),
                target_type: Box::new(result_target_type.clone()),
            });
            Ok(Box::new((result_target_scalar, result_target_type)))
        } else {
            Ok(Box::new((lambda_func, data_type)))
        }
    }

    fn check_lambda_param_count(
        &mut self,
        func_name: &str,
        param_count: usize,
        span: Span,
    ) -> Result<()> {
        // json lambda functions are cast to array or map, ignored here.
        let expected_count = if func_name == "array_reduce" {
            2
        } else if func_name.starts_with("array") {
            1
        } else if func_name.starts_with("map") {
            2
        } else {
            unreachable!()
        };

        if param_count != expected_count {
            return Err(ErrorCode::SemanticError(format!(
                "incorrect number of parameters in lambda function, {} expects {} parameter(s), but got {}",
                func_name, expected_count, param_count
            ))
            .set_span(span));
        }
        Ok(())
    }

    fn resolve_score_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !args.is_empty() {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 0 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let internal_column =
            InternalColumn::new(SEARCH_SCORE_COL_NAME, InternalColumnType::SearchScore);

        let internal_column_binding = InternalColumnBinding {
            database_name: None,
            table_name: None,
            internal_column,
        };
        let column = self.bind_context.add_internal_column_binding(
            &internal_column_binding,
            self.metadata.clone(),
            None,
            false,
        )?;

        let scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span, column });
        let data_type = DataType::Number(NumberDataType::Float32);
        Ok(Box::new((scalar_expr, data_type)))
    }

    /// Resolve match search function.
    /// The first argument is the field or fields to match against,
    /// multiple fields can have a optional per-field boosting that
    /// gives preferential weight to fields being searched in.
    /// For example: title^5, content^1.2
    /// The scond argument is the query text without query syntax.
    fn resolve_match_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !matches!(self.bind_context.expr_context, ExprContext::WhereClause) {
            return Err(ErrorCode::SemanticError(format!(
                "search function {} can only be used in where clause",
                func_name
            ))
            .set_span(span));
        }

        // The optional third argument is additional configuration option.
        if args.len() != 2 && args.len() != 3 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 2 or 3 arguments, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }

        let field_arg = args[0];
        let query_arg = args[1];
        let option_arg = if args.len() == 3 { Some(args[2]) } else { None };

        let box (field_scalar, _) = self.resolve(field_arg)?;
        let column_refs = match field_scalar {
            // single field without boost
            ScalarExpr::BoundColumnRef(column_ref) => {
                vec![(column_ref, None)]
            }
            // constant multiple fields with boosts
            ScalarExpr::ConstantExpr(constant_expr) => {
                let Some(constant_field) = constant_expr.value.as_string() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid arguments for search function, field must be a column or constant string, but got {}",
                        constant_expr.value
                    ))
                    .set_span(constant_expr.span));
                };

                // fields are separated by commas and boost is separated by ^
                let field_strs: Vec<&str> = constant_field.split(',').collect();
                let mut column_refs = Vec::with_capacity(field_strs.len());
                for field_str in field_strs {
                    let field_boosts: Vec<&str> = field_str.split('^').collect();
                    if field_boosts.len() > 2 {
                        return Err(ErrorCode::SemanticError(format!(
                            "invalid arguments for search function, field string must have only one boost, but got {}",
                            constant_field
                        ))
                        .set_span(constant_expr.span));
                    }
                    let column_expr = Expr::ColumnRef {
                        span: constant_expr.span,
                        column: ColumnRef {
                            database: None,
                            table: None,
                            column: ColumnID::Name(Identifier::from_name(
                                constant_expr.span,
                                field_boosts[0].trim(),
                            )),
                        },
                    };
                    let box (field_scalar, _) = self.resolve(&column_expr)?;
                    let Ok(column_ref) = BoundColumnRef::try_from(field_scalar) else {
                        return Err(ErrorCode::SemanticError(
                            "invalid arguments for search function, field must be a column"
                                .to_string(),
                        )
                        .set_span(constant_expr.span));
                    };
                    let boost = if field_boosts.len() == 2 {
                        match f32::from_str(field_boosts[1].trim()) {
                            Ok(boost) => Some(F32::from(boost)),
                            Err(_) => {
                                return Err(ErrorCode::SemanticError(format!(
                                    "invalid arguments for search function, boost must be a float value, but got {}",
                                    field_boosts[1]
                                ))
                                .set_span(constant_expr.span));
                            }
                        }
                    } else {
                        None
                    };
                    column_refs.push((column_ref, boost));
                }
                column_refs
            }
            _ => {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for search function, field must be a column or constant string".to_string(),
                )
                .set_span(span));
            }
        };

        let box (query_scalar, _) = self.resolve(query_arg)?;
        let Ok(query_expr) = ConstantExpr::try_from(query_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };
        let Some(query_text) = query_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };

        let inverted_index_option = self.resolve_search_option(option_arg)?;

        self.resolve_search_function(span, column_refs, query_text, inverted_index_option)
    }

    /// Resolve query search function.
    /// The first argument query text with query syntax.
    /// The following query syntax is supported:
    /// 1. simple terms, like `title:quick`
    /// 2. bool operator terms, like `title:fox AND dog OR cat`
    /// 3. must and negative operator terms, like `title:+fox -cat`
    /// 4. phrase terms, like `title:"quick brown fox"`
    /// 5. multiple field with boost terms, like `title:fox^5 content:dog^2`
    fn resolve_query_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !matches!(self.bind_context.expr_context, ExprContext::WhereClause) {
            return Err(ErrorCode::SemanticError(format!(
                "search function {} can only be used in where clause",
                func_name
            ))
            .set_span(span));
        }

        // The optional second argument is additional configuration option.
        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }

        let query_arg = args[0];
        let option_arg = if args.len() == 2 { Some(args[1]) } else { None };

        let box (query_scalar, _) = self.resolve(query_arg)?;
        let Ok(query_expr) = ConstantExpr::try_from(query_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };
        let Some(query_text) = query_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };

        let field_strs: Vec<&str> = query_text.split(' ').collect();
        let mut column_refs = Vec::with_capacity(field_strs.len());
        for field_str in field_strs {
            if !field_str.contains(':') {
                continue;
            }
            let field_names: Vec<&str> = field_str.split(':').collect();
            // if the field is JSON type, must specify the key path in the object
            // for example:
            // the field `info` has the value: `{"tags":{"id":10,"env":"prod","name":"test"}}`
            // a query can be written like this `info.tags.env:prod`
            let field_name = field_names[0].trim();
            let sub_field_names: Vec<&str> = field_name.split('.').collect();
            let column_expr = Expr::ColumnRef {
                span: query_scalar.span(),
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(Identifier::from_name(
                        query_scalar.span(),
                        sub_field_names[0].trim(),
                    )),
                },
            };
            let box (field_scalar, _) = self.resolve(&column_expr)?;
            let Ok(column_ref) = BoundColumnRef::try_from(field_scalar) else {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for search function, field must be a column".to_string(),
                )
                .set_span(query_scalar.span()));
            };
            column_refs.push((column_ref, None));
        }
        let inverted_index_option = self.resolve_search_option(option_arg)?;

        self.resolve_search_function(span, column_refs, query_text, inverted_index_option)
    }

    fn resolve_search_option(
        &mut self,
        option_arg: Option<&Expr>,
    ) -> Result<Option<InvertedIndexOption>> {
        if let Some(option_arg) = option_arg {
            let box (option_scalar, _) = self.resolve(option_arg)?;
            let Ok(option_expr) = ConstantExpr::try_from(option_scalar.clone()) else {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, option must be a constant string, but got {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            };
            let Some(option_text) = option_expr.value.as_string() else {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, option text must be a constant string, but got {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            };

            let mut lenient = None;
            let mut operator = None;
            let mut fuzziness = None;

            // additional configuration options are separated by semicolon `;`
            let option_strs: Vec<&str> = option_text.split(';').collect();
            for option_str in option_strs {
                if option_str.trim().is_empty() {
                    continue;
                }
                let option_vals: Vec<&str> = option_str.split('=').collect();
                if option_vals.len() != 2 {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid arguments for search function, each option must have key and value joined by equal sign, but got {}",
                        option_arg
                    ))
                    .set_span(option_scalar.span()));
                }
                let option_key = option_vals[0].trim().to_lowercase();
                let option_val = option_vals[1].trim().to_lowercase();
                match option_key.as_str() {
                    "fuzziness" => {
                        // fuzziness is only support 1 and 2 currently.
                        if fuzziness.is_none() {
                            if option_val == "1" {
                                fuzziness = Some(1);
                                continue;
                            } else if option_val == "2" {
                                fuzziness = Some(2);
                                continue;
                            }
                        }
                    }
                    "operator" => {
                        if operator.is_none() {
                            if option_val == "or" {
                                operator = Some(false);
                                continue;
                            } else if option_val == "and" {
                                operator = Some(true);
                                continue;
                            }
                        }
                    }
                    "lenient" => {
                        if lenient.is_none() {
                            if option_val == "false" {
                                lenient = Some(false);
                                continue;
                            } else if option_val == "true" {
                                lenient = Some(true);
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, unsupported option: {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            }

            let inverted_index_option = InvertedIndexOption {
                lenient: lenient.unwrap_or_default(),
                operator: operator.unwrap_or_default(),
                fuzziness,
            };

            return Ok(Some(inverted_index_option));
        }
        Ok(None)
    }

    fn resolve_search_function(
        &mut self,
        span: Span,
        column_refs: Vec<(BoundColumnRef, Option<F32>)>,
        query_text: &String,
        inverted_index_option: Option<InvertedIndexOption>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if column_refs.is_empty() {
            return Err(ErrorCode::SemanticError(
                "invalid arguments for search function, must specify at least one search column"
                    .to_string(),
            )
            .set_span(span));
        }
        if !column_refs.windows(2).all(|c| {
            c[0].0.column.table_index.is_some()
                && c[0].0.column.table_index == c[1].0.column.table_index
        }) {
            return Err(ErrorCode::SemanticError(
                "invalid arguments for search function, all columns must in a table".to_string(),
            )
            .set_span(span));
        }
        let table_index = column_refs[0].0.column.table_index.unwrap();

        let table_entry = self.metadata.read().table(table_index).clone();
        let table = table_entry.table();
        let table_info = table.get_table_info();
        let table_schema = table_info.schema();
        let table_indexes = &table_info.meta.indexes;

        let mut query_fields = Vec::with_capacity(column_refs.len());
        let mut column_ids = Vec::with_capacity(column_refs.len());
        for (column_ref, boost) in &column_refs {
            let column_name = &column_ref.column.column_name;
            let column_id = table_schema.column_id_of(column_name)?;
            column_ids.push(column_id);
            query_fields.push((column_name.clone(), *boost));
        }

        // find inverted index and check schema
        let mut index_name = "".to_string();
        let mut index_version = "".to_string();
        let mut index_schema = None;
        let mut index_options = BTreeMap::new();
        for table_index in table_indexes.values() {
            if column_ids
                .iter()
                .all(|id| table_index.column_ids.contains(id))
            {
                index_name = table_index.name.clone();
                index_version = table_index.version.clone();

                let mut index_fields = Vec::with_capacity(table_index.column_ids.len());
                for column_id in &table_index.column_ids {
                    let table_field = table_schema.field_of_column_id(*column_id)?;
                    let field = DataField::from(table_field);
                    index_fields.push(field);
                }
                index_schema = Some(DataSchema::new(index_fields));
                index_options = table_index.options.clone();
                break;
            }
        }

        if index_schema.is_none() {
            let column_names = query_fields.iter().map(|c| c.0.clone()).join(", ");
            return Err(ErrorCode::SemanticError(format!(
                "columns {} don't have inverted index",
                column_names
            ))
            .set_span(span));
        }

        if self
            .bind_context
            .inverted_index_map
            .contains_key(&table_index)
        {
            return Err(ErrorCode::SemanticError(format!(
                "duplicate search function for table {table_index}"
            ))
            .set_span(span));
        }
        let index_info = InvertedIndexInfo {
            index_name,
            index_version,
            index_options,
            index_schema: index_schema.unwrap(),
            query_fields,
            query_text: query_text.to_string(),
            has_score: false,
            inverted_index_option,
        };

        self.bind_context
            .inverted_index_map
            .insert(table_index, index_info);

        let internal_column =
            InternalColumn::new(SEARCH_MATCHED_COL_NAME, InternalColumnType::SearchMatched);

        let internal_column_binding = InternalColumnBinding {
            database_name: column_refs[0].0.column.database_name.clone(),
            table_name: column_refs[0].0.column.table_name.clone(),
            internal_column,
        };
        let column = self.bind_context.add_internal_column_binding(
            &internal_column_binding,
            self.metadata.clone(),
            None,
            false,
        )?;

        let scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span, column });
        let data_type = DataType::Boolean;
        Ok(Box::new((scalar_expr, data_type)))
    }

    /// Resolve set returning function.
    pub fn resolve_set_returning_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match self.bind_context.expr_context {
            ExprContext::InSetReturningFunction => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be nested".to_string(),
                )
                .set_span(span));
            }
            ExprContext::WhereClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions are not allowed in WHERE clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::HavingClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in HAVING clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::QualifyClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in QUALIFY clause".to_string(),
                )
                .set_span(span));
            }
            _ => {}
        }

        if self.in_window_function {
            return Err(ErrorCode::SemanticError(
                "set-returning functions cannot be used in window spec",
            )
            .set_span(span));
        }

        let original_context = self.bind_context.expr_context.clone();
        self.bind_context
            .set_expr_context(ExprContext::InSetReturningFunction);

        let mut arguments = Vec::with_capacity(args.len());
        for arg in args.iter() {
            let box (scalar, _) = self.resolve(arg)?;
            arguments.push(scalar);
        }

        // Restore the original context
        self.bind_context.set_expr_context(original_context);

        let srf_scalar = ScalarExpr::FunctionCall(FunctionCall {
            span,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
        });
        let srf_expr = srf_scalar.as_expr()?;
        let srf_tuple_types = srf_expr.data_type().as_tuple().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "The return type of srf should be tuple, but got {}",
                srf_expr.data_type()
            ))
        })?;

        // If tuple has more than one field, return the tuple column,
        // otherwise, extract the tuple field to top level column.
        let (return_scalar, return_type) = if srf_tuple_types.len() > 1 {
            (srf_scalar, srf_expr.data_type().clone())
        } else {
            let child_scalar = ScalarExpr::FunctionCall(FunctionCall {
                span,
                func_name: "get".to_string(),
                params: vec![Scalar::Number(NumberScalar::Int64(1))],
                arguments: vec![srf_scalar],
            });
            (child_scalar, srf_tuple_types[0].clone())
        };

        Ok(Box::new((return_scalar, return_type)))
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
        if let Some(rewritten_func_result) =
            self.try_rewrite_sugar_function(span, func_name, arguments)
        {
            return rewritten_func_result;
        }

        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let box (arg, mut arg_type) = self.resolve(argument)?;
            if let ScalarExpr::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type()?.is_nullable() {
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

        if func_name == "grouping" {
            // `grouping` will be rewritten again after resolving grouping sets.
            return Ok(Box::new((
                ScalarExpr::FunctionCall(FunctionCall {
                    span,
                    params: vec![],
                    arguments: args,
                    func_name: "grouping".to_string(),
                }),
                DataType::Number(NumberDataType::UInt32),
            )));
        }

        if let Some(rewritten_func_func) =
            self.try_rewrite_array_function(span, func_name, &params, &mut args, &mut arg_types)
        {
            return rewritten_func_func;
        }

        self.resolve_scalar_function_call(span, func_name, params, args)
    }

    pub fn resolve_scalar_function_call(
        &self,
        span: Span,
        func_name: &str,
        mut params: Vec<Scalar>,
        args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Type check
        let arguments = args.iter().map(|v| v.as_raw_expr()).collect::<Vec<_>>();

        // inject the params
        if ["round", "truncate"].contains(&func_name)
            && !args.is_empty()
            && params.is_empty()
            && args[0].data_type()?.remove_nullable().is_decimal()
        {
            let scale = if args.len() == 2 {
                let scalar_expr = &arguments[1];
                let expr = type_check::check(scalar_expr, &BUILTIN_FUNCTIONS)?;

                let scale = check_number::<_, i64>(
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
        }

        let raw_expr = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.clone(),
            args: arguments,
        };

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;

        // Run constant folding for arguments of the scalar function.
        // This will be helpful to simplify some constant expressions, especially
        // the implicitly casted literal values, e.g. `timestamp > '2001-01-01'`
        // will be folded from `timestamp > to_timestamp('2001-01-01')` to `timestamp > 978307200000000`
        // Note: check function may reorder the args

        let mut folded_args = match &expr {
            EExpr::FunctionCall {
                args: checked_args, ..
            } => {
                let mut folded_args = Vec::with_capacity(args.len());
                for (checked_arg, arg) in checked_args.iter().zip(args.iter()) {
                    match self.try_fold_constant(checked_arg, true) {
                        Some(constant) if arg.evaluable() => {
                            folded_args.push(constant.0);
                        }
                        _ => {
                            folded_args.push(arg.clone());
                        }
                    }
                }
                folded_args
            }
            _ => args,
        };

        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            self.ctx.set_cacheable(false);
        }

        if let Some(constant) = self.try_fold_constant(&expr, true) {
            return Ok(constant);
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
            BinaryOperator::NotLike | BinaryOperator::NotRegexp | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike => BinaryOperator::Like,
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

                self.resolve_scalar_function_call(
                    span,
                    &BinaryOperator::Eq.to_func_name(),
                    vec![],
                    vec![left, right],
                )
            }
            BinaryOperator::Like => {
                // Convert `Like` to compare function , such as `p_type like PROMO%` will be converted to `p_type >= PROMO and p_type < PROMP`
                if let Expr::Literal {
                    value: Literal::String(str),
                    ..
                } = right
                {
                    return self.resolve_like(op, span, left, right, str);
                }
                let name = op.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[left, right])
            }
            other => {
                let name = other.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[left, right])
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
            other => {
                let name = other.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[child])
            }
        }
    }

    pub fn resolve_extract_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        arg: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match interval_kind {
            ASTIntervalKind::Year => self.resolve_function(span, "to_year", vec![], &[arg]),
            ASTIntervalKind::Quarter => self.resolve_function(span, "to_quarter", vec![], &[arg]),
            ASTIntervalKind::Month => self.resolve_function(span, "to_month", vec![], &[arg]),
            ASTIntervalKind::Day => self.resolve_function(span, "to_day_of_month", vec![], &[arg]),
            ASTIntervalKind::Hour => self.resolve_function(span, "to_hour", vec![], &[arg]),
            ASTIntervalKind::Minute => self.resolve_function(span, "to_minute", vec![], &[arg]),
            ASTIntervalKind::Second => self.resolve_function(span, "to_second", vec![], &[arg]),
            ASTIntervalKind::Doy => self.resolve_function(span, "to_day_of_year", vec![], &[arg]),
            ASTIntervalKind::Dow => self.resolve_function(span, "to_day_of_week", vec![], &[arg]),
            ASTIntervalKind::Week => self.resolve_function(span, "to_week_of_year", vec![], &[arg]),
            ASTIntervalKind::Epoch => self.resolve_function(span, "epoch", vec![], &[arg]),
            ASTIntervalKind::MicroSecond => {
                self.resolve_function(span, "to_microsecond", vec![], &[arg])
            }
        }
    }

    pub fn resolve_date_arith(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        date_rhs: &Expr,
        date_lhs: &Expr,
        is_diff: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = if is_diff {
            format!("diff_{}s", interval_kind.to_string().to_lowercase())
        } else {
            format!("add_{}s", interval_kind.to_string().to_lowercase())
        };
        let mut args = vec![];
        let mut arg_types = vec![];

        let (date_lhs, date_lhs_type) = *self.resolve(date_lhs)?;
        args.push(date_lhs);
        arg_types.push(date_lhs_type);

        let (date_rhs, date_rhs_type) = *self.resolve(date_rhs)?;

        args.push(date_rhs);
        arg_types.push(date_rhs_type);

        self.resolve_scalar_function_call(span, &func_name, vec![], args)
    }

    pub fn resolve_date_trunc(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(
                    span,
                    "to_start_of_quarter", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Week => {
                self.resolve_function(
                    span,
                    "to_start_of_week", vec![],
                    &[date, &Expr::Literal {
                        span: None,
                        value: Literal::UInt64(1)
                    }],
                )
            }
            ASTIntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second", vec![],
                    &[date],
                )
            }
            _ => Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second]".to_string()).set_span(span)),
        }
    }

    pub fn resolve_last_day(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(span, "to_last_of_year", vec![], &[date])
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(span, "to_last_of_quarter", vec![], &[date])
            }
            ASTIntervalKind::Month => {
                self.resolve_function(span, "to_last_of_month", vec![], &[date])
            }
            ASTIntervalKind::Week => {
                self.resolve_function(span, "to_last_of_week", vec![], &[date])
            }
            _ => Err(ErrorCode::SemanticError(
                "Only these interval types are currently supported: [year, quarter, month, week]"
                    .to_string(),
            )
            .set_span(span)),
        }
    }

    pub fn resolve_previous_or_next_day(
        &mut self,
        span: Span,
        date: &Expr,
        weekday: &ASTWeekday,
        is_previous: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let prefix = if is_previous {
            "to_previous_"
        } else {
            "to_next_"
        };

        let func_name = match weekday {
            ASTWeekday::Monday => format!("{}monday", prefix),
            ASTWeekday::Tuesday => format!("{}tuesday", prefix),
            ASTWeekday::Wednesday => format!("{}wednesday", prefix),
            ASTWeekday::Thursday => format!("{}thursday", prefix),
            ASTWeekday::Friday => format!("{}friday", prefix),
            ASTWeekday::Saturday => format!("{}saturday", prefix),
            ASTWeekday::Sunday => format!("{}sunday", prefix),
        };

        self.resolve_function(span, &func_name, vec![], &[date])
    }

    pub fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query,
        child_expr: Option<Expr>,
        compare_op: Option<ComparisonOp>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
        let (s_expr, output_context) = binder.bind_query(&mut bind_context, subquery)?;
        self.bind_context
            .cte_context
            .set_cte_context_and_name(output_context.cte_context);

        if (typ == SubqueryType::Scalar || typ == SubqueryType::Any)
            && output_context.columns.len() > 1
        {
            return Err(ErrorCode::SemanticError(format!(
                "Subquery must return only one column, but got {} columns",
                output_context.columns.len()
            )));
        }

        let mut contain_agg = None;
        if let SetExpr::Select(select_stmt) = &subquery.body {
            if typ == SubqueryType::Scalar {
                let select = &select_stmt.select_list[0];
                if matches!(select, SelectTarget::AliasedExpr { .. }) {
                    // Check if contain aggregation function
                    #[derive(Visitor)]
                    #[visitor(Expr(enter), ASTFunctionCall(enter))]
                    struct AggFuncVisitor {
                        contain_agg: bool,
                    }
                    impl AggFuncVisitor {
                        fn enter_ast_function_call(&mut self, func: &ASTFunctionCall) {
                            self.contain_agg = self.contain_agg
                                || AggregateFunctionFactory::instance()
                                    .contains(func.name.to_string());
                        }
                        fn enter_expr(&mut self, expr: &Expr) {
                            self.contain_agg = self.contain_agg
                                || matches!(expr, Expr::CountAll { window: None, .. });
                        }
                    }
                    let mut visitor = AggFuncVisitor { contain_agg: false };
                    select.drive(&mut visitor);
                    contain_agg = Some(visitor.contain_agg);
                }
            }
        }

        let box mut data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        let mut child_scalar = None;
        if let Some(expr) = child_expr {
            assert_eq!(output_context.columns.len(), 1);
            let box (scalar, expr_ty) = self.resolve(&expr)?;
            child_scalar = Some(Box::new(scalar));
            // wrap nullable to make sure expr and list values have common type.
            if expr_ty.is_nullable() {
                data_type = data_type.wrap_nullable();
            }
        }

        if typ.eq(&SubqueryType::Scalar) {
            data_type = data_type.wrap_nullable();
        }
        let subquery_expr = SubqueryExpr {
            span: subquery.span,
            subquery: Box::new(s_expr),
            child_expr: child_scalar,
            compare_op,
            output_column: output_context.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg,
        };

        let data_type = subquery_expr.data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    pub fn all_sugar_functions() -> &'static [Ascii<&'static str>] {
        static FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("current_catalog"),
            Ascii::new("database"),
            Ascii::new("currentdatabase"),
            Ascii::new("current_database"),
            Ascii::new("version"),
            Ascii::new("user"),
            Ascii::new("currentuser"),
            Ascii::new("current_user"),
            Ascii::new("current_role"),
            Ascii::new("connection_id"),
            Ascii::new("timezone"),
            Ascii::new("nullif"),
            Ascii::new("ifnull"),
            Ascii::new("nvl"),
            Ascii::new("nvl2"),
            Ascii::new("is_null"),
            Ascii::new("is_error"),
            Ascii::new("error_or"),
            Ascii::new("coalesce"),
            Ascii::new("last_query_id"),
            Ascii::new("array_sort"),
            Ascii::new("array_aggregate"),
            Ascii::new("to_variant"),
            Ascii::new("try_to_variant"),
            Ascii::new("greatest"),
            Ascii::new("least"),
            Ascii::new("stream_has_data"),
            Ascii::new("getvariable"),
        ];
        FUNCTIONS
    }

    fn try_rewrite_sugar_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        match (func_name.to_lowercase().as_str(), args) {
            ("current_catalog", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_current_catalog()),
            })),
            ("database" | "currentdatabase" | "current_database", &[]) => {
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(self.ctx.get_current_database()),
                }))
            }
            ("version", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_fuse_version()),
            })),
            ("user" | "currentuser" | "current_user", &[]) => match self.ctx.get_current_user() {
                Ok(user) => Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(user.identity().display().to_string()),
                })),
                Err(e) => Some(Err(e)),
            },
            ("current_role", &[]) => Some(
                self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(
                        self.ctx
                            .get_current_role()
                            .map(|r| r.name)
                            .unwrap_or_default(),
                    ),
                }),
            ),
            ("connection_id", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_connection_id()),
            })),
            ("timezone", &[]) => {
                let tz = self.ctx.get_settings().get_timezone().unwrap();
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(tz),
                }))
            }
            ("nullif", &[arg_x, arg_y]) => {
                // Rewrite nullif(x, y) to if(x = y, null, x)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: Box::new(arg_x.clone()),
                        right: Box::new(arg_y.clone()),
                    },
                    &Expr::Literal {
                        span,
                        value: Literal::Null,
                    },
                    arg_x,
                ]))
            }
            ("ifnull" | "nvl", args) => {
                if args.len() == 2 {
                    // Rewrite ifnull(x, y) | nvl(x, y) to if(is_null(x), y, x)
                    Some(self.resolve_function(span, "if", vec![], &[
                        &Expr::IsNull {
                            span,
                            expr: Box::new(args[0].clone()),
                            not: false,
                        },
                        args[1],
                        args[0],
                    ]))
                } else {
                    // Rewrite ifnull(args) to coalesce(x, y)
                    // Rewrite nvl(args) to coalesce(args)
                    // nvl is essentially an alias for ifnull.
                    Some(self.resolve_function(span, "coalesce", vec![], args))
                }
            }
            ("nvl2", &[arg_x, arg_y, arg_z]) => {
                // Rewrite nvl2(x, y, z) to if(is_not_null(x), y, z)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::IsNull {
                        span,
                        expr: Box::new(arg_x.clone()),
                        not: true,
                    },
                    arg_y,
                    arg_z,
                ]))
            }
            ("is_null", &[arg_x]) => {
                // Rewrite is_null(x) to not(is_not_null(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_null"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("is_error", &[arg_x]) => {
                // Rewrite is_error(x) to not(is_not_error(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("error_or", args) => {
                // error_or(arg0, arg1, ..., argN) is essentially
                // if(is_not_error(arg0), arg0, is_not_error(arg1), arg1, ..., argN)
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);

                for arg in args.iter() {
                    let is_not_error = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                    new_args.push(is_not_error);
                    new_args.push((*arg).clone());
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("coalesce", args) => {
                // coalesce(arg0, arg1, ..., argN) is essentially
                // if(is_not_null(arg0), assume_not_null(arg0), is_not_null(arg1), assume_not_null(arg1), ..., argN)
                // with constant Literal::Null arguments removed.
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
                for arg in args.iter() {
                    if let Expr::Literal {
                        span: _,
                        value: Literal::Null,
                    } = arg
                    {
                        continue;
                    }

                    let is_not_null_expr = Expr::IsNull {
                        span,
                        expr: Box::new((*arg).clone()),
                        not: true,
                    };
                    if let Ok(res) = self.resolve(&is_not_null_expr) {
                        if let ScalarExpr::ConstantExpr(c) = res.0 {
                            if Scalar::Boolean(false) == c.value {
                                continue;
                            }
                        }
                    }

                    let assume_not_null_expr = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "assume_not_null"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };

                    new_args.push(is_not_null_expr);
                    new_args.push(assume_not_null_expr);
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                // coalesce(all_null) => null
                if new_args.len() == 1 {
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                }

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("last_query_id", args) => {
                // last_query_id(index) returns query_id in current session by index
                let res: Result<i64> = try {
                    if args.len() > 1 {
                        return Some(Err(ErrorCode::BadArguments(
                            "last_query_id needs at most one integer argument",
                        )
                        .set_span(span)));
                    }
                    if args.is_empty() {
                        -1
                    } else {
                        let box (scalar, _) = self.resolve(args[0])?;

                        let expr = scalar.as_expr()?;
                        match expr {
                            EExpr::Constant { .. } => check_number::<_, i64>(
                                span,
                                &self.func_ctx,
                                &expr,
                                &BUILTIN_FUNCTIONS,
                            )?,
                            _ => {
                                return Some(Err(ErrorCode::BadArguments(
                                    "last_query_id argument only support constant",
                                )
                                .set_span(span)));
                            }
                        }
                    }
                };

                Some(match res {
                    Ok(index) => {
                        let query_id = self.ctx.get_last_query_id(index as i32);
                        self.resolve(&Expr::Literal {
                            span,
                            value: Literal::String(query_id),
                        })
                    }
                    Err(e) => Err(e),
                })
            }
            ("array_sort", args) => {
                if args.is_empty() || args.len() > 3 {
                    return None;
                }
                let mut asc = true;
                let mut nulls_first = None;
                if args.len() >= 2 {
                    let box (arg, _) = self.resolve(args[1]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(sort_order) = arg.value {
                            if sort_order.eq_ignore_ascii_case("asc") {
                                asc = true;
                            } else if sort_order.eq_ignore_ascii_case("desc") {
                                asc = false;
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Sorting order must be either ASC or DESC",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Sorting order must be either ASC or DESC",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Sorting order must be a constant string",
                        )));
                    }
                }
                if args.len() == 3 {
                    let box (arg, _) = self.resolve(args[2]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(nulls_order) = arg.value {
                            if nulls_order.eq_ignore_ascii_case("nulls first") {
                                nulls_first = Some(true);
                            } else if nulls_order.eq_ignore_ascii_case("nulls last") {
                                nulls_first = Some(false);
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Null sorting order must be either NULLS FIRST or NULLS LAST",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Null sorting order must be either NULLS FIRST or NULLS LAST",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Null sorting order must be a constant string",
                        )));
                    }
                }

                let nulls_first = nulls_first.unwrap_or_else(|| {
                    let default_nulls_first = self.ctx.get_settings().get_nulls_first();
                    default_nulls_first(asc)
                });

                let func_name = match (asc, nulls_first) {
                    (true, true) => "array_sort_asc_null_first",
                    (false, true) => "array_sort_desc_null_first",
                    (true, false) => "array_sort_asc_null_last",
                    (false, false) => "array_sort_desc_null_last",
                };
                let args_ref: Vec<&Expr> = vec![args[0]];
                Some(self.resolve_function(span, func_name, vec![], &args_ref))
            }
            ("array_aggregate", args) => {
                if args.len() != 2 {
                    return None;
                }
                let box (arg, _) = self.resolve(args[1]).ok()?;
                if let Ok(arg) = ConstantExpr::try_from(arg) {
                    if let Scalar::String(aggr_func_name) = arg.value {
                        let func_name = format!("array_{}", aggr_func_name);
                        let args_ref: Vec<&Expr> = vec![args[0]];
                        return Some(self.resolve_function(span, &func_name, vec![], &args_ref));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Array aggregate function name be must a constant string",
                )))
            }
            ("to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, false)
            }
            ("try_to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, true)
            }
            ("greatest", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_max", vec![], vec![array]))
            }
            ("least", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_min", vec![], vec![array]))
            }
            ("getvariable", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, _) = self.resolve(args[0]).ok()?;

                if let Ok(arg) = ConstantExpr::try_from(scalar) {
                    if let Scalar::String(var_name) = arg.value {
                        let var_value = self.ctx.get_variable(&var_name).unwrap_or(Scalar::Null);
                        let var_value = shrink_scalar(var_value);
                        let data_type = var_value.as_ref().infer_data_type();
                        return Some(Ok(Box::new((
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span,
                                value: var_value,
                            }),
                            data_type,
                        ))));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Variable name must be a constant string",
                )))
            }
            _ => None,
        }
    }

    fn array_functions() -> &'static [Ascii<&'static str>] {
        static ARRAY_FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("array_count"),
            Ascii::new("array_max"),
            Ascii::new("array_min"),
            Ascii::new("array_any"),
            Ascii::new("array_approx_count_distinct"),
            Ascii::new("array_unique"),
            Ascii::new("array_sort_asc_null_first"),
            Ascii::new("array_sort_desc_null_first"),
            Ascii::new("array_sort_asc_null_last"),
            Ascii::new("array_sort_desc_null_last"),
            Ascii::new("array_remove_first"),
            Ascii::new("array_remove_last"),
            Ascii::new("array_distinct"),
        ];
        ARRAY_FUNCTIONS
    }

    fn try_rewrite_array_function(
        &mut self,
        span: Span,
        func_name: &str,
        params: &[Scalar],
        args: &mut [ScalarExpr],
        arg_types: &mut [DataType],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        // Try auto cast the Variant type to Array(Variant),
        // so that the array functions support Variant type as argument.
        let uni_case_func_name = Ascii::new(func_name);
        if Self::array_functions().contains(&uni_case_func_name)
            && !arg_types.is_empty()
            && arg_types[0].remove_nullable() == DataType::Variant
        {
            let target_type = if arg_types[0].is_nullable() {
                DataType::Nullable(Box::new(DataType::Array(Box::new(DataType::Nullable(
                    Box::new(DataType::Variant),
                )))))
            } else {
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Variant))))
            };
            let arg = args[0].clone();
            args[0] = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(arg),
                target_type: Box::new(target_type.clone()),
            });
            arg_types[0] = target_type;

            let result =
                self.resolve_scalar_function_call(span, func_name, params.to_vec(), args.to_vec());
            if func_name == "array_remove_first"
                || func_name == "array_remove_last"
                || func_name == "array_distinct"
                || func_name == "array_sort_asc_null_first"
                || func_name == "array_sort_desc_null_first"
                || func_name == "array_sort_asc_null_last"
                || func_name == "array_sort_desc_null_last"
            {
                if result.is_err() {
                    return Some(result);
                }
                let box (result_scalar, result_type) = result.unwrap();

                let result_target_type = if result_type.is_nullable() {
                    DataType::Nullable(Box::new(DataType::Variant))
                } else {
                    DataType::Variant
                };
                let result_target_scalar = ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: false,
                    argument: Box::new(result_scalar),
                    target_type: Box::new(result_target_type.clone()),
                });
                Some(Ok(Box::new((result_target_scalar, result_target_type))))
            } else {
                Some(result)
            }
        } else {
            None
        }
    }

    fn resolve_trim_function(
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

            let box (trim_scalar, trim_type) = self.resolve(trim_expr)?;
            (func_name, trim_scalar, trim_type)
        } else {
            let trim_scalar = ConstantExpr {
                span,
                value: Scalar::String(" ".to_string()),
            }
            .into();
            ("trim_both", trim_scalar, DataType::String)
        };

        let box (trim_source, _source_type) = self.resolve(expr)?;
        let args = vec![trim_source, trim_scalar];

        self.resolve_scalar_function_call(span, func_name, vec![], args)
    }

    /// Resolve literal values.
    pub fn resolve_literal_scalar(
        &self,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(Scalar, DataType)>> {
        let value = match literal {
            Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(*value)),
            Literal::Decimal256 {
                value,
                precision,
                scale,
            } => Scalar::Decimal(DecimalScalar::Decimal256(*value, DecimalSize {
                precision: *precision,
                scale: *scale,
            })),
            Literal::Float64(float) => Scalar::Number(NumberScalar::Float64((*float).into())),
            Literal::String(string) => Scalar::String(string.clone()),
            Literal::Boolean(boolean) => Scalar::Boolean(*boolean),
            Literal::Null => Scalar::Null,
        };
        let value = shrink_scalar(value);
        let data_type = value.as_ref().infer_data_type();
        Ok(Box::new((value, data_type)))
    }

    // TODO(leiysky): use an array builder function instead, since we should allow declaring
    // an array with variable as element.
    fn resolve_array(&mut self, span: Span, exprs: &[Expr]) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve(expr)?;
            elems.push(arg);
        }

        self.resolve_scalar_function_call(span, "array", vec![], elems)
    }

    fn resolve_map(
        &mut self,
        span: Span,
        kvs: &[(Literal, Expr)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut keys = Vec::with_capacity(kvs.len());
        let mut vals = Vec::with_capacity(kvs.len());
        for (key_expr, val_expr) in kvs {
            let box (key_arg, _data_type) = self.resolve_literal(span, key_expr)?;
            keys.push(key_arg);
            let box (val_arg, _data_type) = self.resolve(val_expr)?;
            vals.push(val_arg);
        }
        let box (key_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], keys)?;
        let box (val_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], vals)?;
        let args = vec![key_arg, val_arg];

        self.resolve_scalar_function_call(span, "map", vec![], args)
    }

    fn resolve_tuple(&mut self, span: Span, exprs: &[Expr]) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut args = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve(expr)?;
            args.push(arg);
        }

        self.resolve_scalar_function_call(span, "tuple", vec![], args)
    }

    fn resolve_like(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &Expr,
        right: &Expr,
        like_str: &str,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if check_const(like_str) {
            // Convert to equal comparison
            self.resolve_binary_op(span, &BinaryOperator::Eq, left, right)
        } else if check_prefix(like_str) {
            // Convert to `a >= like_str and a < like_str + 1`
            let mut char_vec: Vec<char> = like_str[0..like_str.len() - 1].chars().collect();
            let len = char_vec.len();
            let ascii_val = *char_vec.last().unwrap() as u8 + 1;
            char_vec[len - 1] = ascii_val as char;
            let like_str_plus: String = char_vec.iter().collect();
            let (new_left, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Gte, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(like_str[..like_str.len() - 1].to_owned()),
                })?;
            let (new_right, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Lt, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(like_str_plus),
                })?;
            self.resolve_scalar_function_call(span, "and", vec![], vec![new_left, new_right])
        } else {
            let name = op.to_func_name();
            self.resolve_function(span, name.as_str(), vec![], &[left, right])
        }
    }

    fn resolve_udf(
        &mut self,
        span: Span,
        udf_name: &str,
        arguments: &[Expr],
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        if self.forbid_udf {
            return Ok(None);
        }

        let udf = databend_common_base::runtime::block_on({
            UserApiProvider::instance().get_udf(&self.ctx.get_tenant(), udf_name)
        })?;

        let Some(udf) = udf else {
            return Ok(None);
        };

        let name = udf.name;

        match udf.definition {
            UDFDefinition::LambdaUDF(udf_def) => Ok(Some(
                self.resolve_lambda_udf(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFServer(udf_def) => Ok(Some(
                self.resolve_udf_server(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFScript(udf_def) => Ok(Some(
                self.resolve_udf_script(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDAFScript(udf_def) => Ok(Some(
                self.resolve_udaf_script(span, name, arguments, udf_def)?,
            )),
        }
    }

    fn resolve_udf_server(
        &mut self,
        span: Span,
        name: String,
        arguments: &[Expr],
        udf_definition: UDFServer,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        UDFValidator::is_udf_server_allowed(&udf_definition.address)?;
        if arguments.len() != udf_definition.arg_types.len() {
            return Err(ErrorCode::InvalidArgument(format!(
                "Require {} parameters, but got: {}",
                udf_definition.arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }

        let mut args = Vec::with_capacity(arguments.len());
        for (argument, dest_type) in arguments.iter().zip(udf_definition.arg_types.iter()) {
            let box (arg, ty) = self.resolve(argument)?;
            if ty != *dest_type {
                args.push(wrap_cast(&arg, dest_type));
            } else {
                args.push(arg);
            }
        }

        let arg_names = arguments.iter().map(|arg| format!("{}", arg)).join(", ");
        let display_name = format!("{}({})", udf_definition.handler, arg_names);

        self.bind_context.have_udf_server = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDFCall {
                span,
                name,
                handler: udf_definition.handler,
                display_name,
                udf_type: UDFType::Server(udf_definition.address.clone()),
                arg_types: udf_definition.arg_types,
                return_type: Box::new(udf_definition.return_type.clone()),
                arguments: args,
            }
            .into(),
            udf_definition.return_type.clone(),
        )))
    }

    async fn resolve_udf_with_stage(&mut self, code: String) -> Result<Vec<u8>> {
        let file_location = match code.strip_prefix('@') {
            Some(location) => FileLocation::Stage(location.to_string()),
            None => {
                let uri = UriLocation::from_uri(code.clone(), BTreeMap::default());

                match uri {
                    Ok(uri) => FileLocation::Uri(uri),
                    Err(_) => {
                        // fallback to use the code as real code
                        return Ok(code.into());
                    }
                }
            }
        };

        let (stage_info, module_path) = resolve_file_location(self.ctx.as_ref(), &file_location)
            .await
            .map_err(|err| {
                ErrorCode::SemanticError(format!(
                    "Failed to resolve code location {:?}: {}",
                    code, err
                ))
            })?;

        let op = init_stage_operator(&stage_info).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to get StageTable operator: {}", err))
        })?;

        let code_blob = op
            .read(&module_path)
            .await
            .map_err(|err| {
                ErrorCode::SemanticError(format!("Failed to read module {}: {}", module_path, err))
            })?
            .to_vec();

        let compress_algo = CompressAlgorithm::from_path(&module_path);
        log::trace!(
            "Detecting compression algorithm for module: {}",
            &module_path
        );
        log::info!("Detected compression algorithm: {:#?}", &compress_algo);

        let code_blob = match compress_algo {
            Some(algo) => {
                log::trace!("Decompressing module using {:?} algorithm", algo);
                let mut decoder = DecompressDecoder::new(algo);
                decoder.decompress_all(&code_blob).map_err(|err| {
                    let error_msg = format!("Failed to decompress module {}: {}", module_path, err);
                    log::error!("{}", error_msg);
                    ErrorCode::SemanticError(error_msg)
                })?
            }
            None => code_blob,
        };

        Ok(code_blob)
    }

    fn resolve_udf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let UDFScript {
            code,
            handler,
            language,
            arg_types,
            return_type,
            runtime_version,
        } = udf_definition;
        let language = language.parse()?;
        let mut arguments = Vec::with_capacity(args.len());
        for (argument, dest_type) in args.iter().zip(arg_types.iter()) {
            let box (arg, ty) = self.resolve(argument)?;
            if ty != *dest_type {
                arguments.push(wrap_cast(&arg, dest_type));
            } else {
                arguments.push(arg);
            }
        }

        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();
        let udf_type = UDFType::Script(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
        });

        let arg_names = args.iter().map(|arg| format!("{arg}")).join(", ");
        let display_name = format!("{}({})", &handler, arg_names);

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDFCall {
                span,
                name,
                handler,
                display_name,
                arg_types,
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        )))
    }

    fn resolve_udaf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDAFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let UDAFScript {
            code,
            language,
            arg_types,
            state_fields,
            return_type,
            runtime_version,
        } = udf_definition;
        let language = language.parse()?;
        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();
        let udf_type = UDFType::Script(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
        });

        let arguments = args
            .iter()
            .zip(arg_types.iter())
            .map(|(argument, dest_type)| {
                let box (arg, ty) = self.resolve(argument)?;
                Ok(if ty == *dest_type {
                    arg
                } else {
                    wrap_cast(&arg, dest_type)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let display_name = format!(
            "{name}({})",
            args.iter().map(|arg| format!("{:#}", arg)).join(", ")
        );

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDAFCall {
                span,
                name,
                display_name,
                arg_types,
                state_fields: state_fields
                    .iter()
                    .map(|f| UDFField {
                        name: f.name().to_string(),
                        data_type: f.data_type().clone(),
                    })
                    .collect(),
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        )))
    }

    fn resolve_lambda_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[Expr],
        udf_definition: LambdaUDF,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let parameters = udf_definition.parameters;
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
        let sql_tokens = tokenize_sql(udf_definition.definition.as_str())?;
        let expr = parse_expr(&sql_tokens, sql_dialect)?;
        let mut args_map = HashMap::new();
        arguments.iter().enumerate().for_each(|(idx, argument)| {
            if let Some(parameter) = parameters.get(idx) {
                args_map.insert(parameter.as_str(), (*argument).clone());
            }
        });
        let udf_expr = self
            .clone_expr_with_replacement(&expr, &|nest_expr| {
                if let Expr::ColumnRef { column, .. } = nest_expr {
                    if let Some(arg) = args_map.get(column.column.name()) {
                        return Ok(Some(arg.clone()));
                    }
                }
                Ok(None)
            })
            .map_err(|e| e.set_span(span))?;
        let scalar = self.resolve(&udf_expr)?;
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(scalar.0),
            }
            .into(),
            scalar.1,
        )))
    }

    fn resolve_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(self.bind_context.expr_context, ExprContext::InAsyncFunction) {
            return Err(
                ErrorCode::SemanticError("async functions cannot be nested".to_string())
                    .set_span(span),
            );
        }
        let original_context = self.bind_context.expr_context.clone();
        self.bind_context
            .set_expr_context(ExprContext::InAsyncFunction);
        let result = match func_name {
            "nextval" => self.resolve_nextval_async_function(span, func_name, arguments)?,
            "dict_get" => self.resolve_dict_get_async_function(span, func_name, arguments)?,
            _ => {
                return Err(ErrorCode::SemanticError(format!(
                    "cannot find async function {}",
                    func_name
                ))
                .set_span(span));
            }
        };
        // Restore the original context
        self.bind_context.set_expr_context(original_context);
        self.bind_context.have_async_func = true;
        Ok(result)
    }

    fn resolve_nextval_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if arguments.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function need one argument but got {}",
                arguments.len()
            ))
            .set_span(span));
        }
        let sequence_name = if let Expr::ColumnRef { column, .. } = arguments[0] {
            if column.database.is_some() || column.table.is_some() {
                return Err(ErrorCode::SemanticError(
                    "nextval function argument identifier should only contain one part".to_string(),
                )
                .set_span(span));
            }
            match &column.column {
                ColumnID::Name(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "nextval function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(span));
                }
            }
        } else {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function argument don't support expr {}",
                arguments[0]
            ))
            .set_span(span));
        };

        let catalog = self.ctx.get_default_catalog()?;
        let req = GetSequenceReq {
            ident: SequenceIdent::new(self.ctx.get_tenant(), sequence_name.clone()),
        };

        databend_common_base::runtime::block_on(catalog.get_sequence(req))?;

        let display_name = format!("{}({})", func_name, sequence_name);
        let return_type = DataType::Number(NumberDataType::UInt64);
        let func_arg = AsyncFunctionArgument::SequenceFunction(sequence_name);

        let async_func = AsyncFunctionCall {
            span,
            func_name: func_name.to_string(),
            display_name,
            return_type: Box::new(return_type.clone()),
            arguments: vec![],
            func_arg,
        };

        Ok(Box::new((async_func.into(), return_type)))
    }

    fn resolve_dict_get_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 3 {
            return Err(ErrorCode::SemanticError(format!(
                "dict_get function need three arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;

        let dict_name_arg = args[0];
        let field_arg = args[1];
        let key_arg = args[2];

        // Get dict_name and dict_meta.
        let (db_name, dict_name) = if let Expr::ColumnRef { column, .. } = dict_name_arg {
            if column.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(dict_name_arg.span()));
            }
            let db_name = match &column.table {
                Some(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                None => self.ctx.get_current_database(),
            };
            let dict_name = match &column.column {
                ColumnID::Name(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "dict_get function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(dict_name_arg.span()));
                }
            };
            (db_name, dict_name)
        } else {
            return Err(ErrorCode::SemanticError(
                "async function can only used as column".to_string(),
            )
            .set_span(dict_name_arg.span()));
        };
        let db = databend_common_base::runtime::block_on(
            catalog.get_database(&tenant, db_name.as_str()),
        )?;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.clone()),
        );
        let reply = databend_common_base::runtime::block_on(catalog.get_dictionary(req))?;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {}",
                dict_name,
            )));
        };

        // Get attr_name, attr_type and return_type.
        let box (field_scalar, _field_data_type) = self.resolve(field_arg)?;
        let Ok(field_expr) = ConstantExpr::try_from(field_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let Some(attr_name) = field_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let attr_field = dictionary.schema.field_with_name(attr_name)?;
        let attr_type: DataType = (&attr_field.data_type).into();
        let default_value = field_default_value(self.ctx.clone(), attr_field)?;

        // Get primary_key_value and check type.
        let primary_column_id = dictionary.primary_column_ids[0];
        let primary_field = dictionary.schema.field_of_column_id(primary_column_id)?;
        let primary_type: DataType = (&primary_field.data_type).into();

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve(key_arg)?;

        if primary_type != key_type.remove_nullable() {
            args.push(wrap_cast(&key_scalar, &primary_type));
        } else {
            args.push(key_scalar);
        }
        let dict_source = match dictionary.source.as_str() {
            "mysql" => {
                let connection_url = dictionary.build_sql_connection_url()?;
                let table = dictionary
                    .options
                    .get("table")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `table`"))?;
                DictionarySource::Mysql(SqlSource {
                    connection_url,
                    table: table.to_string(),
                    key_field: primary_field.name.clone(),
                    value_field: attr_field.name.clone(),
                })
            }
            "redis" => {
                let host = dictionary
                    .options
                    .get("host")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
                let port_str = dictionary
                    .options
                    .get("port")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
                let port = port_str
                    .parse()
                    .expect("Failed to parse String port to u16");
                let username = dictionary.options.get("username").cloned();
                let password = dictionary.options.get("password").cloned();
                let db_index = dictionary
                    .options
                    .get("db_index")
                    .map(|i| i.parse::<i64>().unwrap());
                DictionarySource::Redis(RedisSource {
                    host: host.to_string(),
                    port,
                    username,
                    password,
                    db_index,
                })
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported source {}",
                    dictionary.source
                )));
            }
        };

        let dict_get_func_arg = DictGetFunctionArgument {
            dict_source,
            default_value,
        };
        let display_name = format!(
            "{}({}.{}, {}, {})",
            func_name, db_name, dict_name, field_arg, key_arg,
        );
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(attr_type.clone()),
                arguments: args,
                func_arg: AsyncFunctionArgument::DictGetFunction(dict_get_func_arg),
            }),
            attr_type,
        )))
    }

    fn resolve_cast_to_variant(
        &mut self,
        span: Span,
        source_type: &DataType,
        scalar: &ScalarExpr,
        is_try: bool,
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        if !matches!(source_type.remove_nullable(), DataType::Tuple(_)) {
            return None;
        }
        // If the type of source column is a tuple, rewrite to json_object_keep_null function,
        // using the name of tuple inner fields as the object name.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            let column_entry = self.metadata.read().column(column.index).clone();
            if let ColumnEntry::BaseTableColumn(BaseTableColumn { data_type, .. }) = column_entry {
                let new_scalar = Self::rewrite_cast_to_variant(span, scalar, &data_type, is_try);
                let return_type = if is_try || source_type.is_nullable() {
                    DataType::Nullable(Box::new(DataType::Variant))
                } else {
                    DataType::Variant
                };
                return Some(Ok(Box::new((new_scalar, return_type))));
            }
        }
        None
    }

    fn rewrite_cast_to_variant(
        span: Span,
        scalar: &ScalarExpr,
        data_type: &TableDataType,
        is_try: bool,
    ) -> ScalarExpr {
        match data_type.remove_nullable() {
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let mut args = Vec::with_capacity(fields_name.len() * 2);
                for ((idx, field_name), field_type) in
                    fields_name.iter().enumerate().zip(fields_type.iter())
                {
                    let key = ConstantExpr {
                        span,
                        value: Scalar::String(field_name.clone()),
                    }
                    .into();

                    let value = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                    }
                    .into();

                    let value =
                        if matches!(field_type.remove_nullable(), TableDataType::Tuple { .. }) {
                            Self::rewrite_cast_to_variant(span, &value, field_type, is_try)
                        } else {
                            value
                        };

                    args.push(key);
                    args.push(value);
                }
                let func_name = if is_try {
                    "try_json_object_keep_null".to_string()
                } else {
                    "json_object_keep_null".to_string()
                };
                FunctionCall {
                    span,
                    params: vec![],
                    arguments: args,
                    func_name,
                }
                .into()
            }
            _ => {
                let func_name = if is_try {
                    "try_to_variant".to_string()
                } else {
                    "to_variant".to_string()
                };
                FunctionCall {
                    span,
                    params: vec![],
                    arguments: vec![scalar.clone()],
                    func_name,
                }
                .into()
            }
        }
    }

    fn resolve_map_access(
        &mut self,
        expr: &Expr,
        mut paths: VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (mut scalar, data_type) = self.resolve(expr)?;
        // Variant type can be converted to `get_by_keypath` function.
        if data_type.remove_nullable() == DataType::Variant {
            return self.resolve_variant_map_access(scalar, &mut paths);
        }

        let mut table_data_type = infer_schema_type(&data_type)?;
        // If it is a tuple column, convert it to the internal column specified by the paths.
        // For other types of columns, convert it to get functions.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            if column.index < self.metadata.read().columns().len() {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn(BaseTableColumn { ref data_type, .. }) =
                    column_entry
                {
                    // Use data type from meta to get the field names of tuple type.
                    table_data_type = data_type.clone();
                    if let TableDataType::Tuple { .. } = table_data_type.remove_nullable() {
                        let box (inner_scalar, _inner_data_type) = self
                            .resolve_tuple_map_access_pushdown(
                                expr.span(),
                                column.clone(),
                                &mut table_data_type,
                                &mut paths,
                            )?;
                        scalar = inner_scalar;
                    }
                }
            }
        }

        // Otherwise, desugar it into a `get` function.
        while let Some((span, path_lit)) = paths.pop_front() {
            table_data_type = table_data_type.remove_nullable();
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type
            {
                let idx = match path_lit {
                    Literal::UInt64(idx) => {
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
                        (idx - 1) as usize
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => idx,
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            )));
                        }
                    },
                    _ => unreachable!(),
                };
                table_data_type = fields_type.get(idx).unwrap().clone();
                scalar = FunctionCall {
                    span: expr.span(),
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                    arguments: vec![scalar.clone()],
                }
                .into();
                continue;
            }
            let box (path_scalar, _) = self.resolve_literal(span, &path_lit)?;
            if let TableDataType::Array(inner_type) = table_data_type {
                table_data_type = *inner_type;
            }
            table_data_type = table_data_type.wrap_nullable();
            scalar = FunctionCall {
                span: path_scalar.span(),
                func_name: "get".to_string(),
                params: vec![],
                arguments: vec![scalar.clone(), path_scalar],
            }
            .into();
        }
        let return_type = scalar.data_type()?;
        Ok(Box::new((scalar, return_type)))
    }

    fn resolve_tuple_map_access_pushdown(
        &mut self,
        span: Span,
        column: ColumnBinding,
        table_data_type: &mut TableDataType,
        paths: &mut VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut names = Vec::new();
        names.push(column.column_name.clone());
        let mut index_with_types = VecDeque::with_capacity(paths.len());
        while paths.front().is_some() {
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type.remove_nullable()
            {
                let (span, path) = paths.pop_front().unwrap();
                let idx = match path {
                    Literal::UInt64(idx) => {
                        if idx == 0 {
                            return Err(ErrorCode::SemanticError(
                                "tuple index is starting from 1, but 0 is found".to_string(),
                            )
                            .set_span(span));
                        }
                        if idx as usize > fields_type.len() {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple index {} is out of bounds for length {}",
                                idx,
                                fields_type.len()
                            ))
                            .set_span(span));
                        }
                        idx as usize - 1
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => idx,
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            ))
                            .set_span(span));
                        }
                    },
                    _ => unreachable!(),
                };
                let inner_field_name = fields_name.get(idx).unwrap();
                let inner_name = display_tuple_field_name(inner_field_name);
                names.push(inner_name);
                let inner_type = fields_type.get(idx).unwrap();
                index_with_types.push_back((idx + 1, inner_type.clone()));
                *table_data_type = inner_type.clone();
            } else {
                // other data types use `get` function.
                break;
            };
        }

        let inner_column_ident = Identifier::from_name(span, names.join(":"));
        match self.bind_context.resolve_name(
            column.database_name.as_deref(),
            column.table_name.as_deref(),
            &inner_column_ident,
            self.aliases,
            self.name_resolution_ctx,
        ) {
            Ok(result) => {
                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        let data_type = *column.data_type.clone();
                        (BoundColumnRef { span, column }.into(), data_type)
                    }
                    _ => unreachable!(),
                };
                Ok(Box::new((scalar, data_type)))
            }
            Err(_) => {
                // inner column is not exist in view, desugar it into a `get` function.
                let mut scalar: ScalarExpr = BoundColumnRef { span, column }.into();
                while let Some((idx, table_data_type)) = index_with_types.pop_front() {
                    scalar = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64(idx as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                    }
                    .into();
                    scalar = wrap_cast(&scalar, &DataType::from(&table_data_type));
                }
                let return_type = scalar.data_type()?;
                Ok(Box::new((scalar, return_type)))
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
        let distinct_const_scan = SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items: vec![ScalarItem {
                        scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: ctx.columns[0].clone(),
                        }),
                        index: self.metadata.read().columns().len() - 1,
                    }],
                    ..Default::default()
                }
                .into(),
            ),
            Arc::new(const_scan),
        );

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
            compare_op: Some(ComparisonOp::Equal),
            output_column: ctx.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ: SubqueryType::Any,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg: None,
        };
        let data_type = subquery_expr.data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    fn try_rewrite_virtual_column(
        &mut self,
        base_column: &BaseTableColumn,
        keypaths: &KeyPaths,
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        if !self.bind_context.virtual_column_context.allow_pushdown {
            return Ok(None);
        }
        if let Some(virtual_column_name_map) = self
            .bind_context
            .virtual_column_context
            .virtual_column_names
            .get(&base_column.table_index)
        {
            let mut name = String::new();
            name.push_str(&base_column.column_name);
            for path in &keypaths.paths {
                name.push('[');
                match path {
                    KeyPath::Index(idx) => {
                        name.push_str(&idx.to_string());
                    }
                    KeyPath::QuotedName(field) | KeyPath::Name(field) => {
                        name.push('\'');
                        name.push_str(field.as_ref());
                        name.push('\'');
                    }
                }
                name.push(']');
            }

            let virtual_type = virtual_column_name_map.get(&name);
            let is_created = virtual_type.is_some();

            let mut index = 0;
            // Check for duplicate virtual columns
            for table_column in self
                .metadata
                .read()
                .virtual_columns_by_table_index(base_column.table_index)
            {
                if table_column.name() == name {
                    index = table_column.index();
                    break;
                }
            }

            let table_data_type = if let Some(virtual_type) = virtual_type {
                virtual_type.wrap_nullable()
            } else {
                TableDataType::Nullable(Box::new(TableDataType::Variant))
            };

            if index == 0 {
                let column_id = self
                    .bind_context
                    .virtual_column_context
                    .next_column_ids
                    .get(&base_column.table_index)
                    .unwrap();

                let keypaths_str = format!("{}", keypaths);
                let keypaths_value = Scalar::String(keypaths_str);

                index = self.metadata.write().add_virtual_column(
                    base_column,
                    *column_id,
                    name.clone(),
                    table_data_type.clone(),
                    keypaths_value.clone(),
                    None,
                    is_created,
                );

                // Increments the column id of the virtual column.
                let column_id = self
                    .bind_context
                    .virtual_column_context
                    .next_column_ids
                    .get_mut(&base_column.table_index)
                    .unwrap();
                *column_id += 1;
            }

            if let Some(indices) = self
                .bind_context
                .virtual_column_context
                .virtual_column_indices
                .get_mut(&base_column.table_index)
            {
                indices.push(index);
            } else {
                self.bind_context
                    .virtual_column_context
                    .virtual_column_indices
                    .insert(base_column.table_index, vec![index]);
            }

            let data_type = DataType::from(&table_data_type);
            let column_binding = ColumnBindingBuilder::new(
                name,
                index,
                Box::new(data_type.clone()),
                Visibility::InVisible,
            )
            .table_index(Some(base_column.table_index))
            .build();

            let virtual_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: column_binding,
            });
            Ok(Some(Box::new((virtual_column, data_type))))
        } else {
            Ok(None)
        }
    }

    // Rewrite variant map access as `get_by_keypath` function
    fn resolve_variant_map_access(
        &mut self,
        scalar: ScalarExpr,
        paths: &mut VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut key_paths = Vec::with_capacity(paths.len());
        for (span, path) in paths.iter() {
            let key_path = match path {
                Literal::UInt64(idx) => {
                    if let Ok(i) = i32::try_from(*idx) {
                        KeyPath::Index(i)
                    } else {
                        return Err(ErrorCode::SemanticError(format!(
                            "path index is overflow, max allowed value is {}, but got {}",
                            i32::MAX,
                            idx
                        ))
                        .set_span(*span));
                    }
                }
                Literal::String(field) => KeyPath::QuotedName(std::borrow::Cow::Borrowed(field)),
                _ => unreachable!(),
            };
            key_paths.push(key_path);
        }

        let keypaths = KeyPaths { paths: key_paths };

        // try rewrite as virtual column and pushdown to storage layer.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            if column.index < self.metadata.read().columns().len() {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn(base_column) = column_entry {
                    if let Some(box (scalar, data_type)) =
                        self.try_rewrite_virtual_column(&base_column, &keypaths)?
                    {
                        return Ok(Box::new((scalar, data_type)));
                    }
                }
            }
        }

        let keypaths_str = format!("{}", keypaths);
        let path_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::String(keypaths_str),
        });
        let args = vec![scalar, path_scalar];

        Ok(Box::new((
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "get_by_keypath".to_string(),
                params: vec![],
                arguments: args,
            }),
            DataType::Nullable(Box::new(DataType::Variant)),
        )))
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
                Expr::DatePart { span, kind, expr } => Ok(Expr::DatePart {
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
                    func:
                        ASTFunctionCall {
                            distinct,
                            name,
                            args,
                            params,
                            order_by,
                            window,
                            lambda,
                        },
                } => Ok(Expr::FunctionCall {
                    span: *span,
                    func: ASTFunctionCall {
                        distinct: *distinct,
                        name: name.clone(),
                        args: args
                            .iter()
                            .map(|arg| self.clone_expr_with_replacement(arg, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                        params: params.clone(),
                        order_by: order_by.clone(),
                        window: window.clone(),
                        lambda: if let Some(lambda) = lambda {
                            Some(Lambda {
                                params: lambda.params.clone(),
                                expr: Box::new(
                                    self.clone_expr_with_replacement(&lambda.expr, replacement_fn)?,
                                ),
                            })
                        } else {
                            None
                        },
                    },
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

    fn try_fold_constant<Index: ColumnIndex>(
        &self,
        expr: &EExpr<Index>,
        enable_shrink: bool,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if expr.is_deterministic(&BUILTIN_FUNCTIONS) && enable_shrink {
            if let (EExpr::Constant { scalar, .. }, _) =
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

pub fn resolve_type_name_by_str(name: &str, not_null: bool) -> Result<TableDataType> {
    let sql_tokens = databend_common_ast::parser::tokenize_sql(name)?;
    let ast = databend_common_ast::parser::run_parser(
        &sql_tokens,
        databend_common_ast::parser::Dialect::default(),
        databend_common_ast::parser::ParseMode::Default,
        false,
        databend_common_ast::parser::expr::type_name,
    )?;
    resolve_type_name(&ast, not_null)
}

pub fn resolve_type_name(type_name: &TypeName, not_null: bool) -> Result<TableDataType> {
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
        TypeName::Binary => TableDataType::Binary,
        TypeName::String => TableDataType::String,
        TypeName::Timestamp => TableDataType::Timestamp,
        TypeName::Date => TableDataType::Date,
        TypeName::Array(item_type) => {
            TableDataType::Array(Box::new(resolve_type_name(item_type, not_null)?))
        }
        TypeName::Map { key_type, val_type } => {
            let key_type = resolve_type_name(key_type, true)?;
            match key_type {
                TableDataType::Boolean
                | TableDataType::String
                | TableDataType::Number(_)
                | TableDataType::Decimal(_)
                | TableDataType::Timestamp
                | TableDataType::Date => {
                    let val_type = resolve_type_name(val_type, not_null)?;
                    let inner_type = TableDataType::Tuple {
                        fields_name: vec!["key".to_string(), "value".to_string()],
                        fields_type: vec![key_type, val_type],
                    };
                    TableDataType::Map(Box::new(inner_type))
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid Map key type \'{:?}\'",
                        key_type
                    )));
                }
            }
        }
        TypeName::Bitmap => TableDataType::Bitmap,
        TypeName::Interval => TableDataType::Interval,
        TypeName::Tuple {
            fields_type,
            fields_name,
        } => TableDataType::Tuple {
            fields_name: match fields_name {
                None => (0..fields_type.len())
                    .map(|i| (i + 1).to_string())
                    .collect(),
                Some(names) => names
                    .iter()
                    .map(|i| {
                        if i.is_quoted() {
                            i.name.clone()
                        } else {
                            i.name.to_lowercase()
                        }
                    })
                    .collect(),
            },
            fields_type: fields_type
                .iter()
                .map(|item_type| resolve_type_name(item_type, not_null))
                .collect::<Result<Vec<_>>>()?,
        },
        TypeName::Nullable(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.wrap_nullable()
        }
        TypeName::Variant => TableDataType::Variant,
        TypeName::Geometry => TableDataType::Geometry,
        TypeName::Geography => TableDataType::Geography,
        TypeName::NotNull(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.remove_nullable()
        }
    };
    if !matches!(type_name, TypeName::Nullable(_) | TypeName::NotNull(_)) && !not_null {
        return Ok(data_type.wrap_nullable());
    }
    Ok(data_type)
}

pub fn resolve_type_name_udf(type_name: &TypeName) -> Result<TableDataType> {
    let type_name = match type_name {
        name @ TypeName::Nullable(_) | name @ TypeName::NotNull(_) => name,
        name => &name.clone().wrap_nullable(),
    };
    resolve_type_name(type_name, true)
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

// Some check functions for like expression
fn check_const(like_str: &str) -> bool {
    for char in like_str.chars() {
        if char == '_' || char == '%' {
            return false;
        }
    }
    true
}

fn check_prefix(like_str: &str) -> bool {
    if like_str.contains("\\%") {
        return false;
    }
    if like_str.len() == 1 && matches!(like_str, "%" | "_") {
        return false;
    }
    if like_str.chars().filter(|c| *c == '%').count() != 1 {
        return false;
    }

    let mut i: usize = like_str.len();
    while i > 0 {
        if let Some(c) = like_str.chars().nth(i - 1) {
            if c != '%' {
                break;
            }
        } else {
            return false;
        }
        i -= 1;
    }
    if i == like_str.len() {
        return false;
    }
    for j in (0..i).rev() {
        if let Some(c) = like_str.chars().nth(j) {
            if c == '_' {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

// If `InList` expr satisfies the following conditions, it can be converted to `contain` function
// Note: the method mainly checks if list contains NULL literal, because `contain` can't handle NULL.
fn satisfy_contain_func(expr: &Expr) -> bool {
    match expr {
        Expr::Literal { value, .. } => !matches!(value, Literal::Null),
        Expr::Tuple { exprs, .. } => {
            // For each expr in `exprs`, check if it satisfies the conditions
            exprs.iter().all(satisfy_contain_func)
        }
        Expr::Array { exprs, .. } => exprs.iter().all(satisfy_contain_func),
        // FIXME: others expr won't exist in `InList` expr
        _ => false,
    }
}
