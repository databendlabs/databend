// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;

use super::UDFFetcher;

pub struct UDFTransformer;

impl UDFTransformer {
    pub async fn transform_function<F: UDFFetcher>(
        function: &Function,
        fetcher: &F,
    ) -> Result<Expr> {
        let definition = fetcher
            .get_udf_definition(&function.name.to_string())
            .await?;
        let parameters = definition.parameters;
        let expr = definition.expr;

        if parameters.len() != function.args.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Requir {} parameters, but got: {}",
                parameters.len(),
                function.args.len()
            )));
        }

        let mut args_map = HashMap::new();
        function.args.iter().enumerate().for_each(|(index, f_arg)| {
            if let Some(param) = parameters.get(index) {
                args_map.insert(param, match f_arg {
                    FunctionArg::Named { arg, .. } => arg.clone(),
                    FunctionArg::Unnamed(unnamed_arg) => unnamed_arg.clone(),
                });
            }
        });

        Self::clone_expr_with_replacement(&expr, &|nest_expr| {
            if let Expr::Identifier(Ident { value, .. }) = nest_expr {
                if let Some(arg) = args_map.get(value) {
                    if let Ok(expr) = function_arg_as_expr(arg) {
                        return Ok(Some(expr));
                    }
                }
            }

            Ok(None)
        })
    }

    fn clone_expr_with_replacement<F>(original_expr: &Expr, replacement_fn: &F) -> Result<Expr>
    where F: Fn(&Expr) -> Result<Option<Expr>> {
        let replacement_opt = replacement_fn(original_expr)?;

        match replacement_opt {
            // If we were provided a replacement, use the replacement. Do not
            // descend further.
            Some(replacement) => Ok(replacement),
            // No replacement was provided, clone the node and recursively call
            // clone_expr_with_replacement() on any nested Expressionessions.
            None => match original_expr {
                Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                    Self::clone_expr_with_replacement(&**expr, replacement_fn)?,
                ))),
                Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                    Self::clone_expr_with_replacement(&**expr, replacement_fn)?,
                ))),
                Expr::IsDistinctFrom(left, right) => Ok(Expr::IsDistinctFrom(
                    Box::new(Self::clone_expr_with_replacement(&**left, replacement_fn)?),
                    Box::new(Self::clone_expr_with_replacement(&**right, replacement_fn)?),
                )),
                Expr::IsNotDistinctFrom(left, right) => Ok(Expr::IsNotDistinctFrom(
                    Box::new(Self::clone_expr_with_replacement(&**left, replacement_fn)?),
                    Box::new(Self::clone_expr_with_replacement(&**right, replacement_fn)?),
                )),
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => Ok(Expr::InList {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    list: list
                        .iter()
                        .map(|item| {
                            Self::clone_expr_with_replacement(item, replacement_fn).unwrap()
                        })
                        .collect::<Vec<Expr>>(),
                    negated: *negated,
                }),
                Expr::Between {
                    expr,
                    negated,
                    low,
                    high,
                } => Ok(Expr::Between {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    negated: *negated,
                    low: Box::new(Self::clone_expr_with_replacement(&**low, replacement_fn)?),
                    high: Box::new(Self::clone_expr_with_replacement(&**high, replacement_fn)?),
                }),
                Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                    left: Box::new(Self::clone_expr_with_replacement(&**left, replacement_fn)?),
                    op: op.clone(),
                    right: Box::new(Self::clone_expr_with_replacement(&**right, replacement_fn)?),
                }),
                Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                }),
                Expr::Cast { expr, data_type } => Ok(Expr::Cast {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    data_type: data_type.clone(),
                }),
                Expr::TryCast { expr, data_type } => Ok(Expr::TryCast {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    data_type: data_type.clone(),
                }),
                Expr::Extract { expr, field } => Ok(Expr::Extract {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    field: field.clone(),
                }),
                Expr::Position {
                    substr_expr,
                    str_expr,
                } => Ok(Expr::Position {
                    substr_expr: Box::new(Self::clone_expr_with_replacement(
                        &**substr_expr,
                        replacement_fn,
                    )?),
                    str_expr: Box::new(Self::clone_expr_with_replacement(
                        &**str_expr,
                        replacement_fn,
                    )?),
                }),
                Expr::Substring {
                    expr,
                    substring_from,
                    substring_for,
                } => Ok(Expr::Substring {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    substring_from: if let Some(substring_from_expr) = substring_from {
                        Some(Box::new(Self::clone_expr_with_replacement(
                            &**substring_from_expr,
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    substring_for: if let Some(substring_for_expr) = substring_for {
                        Some(Box::new(Self::clone_expr_with_replacement(
                            &**substring_for_expr,
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                Expr::Trim { expr, trim_where } => Ok(Expr::Trim {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    trim_where: if let Some((field, trim_expr)) = trim_where {
                        Some((
                            field.clone(),
                            Box::new(Self::clone_expr_with_replacement(
                                &**trim_expr,
                                replacement_fn,
                            )?),
                        ))
                    } else {
                        None
                    },
                }),
                Expr::Collate { expr, collation } => Ok(Expr::Collate {
                    expr: Box::new(Self::clone_expr_with_replacement(&**expr, replacement_fn)?),
                    collation: collation.clone(),
                }),
                Expr::Nested(expr) => Ok(Expr::Nested(Box::new(
                    Self::clone_expr_with_replacement(&**expr, replacement_fn)?,
                ))),
                Expr::Tuple(exprs) => Ok(Expr::Tuple(
                    exprs
                        .iter()
                        .map(|expr| {
                            Self::clone_expr_with_replacement(expr, replacement_fn).unwrap()
                        })
                        .collect::<Vec<Expr>>(),
                )),
                Expr::Value(value) => Ok(Expr::Value(value.clone())),
                Expr::TypedString { data_type, value } => Ok(Expr::TypedString {
                    data_type: data_type.clone(),
                    value: value.clone(),
                }),
                Expr::MapAccess { column, keys } => Ok(Expr::MapAccess {
                    column: Box::new(Self::clone_expr_with_replacement(
                        &**column,
                        replacement_fn,
                    )?),
                    keys: keys.clone(),
                }),
                Expr::Function(Function {
                    name,
                    params,
                    args,
                    over,
                    distinct,
                }) => Ok(Expr::Function(Function {
                    name: name.clone(),
                    params: params.clone(),
                    args: args
                        .iter()
                        .map(|f_arg| match f_arg {
                            FunctionArg::Named { name, arg } => FunctionArg::Named {
                                name: name.clone(),
                                arg: FunctionArgExpr::Expr(
                                    Self::clone_expr_with_replacement(
                                        &function_arg_as_expr(arg).unwrap(),
                                        replacement_fn,
                                    )
                                    .unwrap(),
                                ),
                            },
                            FunctionArg::Unnamed(arg) => {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Self::clone_expr_with_replacement(
                                        &function_arg_as_expr(arg).unwrap(),
                                        replacement_fn,
                                    )
                                    .unwrap(),
                                ))
                            }
                        })
                        .collect::<Vec<FunctionArg>>(),
                    over: over.clone(),
                    distinct: *distinct,
                })),
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => Ok(Expr::Case {
                    operand: if let Some(operand_expr) = operand {
                        Some(Box::new(Self::clone_expr_with_replacement(
                            &**operand_expr,
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    conditions: conditions
                        .iter()
                        .map(|expr| {
                            Self::clone_expr_with_replacement(expr, replacement_fn).unwrap()
                        })
                        .collect::<Vec<Expr>>(),
                    results: results
                        .iter()
                        .map(|expr| {
                            Self::clone_expr_with_replacement(expr, replacement_fn).unwrap()
                        })
                        .collect::<Vec<Expr>>(),
                    else_result: if let Some(else_result_expr) = else_result {
                        Some(Box::new(Self::clone_expr_with_replacement(
                            &**else_result_expr,
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                _ => Ok(original_expr.clone()),
            },
        }
    }
}

fn function_arg_as_expr(arg_expr: &FunctionArgExpr) -> Result<Expr> {
    match arg_expr {
        FunctionArgExpr::Expr(expr) => Ok(expr.clone()),
        FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_) => Err(
            ErrorCode::SyntaxException(std::format!("Unsupported arg statement: {}", arg_expr)),
        ),
    }
}
