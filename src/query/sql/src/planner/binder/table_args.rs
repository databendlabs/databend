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
use databend_common_ast::ast::Identifier;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::planner::QueryExecutor;
use crate::plans::ConstantExpr;

/// Check if an AST expression contains a subquery
fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery { .. } => true,
        Expr::InSubquery { .. } => true,
        Expr::Exists { .. } => true,
        Expr::Cast { expr, .. } => contains_subquery(expr),
        Expr::TryCast { expr, .. } => contains_subquery(expr),
        Expr::FunctionCall { func, .. } => func.args.iter().any(contains_subquery),
        Expr::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        Expr::UnaryOp { expr, .. } => contains_subquery(expr),
        Expr::IsNull { expr, .. } => contains_subquery(expr),
        Expr::IsDistinctFrom { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        Expr::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_subquery(expr) || contains_subquery(low) || contains_subquery(high),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|e| contains_subquery(e))
                || conditions.iter().any(contains_subquery)
                || results.iter().any(contains_subquery)
                || else_result.as_ref().is_some_and(|e| contains_subquery(e))
        }
        Expr::MapAccess { expr, .. } => contains_subquery(expr),
        Expr::Array { exprs, .. } => exprs.iter().any(contains_subquery),
        Expr::Tuple { exprs, .. } => exprs.iter().any(contains_subquery),
        _ => false,
    }
}

/// Execute a subquery and extract a single scalar value from the result
fn execute_subquery_for_scalar(
    subquery_executor: &Arc<dyn QueryExecutor>,
    expr: &Expr,
) -> Result<Scalar> {
    let sql = expr.to_string();
    let data_blocks = databend_common_base::runtime::block_on(async {
        subquery_executor
            .execute_query_with_sql_string(&format!("SELECT {}", sql))
            .await
    })?;

    if data_blocks.is_empty() {
        return Err(ErrorCode::SemanticError(
            "Scalar subquery in table function argument returned no rows".to_string(),
        ));
    }

    let mut total_rows = 0;
    let mut result_scalar: Option<Scalar> = None;

    for block in &data_blocks {
        total_rows += block.num_rows();
        if total_rows > 1 {
            return Err(ErrorCode::SemanticError(
                "Scalar subquery in table function argument returned more than one row".to_string(),
            ));
        }

        if block.num_rows() == 1 && result_scalar.is_none() {
            if block.num_columns() != 1 {
                return Err(ErrorCode::SemanticError(
                    "Scalar subquery in table function argument must return exactly one column"
                        .to_string(),
                ));
            }
            let column = &block.columns()[0];
            let col_value = column.value();
            let value = col_value.index(0).unwrap();
            result_scalar = Some(value.to_owned());
        }
    }

    result_scalar.ok_or_else(|| {
        ErrorCode::SemanticError(
            "Scalar subquery in table function argument returned no rows".to_string(),
        )
    })
}

/// Try to fold an expression to a constant scalar value.
/// If constant folding fails and the expression contains a subquery,
/// execute the subquery and return the result.
fn try_fold_to_scalar(
    scalar: ScalarExpr,
    ast_expr: &Expr,
    scalar_binder: &ScalarBinder<'_>,
    subquery_executor: &Option<Arc<dyn QueryExecutor>>,
) -> Result<Scalar> {
    let expr = scalar.as_expr()?;
    let (expr, _) = ConstantFolder::fold(&expr, &scalar_binder.get_func_ctx()?, &BUILTIN_FUNCTIONS);

    match expr.into_constant() {
        Ok(Constant { scalar, .. }) => Ok(scalar),
        Err(_) => {
            if contains_subquery(ast_expr) {
                if let Some(executor) = subquery_executor {
                    return execute_subquery_for_scalar(executor, ast_expr);
                }
                return Err(ErrorCode::Internal(
                    "Subquery executor is not available for evaluating table function arguments"
                        .to_string(),
                ));
            }
            Err(ErrorCode::Unimplemented(format!(
                "Unsupported table argument type: {:?}",
                scalar
            )))
        }
    }
}

pub fn bind_table_args(
    scalar_binder: &mut ScalarBinder<'_>,
    params: &[Expr],
    named_params: &[(Identifier, Expr)],
) -> Result<TableArgs> {
    bind_table_args_with_subquery_executor(scalar_binder, params, named_params, &None)
}

pub fn bind_table_args_with_subquery_executor(
    scalar_binder: &mut ScalarBinder<'_>,
    params: &[Expr],
    named_params: &[(Identifier, Expr)],
    subquery_executor: &Option<Arc<dyn QueryExecutor>>,
) -> Result<TableArgs> {
    let mut args = Vec::with_capacity(params.len());
    for arg in params.iter() {
        args.push((scalar_binder.bind(arg)?.0, arg));
    }

    let mut named_args = Vec::with_capacity(named_params.len());
    for (name, arg) in named_params.iter() {
        named_args.push((name.clone(), scalar_binder.bind(arg)?.0, arg));
    }

    let positioned_args = args
        .into_iter()
        .map(|(scalar, ast_expr)| {
            try_fold_to_scalar(scalar, ast_expr, scalar_binder, subquery_executor)
        })
        .collect::<Result<Vec<_>>>()?;

    let named_args: HashMap<String, Scalar> = named_args
        .into_iter()
        .map(|(name, scalar, ast_expr)| match scalar {
            ScalarExpr::ConstantExpr(ConstantExpr { value, .. }) => Ok((name.name.clone(), value)),
            _ => {
                let value = try_fold_to_scalar(scalar, ast_expr, scalar_binder, subquery_executor)?;
                Ok((name.name.clone(), value))
            }
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(TableArgs {
        positioned: positioned_args,
        named: named_args,
    })
}
