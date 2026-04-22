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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::planner::query_executor::QueryExecutor;
use crate::plans::ConstantExpr;
use crate::ScalarBinder;
use crate::ScalarExpr;

pub(crate) fn execute_subquery_for_scalar(
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
        return Ok(Scalar::Null);
    }

    let block = DataBlock::concat(&data_blocks)?;

    if block.num_columns() != 1 {
        return Err(ErrorCode::SemanticError(
            "Scalar subquery in table function argument must return exactly one column".to_string(),
        ));
    }

    if block.num_rows() > 1 {
        return Err(ErrorCode::SemanticError(
            "Scalar subquery in table function argument returned more than one row".to_string(),
        ));
    }

    if block.num_rows() == 0 {
        return Ok(Scalar::Null);
    }

    let column = &block.columns()[0];
    let col_value = &column.value;
    Ok(col_value.index(0).unwrap().to_owned())
}

pub fn bind_table_args(
    scalar_binder: &mut ScalarBinder<'_>,
    params: &[Expr],
    named_params: &[(Identifier, Expr)],
) -> Result<TableArgs> {
    let mut args = Vec::with_capacity(params.len());
    for arg in params.iter() {
        args.push(scalar_binder.bind(arg)?.0);
    }

    let mut named_args = Vec::with_capacity(named_params.len());
    for (name, arg) in named_params.iter() {
        named_args.push((name.clone(), scalar_binder.bind(arg)?.0));
    }

    let positioned_args = args
        .into_iter()
        .map(|scalar| {
            let expr = scalar.as_expr()?;
            let (expr, _) =
                ConstantFolder::fold(&expr, &scalar_binder.get_func_ctx()?, &BUILTIN_FUNCTIONS);
            match expr {
                databend_common_expression::Expr::Constant { scalar, .. } => Ok(scalar),
                _ => Err(ErrorCode::Unimplemented(format!(
                    "Unsupported table argument type: {:?}",
                    scalar
                ))),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let named_args: HashMap<String, Scalar> = named_args
        .into_iter()
        .map(|(name, scalar)| match scalar {
            ScalarExpr::ConstantExpr(ConstantExpr { value, .. }) => Ok((name.name.clone(), value)),
            _ => {
                let expr = scalar.as_expr()?;
                let (expr, _) =
                    ConstantFolder::fold(&expr, &scalar_binder.get_func_ctx()?, &BUILTIN_FUNCTIONS);
                match expr {
                    databend_common_expression::Expr::Constant { scalar, .. } => {
                        Ok((name.name.clone(), scalar))
                    }
                    _ => Err(ErrorCode::Unimplemented(format!(
                        "Unsupported table argument type: {:?}",
                        scalar
                    ))),
                }
            }
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(TableArgs {
        positioned: positioned_args,
        named: named_args,
    })
}
