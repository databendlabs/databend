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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;

use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::optimizer::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::Binder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarExpr;

#[derive(Debug, Clone)]
pub struct ReclusterPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub limit: Option<usize>,
    pub selection: Option<Expr>,
    pub is_final: bool,
}

pub fn set_update_stream_columns(s_expr: &SExpr) -> Result<SExpr> {
    match s_expr.plan() {
        RelOperator::Scan(scan) if scan.table_index == 0 => {
            let mut scan = scan.clone();
            scan.set_update_stream_columns(true);
            Ok(SExpr::create_leaf(Arc::new(scan.into())))
        }
        _ => {
            let mut children = Vec::with_capacity(s_expr.arity());
            for child in s_expr.children() {
                let child = set_update_stream_columns(child)?;
                children.push(Arc::new(child));
            }
            Ok(s_expr.replace_children(children))
        }
    }
}

#[async_backtrace::framed]
#[fastrace::trace]
pub async fn plan_hilbert_sql(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    sql: &str,
) -> Result<Plan> {
    let settings = ctx.get_settings();
    let tokens = tokenize_sql(sql)?;
    let sql_dialect = settings.get_sql_dialect().unwrap_or_default();
    let (stmt, _) = parse_sql(&tokens, sql_dialect)?;

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata.clone(),
    );
    let plan = binder.bind(&stmt).await?;

    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata)
        .with_enable_distributed_optimization(!ctx.get_cluster().is_empty())
        .with_enable_join_reorder(unsafe { !settings.get_disable_join_reorder()? })
        .with_enable_dphyp(settings.get_enable_dphyp()?)
        .with_max_push_down_limit(settings.get_max_push_down_limit()?);
    optimize(opt_ctx, plan).await
}

pub fn replace_with_constant(expr: &SExpr, variables: &VecDeque<Scalar>, partitions: u16) -> SExpr {
    #[recursive::recursive]
    fn visit_expr_column(expr: &mut ScalarExpr, variables: &mut VecDeque<Scalar>) {
        match expr {
            ScalarExpr::CastExpr(cast) => {
                visit_expr_column(&mut cast.argument, variables);
            }
            ScalarExpr::FunctionCall(call) => {
                if call.func_name == "range_partition_id" {
                    debug_assert_eq!(call.arguments.len(), 2);
                    let last = call.arguments.last_mut().unwrap();
                    let value = variables.pop_front().unwrap();
                    *last = ScalarExpr::ConstantExpr(ConstantExpr { span: None, value });
                }

                for arg in &mut call.arguments {
                    visit_expr_column(arg, variables);
                }
            }
            _ => (),
        }
    }

    #[recursive::recursive]
    fn replace_with_constant_into_child(
        s_expr: &SExpr,
        variables: &mut VecDeque<Scalar>,
        partitions: u16,
    ) -> SExpr {
        let mut s_expr = s_expr.clone();
        s_expr.plan = match s_expr.plan.as_ref() {
            RelOperator::EvalScalar(expr) if !variables.is_empty() => {
                let mut expr = expr.clone();
                for item in &mut expr.items {
                    visit_expr_column(&mut item.scalar, variables);
                }
                Arc::new(expr.into())
            }
            RelOperator::Aggregate(aggr) => {
                let mut aggr = aggr.clone();
                for item in &mut aggr.aggregate_functions {
                    if let ScalarExpr::AggregateFunction(func) = &mut item.scalar {
                        if func.func_name == "range_bound" {
                            func.params[0] = Scalar::Number(NumberScalar::UInt16(partitions));
                        }
                    }
                }
                Arc::new(aggr.into())
            }
            _ => s_expr.plan.clone(),
        };

        if s_expr.children.is_empty() {
            s_expr
        } else {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(replace_with_constant_into_child(
                    child, variables, partitions,
                )));
            }
            s_expr.children = children;
            s_expr
        }
    }

    let mut variables = variables.clone();
    replace_with_constant_into_child(expr, &mut variables, partitions)
}
