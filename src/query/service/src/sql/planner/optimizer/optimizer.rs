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

use std::sync::Arc;

use common_ast::ast::ExplainKind;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_planner::MetadataRef;

use crate::sql::optimizer::cascades::CascadesOptimizer;
use crate::sql::optimizer::distributed::optimize_distributed_query;
use crate::sql::optimizer::heuristic::RuleList;
use crate::sql::optimizer::util::contains_local_table_scan;
use crate::sql::optimizer::util::validate_distributed_query;
use crate::sql::optimizer::HeuristicOptimizer;
use crate::sql::optimizer::SExpr;
use crate::sql::optimizer::DEFAULT_REWRITE_RULES;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::BindContext;

#[derive(Debug, Clone, Default)]
pub struct OptimizerConfig {
    pub enable_distributed_optimization: bool,
}

#[derive(Debug)]
pub struct OptimizerContext {
    pub config: OptimizerConfig,
}

impl OptimizerContext {
    pub fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }
}

pub fn optimize(
    ctx: Arc<dyn TableContext>,
    opt_ctx: Arc<OptimizerContext>,
    plan: Plan,
) -> Result<Plan> {
    match plan {
        Plan::Query {
            s_expr,
            bind_context,
            metadata,
            rewrite_kind,
        } => Ok(Plan::Query {
            s_expr: Box::new(optimize_query(
                ctx,
                opt_ctx,
                metadata.clone(),
                bind_context.clone(),
                *s_expr,
            )?),
            bind_context,
            metadata,
            rewrite_kind,
        }),
        Plan::Explain { kind, plan } => match kind {
            ExplainKind::Raw | ExplainKind::Ast(_) | ExplainKind::Syntax(_) => {
                Ok(Plan::Explain { kind, plan })
            }
            _ => Ok(Plan::Explain {
                kind,
                plan: Box::new(optimize(ctx, opt_ctx, *plan)?),
            }),
        },
        Plan::Copy(v) => {
            Ok(Plan::Copy(Box::new(match *v {
                CopyPlanV2::IntoStage {
                    stage,
                    path,
                    validation_mode,
                    from,
                } => {
                    CopyPlanV2::IntoStage {
                        stage,
                        path,
                        validation_mode,
                        // Make sure the subquery has been optimized.
                        from: Box::new(optimize(ctx, opt_ctx, *from)?),
                    }
                }
                into_table => into_table,
            })))
        }
        // Passthrough statements
        _ => Ok(plan),
    }
}

pub fn optimize_query(
    ctx: Arc<dyn TableContext>,
    opt_ctx: Arc<OptimizerContext>,
    metadata: MetadataRef,
    bind_context: Box<BindContext>,
    s_expr: SExpr,
) -> Result<SExpr> {
    let rules = RuleList::create(DEFAULT_REWRITE_RULES.clone())?;

    let contains_local_table_scan = contains_local_table_scan(&s_expr, &metadata);

    let mut heuristic = HeuristicOptimizer::new(ctx.clone(), bind_context, metadata, rules);
    let mut result = heuristic.optimize(s_expr)?;

    let cascades = CascadesOptimizer::create(ctx)?;
    result = cascades.optimize(result)?;

    // So far, we don't have ability to execute distributed query
    // with reading data from local tales(e.g. system tables).
    let enable_distributed_query =
        opt_ctx.config.enable_distributed_optimization && !contains_local_table_scan;
    if enable_distributed_query && validate_distributed_query(&result) {
        result = optimize_distributed_query(&result)?;
    }

    Ok(result)
}
