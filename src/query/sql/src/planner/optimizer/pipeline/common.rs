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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table::DistributionLevel;
use databend_common_exception::Result;

use super::OptimizerPipeline;
use crate::MetadataRef;
use crate::Symbol;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::CTEFilterPushdownOptimizer;
use crate::optimizer::optimizers::CommonSubexpressionOptimizer;
use crate::optimizer::optimizers::DPhpyOptimizer;
use crate::optimizer::optimizers::EliminateSelfJoinOptimizer;
use crate::optimizer::optimizers::operator::DeduplicateJoinConditionOptimizer;
use crate::optimizer::optimizers::operator::PullUpFilterOptimizer;
use crate::optimizer::optimizers::operator::RuleNormalizeAggregateOptimizer;
use crate::optimizer::optimizers::operator::RuleStatsAggregateOptimizer;
use crate::optimizer::optimizers::operator::SingleToInnerOptimizer;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use crate::optimizer::optimizers::rule::RuleEagerAggregation;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::statistics::CollectStatisticsOptimizer;
use crate::plans::RelOperator;

/// Check if a query will read data from local tables(e.g. system tables).
pub fn contains_local_table_scan(s_expr: &SExpr, metadata: &MetadataRef) -> bool {
    s_expr
        .children()
        .any(|s_expr| contains_local_table_scan(s_expr, metadata))
        || if let RelOperator::Scan(get) = s_expr.plan() {
            matches!(
                metadata
                    .read()
                    .table(get.table_index)
                    .table()
                    .distribution_level(),
                DistributionLevel::Local
            )
        } else {
            false
        }
        || matches!(s_expr.plan(), RelOperator::RecursiveCteScan { .. })
}

pub fn contains_warehouse_table_scan(s_expr: &SExpr, metadata: &MetadataRef) -> bool {
    if s_expr
        .children()
        .any(|s_expr| contains_warehouse_table_scan(s_expr, metadata))
    {
        return true;
    }

    if let RelOperator::Scan(scan) = s_expr.plan() {
        return matches!(
            metadata
                .read()
                .table(scan.table_index)
                .table()
                .distribution_level(),
            DistributionLevel::Warehouse,
        );
    }

    false
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizerStrategy {
    /// All query logical passes; physical planning runs separately.
    Query,
    /// Existing EXPLAIN MEMO preparation, ending after DPhyp. Retain its pass
    /// set, which excludes aggregate normalization, CSE and CTE pushdown.
    Memo,
    /// The production query input to DPhyp, before candidate search.
    BeforeDPhyp,
    /// The production query input to eager aggregation, before that rule.
    /// Tests needing the original join order use disable_join_reorder.
    BeforeEagerAggregation,
}

impl OptimizerPipeline {
    pub async fn query(
        opt_ctx: Arc<OptimizerContext>,
        s_expr: SExpr,
        output_columns: Option<HashSet<Symbol>>,
        strategy: OptimizerStrategy,
    ) -> Result<Self> {
        let settings = opt_ctx.get_table_ctx().get_settings();
        let memo = strategy == OptimizerStrategy::Memo;
        let pipeline = Self::new(opt_ctx.clone(), s_expr)
            .await?
            .add(SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None))
            .add(RuleStatsAggregateOptimizer::new(opt_ctx.clone()))
            .add(CollectStatisticsOptimizer::new(opt_ctx.clone()))
            // Normalization precedes SplitAggregate in query preparation.
            .add_if(!memo, RuleNormalizeAggregateOptimizer::new())
            .add(PullUpFilterOptimizer::new(opt_ctx.clone()))
            .add_if(
                !memo && settings.get_enable_cse_optimizer()?,
                CommonSubexpressionOptimizer::new(opt_ctx.clone()),
            )
            // Only outer queries supply authoritative result columns.
            .add(
                RecursiveRuleOptimizer::new_with_materialized_view_output_columns(
                    opt_ctx.clone(),
                    &DEFAULT_REWRITE_RULES,
                    output_columns,
                ),
            )
            .add_if(!memo, CTEFilterPushdownOptimizer::new(opt_ctx.clone()))
            .add(RecursiveRuleOptimizer::new(opt_ctx.clone(), &[
                RuleID::SplitAggregate,
            ]));

        if strategy == OptimizerStrategy::BeforeDPhyp {
            return Ok(pipeline);
        }
        let pipeline = pipeline.add(DPhpyOptimizer::new(opt_ctx.clone()));
        if memo {
            return Ok(pipeline);
        }
        let pipeline = pipeline
            .add(EliminateSelfJoinOptimizer::new(opt_ctx.clone()))
            .add(SingleToInnerOptimizer::new())
            .add(DeduplicateJoinConditionOptimizer::new())
            .add_if(
                opt_ctx.get_enable_join_reorder(),
                RecursiveRuleOptimizer::new(opt_ctx.clone(), &[RuleID::CommuteJoin]),
            );
        if strategy == OptimizerStrategy::BeforeEagerAggregation {
            return Ok(pipeline);
        }
        Ok(pipeline.add_if(
            settings.get_force_eager_aggregate()?,
            RuleEagerAggregation::new(opt_ctx.get_metadata()),
        ))
    }
}
