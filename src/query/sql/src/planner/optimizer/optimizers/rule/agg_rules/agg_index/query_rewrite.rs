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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use super::prepare::AggIndexViewInfo;
use super::prepare::PreparedAggIndexQuery;
use crate::AggIndexPlan;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::RuleID;
use crate::planner::QueryExecutor;

pub fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    base_columns: &[ColumnEntry],
    s_expr: &SExpr,
    index_plans: &[AggIndexPlan],
) -> Result<Option<SExpr>> {
    if index_plans.is_empty() {
        return Ok(None);
    }

    let prepared_query =
        PreparedAggIndexQuery::prepare(table_index, table_name, base_columns, s_expr)?;
    let matcher = prepared_query.matcher();

    for index_plan in index_plans {
        if let Some(result) = matcher.try_rewrite_index(
            s_expr,
            index_plan.index_id,
            &index_plan.sql,
            index_plan.prepared.as_ref(),
        )? {
            return Ok(Some(result));
        }
    }

    Ok(None)
}

pub fn build_agg_index_plan_for_table(
    table_ctx: Arc<dyn TableContext>,
    query_executor: Option<Arc<dyn QueryExecutor>>,
    metadata: MetadataRef,
    table_index: IndexType,
    index_id: u64,
    sql: String,
    s_expr: SExpr,
) -> Result<AggIndexPlan> {
    let s_expr = optimize_agg_index_s_expr(table_ctx, query_executor, metadata.clone(), &s_expr)?;
    let metadata_ref = metadata.read();
    let table_name = metadata_ref.table(table_index).name();
    let base_columns = metadata_ref.columns_by_table_index(table_index);

    Ok(AggIndexPlan {
        index_id,
        sql,
        metadata: metadata.clone(),
        prepared: Arc::new(AggIndexViewInfo::new(
            table_index,
            table_name,
            &base_columns,
            &s_expr,
        )?),
        s_expr,
    })
}

fn optimize_agg_index_s_expr(
    table_ctx: Arc<dyn TableContext>,
    query_executor: Option<Arc<dyn QueryExecutor>>,
    metadata: MetadataRef,
    s_expr: &SExpr,
) -> Result<SExpr> {
    let settings = table_ctx.get_settings();
    let opt_ctx = OptimizerContext::new(table_ctx, metadata)
        .with_settings(&settings)?
        .set_sample_executor(query_executor)
        .clone();
    let optimizer = RecursiveRuleOptimizer::new(opt_ctx, &[
        RuleID::NormalizeScalarFilter,
        RuleID::FilterNulls,
        RuleID::EliminateFilter,
        RuleID::MergeFilter,
    ]);
    optimizer.optimize_sync(s_expr)
}
