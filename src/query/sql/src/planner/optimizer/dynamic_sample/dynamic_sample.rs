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
use std::time::Duration;

use databend_common_base::base::tokio::time::Instant;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use crate::optimizer::dynamic_sample::filter_selectivity_sample::filter_selectivity_sample;
use crate::optimizer::dynamic_sample::join_selectivity_sample::join_selectivity_sample;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::planner::query_executor::QueryExecutor;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::MetadataRef;

#[async_recursion::async_recursion(#[recursive::recursive])]
pub async fn dynamic_sample(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    s_expr: &SExpr,
    sample_executor: Arc<dyn QueryExecutor>,
) -> Result<Arc<StatInfo>> {
    let time_budget =
        Duration::from_millis(ctx.get_settings().get_dynamic_sample_time_budget_ms()?);
    let start_time = Instant::now();

    async fn sample_with_budget<F, Fut>(
        start_time: Instant,
        time_budget: Duration,
        fallback: F,
        sample_fn: impl FnOnce() -> Fut,
    ) -> Result<Arc<StatInfo>>
    where
        F: FnOnce() -> Result<Arc<StatInfo>>,
        Fut: std::future::Future<Output = Result<Arc<StatInfo>>>,
    {
        if time_budget.as_millis() == 0 || start_time.elapsed() > time_budget {
            fallback()
        } else {
            let remaining_time = time_budget - start_time.elapsed();
            match tokio::time::timeout(remaining_time, sample_fn()).await {
                Ok(Ok(result)) => Ok(result),
                // The error contains the timeout error or the error from the sample_fn
                Ok(Err(_)) | Err(_) => fallback(),
            }
        }
    }

    match s_expr.plan() {
        RelOperator::Filter(_) => {
            sample_with_budget(
                start_time,
                time_budget,
                || {
                    let rel_expr = RelExpr::with_s_expr(s_expr);
                    rel_expr.derive_cardinality()
                },
                || filter_selectivity_sample(ctx, metadata, s_expr, sample_executor),
            )
            .await
        }
        RelOperator::Join(_) => {
            join_selectivity_sample(ctx, metadata, s_expr, sample_executor).await
        }
        RelOperator::Scan(_) => s_expr.plan().derive_stats(&RelExpr::with_s_expr(s_expr)),
        // Todo: add more operators here, and support more query patterns.
        _ => {
            let rel_expr = RelExpr::with_s_expr(s_expr);
            rel_expr.derive_cardinality()
        }
    }
}
