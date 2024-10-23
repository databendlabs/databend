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

use crate::optimizer::dynamic_sample::dynamic_sample;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::planner::query_executor::QueryExecutor;
use crate::plans::Join;
use crate::MetadataRef;

pub async fn join_selectivity_sample(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    s_expr: &SExpr,
    sample_executor: Arc<dyn QueryExecutor>,
) -> Result<Arc<StatInfo>> {
    let left_stat_info = dynamic_sample(
        ctx.clone(),
        metadata.clone(),
        s_expr.child(0)?,
        sample_executor.clone(),
    )
    .await?;
    let right_stat_info = dynamic_sample(
        ctx.clone(),
        metadata.clone(),
        s_expr.child(1)?,
        sample_executor.clone(),
    )
    .await?;
    let join = Join::try_from(s_expr.plan().clone())?;
    join.derive_join_stats(left_stat_info, right_stat_info)
}
