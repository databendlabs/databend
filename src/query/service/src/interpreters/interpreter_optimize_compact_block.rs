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
use std::time::Duration;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::MAX_SEGMENT_LOCATIONS_PER_CLAIM;
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use rand::Rng;

use crate::interpreters::Interpreter;
use crate::locks::CoordinationManager;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct OptimizeCompactBlockInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    allow_segment_claims: bool,
}

impl OptimizeCompactBlockInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        allow_segment_claims: bool,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            s_expr,
            allow_segment_claims,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeCompactBlockInterpreter {
    fn name(&self) -> &str {
        "OptimizeCompactBlockInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let mut request: OptimizeCompactBlock = self.s_expr.plan().clone().try_into()?;
            for attempt in 0..=3 {
                self.ctx.evict_table_from_cache(
                    &request.catalog,
                    &request.database,
                    &request.table,
                )?;
                self.ctx.clear_table_meta_timestamps_cache();
                let table = self
                    .ctx
                    .get_table(&request.catalog, &request.database, &request.table)
                    .await?;
                let table_id = table.get_id();
                let claim_manager = (self.allow_segment_claims
                    && self.ctx.get_settings().get_enable_table_lock()?
                    && !table.is_temp())
                .then(CoordinationManager::instance);
                if claim_manager.is_some() {
                    request.limit.segment_limit = Some(
                        request
                            .limit
                            .segment_limit
                            .unwrap_or(MAX_SEGMENT_LOCATIONS_PER_CLAIM)
                            .min(MAX_SEGMENT_LOCATIONS_PER_CLAIM),
                    );
                }
                let claimed = if let Some(manager) = &claim_manager {
                    manager
                        .claimed_segments(self.ctx.as_ref(), table_id)
                        .await?
                } else {
                    HashSet::new()
                };
                let mut builder =
                    PhysicalPlanBuilder::new(MetadataRef::default(), self.ctx.clone(), false);
                let planned = builder
                    .build_compact_block(&request, table, claim_manager.is_some(), &claimed)
                    .await;
                let (plan, sources) = match planned {
                    Ok(planned) => planned,
                    Err(error) if error.code() == ErrorCode::NO_NEED_TO_COMPACT => {
                        return Ok(PipelineBuildResult::create());
                    }
                    Err(error) => return Err(error),
                };
                let claim_guard = if let Some(manager) = &claim_manager {
                    let claim = manager
                        .try_segment_claim(self.ctx.clone(), table_id, sources)
                        .await?;
                    if claim.is_none() {
                        if attempt == 3 {
                            return Err(ErrorCode::TableAlreadyLocked(
                                "Compact segment-claim retries exhausted",
                            ));
                        }
                        let delay_ms = rand::thread_rng().gen_range(5..=20);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    claim
                } else {
                    None
                };
                let mut result =
                    build_query_pipeline_without_render_result_set(&self.ctx, &plan).await?;
                result.main_pipeline.add_lock_guard(claim_guard);
                return Ok(result);
            }
            unreachable!("compact claim retries return success or an explicit conflict")
        })
    }
}
