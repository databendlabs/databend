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

use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::OptimizePurgePlan;
use databend_common_storages_factory::NavigationPoint;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizePurgeInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizePurgePlan,
}

impl OptimizePurgeInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizePurgePlan) -> Result<Self> {
        Ok(OptimizePurgeInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizePurgeInterpreter {
    fn name(&self) -> &str {
        "OptimizePurgeInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        purge(
            self.ctx.clone(),
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.table,
            self.plan.num_snapshot_limit,
            self.plan.instant.clone(),
        )
        .await?;
        Ok(PipelineBuildResult::create())
    }
}

pub(crate) async fn purge(
    ctx: Arc<QueryContext>,
    catalog: &str,
    database: &str,
    table: &str,
    num_snapshot_limit: Option<usize>,
    instant: Option<NavigationPoint>,
) -> Result<()> {
    let catalog = ctx.get_catalog(catalog).await?;
    // currently, context caches the table, we have to "refresh"
    // the table by using the catalog API directly
    let table = catalog
        .get_table(&ctx.get_tenant(), database, table)
        .await?;
    // check mutability
    table.check_mutable()?;

    let keep_latest = true;
    let res = table
        .purge(ctx, instant, num_snapshot_limit, keep_latest, false)
        .await?;
    assert!(res.is_none());
    Ok(())
}
