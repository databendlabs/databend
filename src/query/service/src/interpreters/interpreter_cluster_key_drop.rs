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

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::DropTableClusterKeyPlan;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableClusterKeyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableClusterKeyPlan,
}

impl DropTableClusterKeyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableClusterKeyPlan) -> Result<Self> {
        Ok(DropTableClusterKeyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableClusterKeyInterpreter {
    fn name(&self) -> &str {
        "DropTableClusterKeyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&plan.catalog).await?;

        let table = catalog
            .get_table(&tenant, &plan.database, &plan.table)
            .await?;
        if table.cluster_key_meta().is_none() {
            return Ok(PipelineBuildResult::create());
        }
        // check mutability
        table.check_mutable()?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let table_info = fuse_table.get_table_info();
        let mut new_table_meta = table_info.meta.clone();
        new_table_meta.cluster_key = None;
        new_table_meta.options.remove(OPT_KEY_CLUSTER_TYPE);

        let req = UpdateTableMetaReq {
            table_id: table_info.ident.table_id,
            seq: MatchSeq::Exact(table_info.ident.seq),
            new_table_meta,
            base_snapshot_location: fuse_table.snapshot_loc(),
        };
        catalog.update_single_table_meta(req, table_info).await?;

        Ok(PipelineBuildResult::create())
    }
}
