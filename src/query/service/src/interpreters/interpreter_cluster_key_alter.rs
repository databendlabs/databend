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
use databend_common_sql::plans::AlterTableClusterKeyPlan;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;

use super::Interpreter;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AlterTableClusterKeyInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTableClusterKeyPlan,
}

impl AlterTableClusterKeyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTableClusterKeyPlan) -> Result<Self> {
        Ok(AlterTableClusterKeyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterTableClusterKeyInterpreter {
    fn name(&self) -> &str {
        "AlterTableClusterKeyInterpreter"
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
            .get_table_with_batch(&tenant, &plan.database, &plan.table, plan.branch.as_deref())
            .await?;
        // check mutability
        table.check_mutable()?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let cluster_key_str = format!("({})", plan.cluster_keys.join(", "));
        // if new cluster_key_str is the same with old one,
        // no need to change
        if let Some(old_cluster_key_str) = fuse_table.cluster_key_str()
            && *old_cluster_key_str == cluster_key_str
        {
            let old_cluster_type = fuse_table.cluster_type();
            if old_cluster_type.is_some_and(|v| v.to_string().to_lowercase() == plan.cluster_type) {
                return Ok(PipelineBuildResult::create());
            }
        }

        let mut new_table_meta = fuse_table.get_table_info().meta.clone();
        new_table_meta.cluster_key_seq += 1;
        let cluster_key_id = new_table_meta.cluster_key_seq;
        commit_table_meta(
            self.ctx.as_ref(),
            table.as_ref(),
            new_table_meta,
            catalog,
            |snapshot_opt, meta| {
                if let Some(snapshot) = snapshot_opt {
                    snapshot.cluster_key_meta = Some((cluster_key_id, cluster_key_str.clone()));
                }
                if plan.branch.is_none() {
                    meta.cluster_key = Some(cluster_key_str);
                    meta.cluster_key_id = Some(cluster_key_id);
                    meta.options
                        .insert(OPT_KEY_CLUSTER_TYPE.to_owned(), plan.cluster_type.clone());
                }
            },
        )
        .await?;
        Ok(PipelineBuildResult::create())
    }
}
