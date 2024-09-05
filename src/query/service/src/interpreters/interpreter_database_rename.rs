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

use databend_common_exception::Result;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_sql::plans::RenameDatabasePlan;
use databend_common_storages_share::remove_share_table_object;
use databend_common_storages_share::save_share_spec;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct RenameDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameDatabasePlan,
}

impl RenameDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameDatabasePlan) -> Result<Self> {
        Ok(RenameDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameDatabaseInterpreter {
    fn name(&self) -> &str {
        "RenameDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        for entity in &self.plan.entities {
            let catalog = self.ctx.get_catalog(&entity.catalog).await?;
            let tenant = self.plan.tenant.clone();
            let reply = catalog
                .rename_database(RenameDatabaseReq {
                    if_exists: entity.if_exists,
                    name_ident: DatabaseNameIdent::new(tenant, &entity.database),
                    new_db_name: entity.new_database.clone(),
                })
                .await?;
            if let Some((share_specs, object)) = reply.share_spec {
                save_share_spec(
                    self.ctx.get_tenant().tenant_name(),
                    self.ctx.get_application_level_data_operator()?.operator(),
                    &share_specs,
                )
                .await?;

                for share_spec in &share_specs {
                    remove_share_table_object(
                        self.ctx.get_tenant().tenant_name(),
                        self.ctx.get_application_level_data_operator()?.operator(),
                        &share_spec.name,
                        &[object.clone()],
                    )
                    .await?;
                }
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
