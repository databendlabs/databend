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
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_sql::plans::DropDatabasePlan;
use databend_common_storages_share::remove_share_db_dir;
use databend_common_storages_share::save_share_spec;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropDatabasePlan,
}

impl DropDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropDatabasePlan) -> Result<Self> {
        Ok(DropDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropDatabaseInterpreter {
    fn name(&self) -> &str {
        "DropDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        // unset the ownership of the database, the database may not exists.
        let db = catalog.get_database(&tenant, &self.plan.database).await;
        if let Ok(db) = db {
            let role_api = UserApiProvider::instance().role_api(&tenant);
            let owner_object = OwnershipObject::Database {
                catalog_name: self.plan.catalog.clone(),
                db_id: db.get_db_info().database_id.db_id,
            };

            role_api
                .revoke_ownership(
                    &owner_object,
                    self.ctx.get_settings().get_enable_query_result_cache()?,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        // actual drop database
        let reply = catalog.drop_database(self.plan.clone().into()).await?;

        // handle share cleanups with the DropDatabaseReply
        if let Some(share_specs) = reply.share_specs {
            // since db is dropped, first we need to clean share db dir
            remove_share_db_dir(
                self.ctx.get_tenant().tenant_name(),
                self.ctx.get_application_level_data_operator()?.operator(),
                reply.db_id,
                &share_specs,
            )
            .await?;

            // then write the new share spec
            save_share_spec(
                self.ctx.get_tenant().tenant_name(),
                self.ctx.get_application_level_data_operator()?.operator(),
                &share_specs,
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
