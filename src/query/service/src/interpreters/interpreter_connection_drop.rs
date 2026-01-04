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
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_sql::plans::DropConnectionPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropConnectionInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropConnectionPlan,
}

impl DropConnectionInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropConnectionPlan) -> Result<Self> {
        Ok(DropConnectionInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropConnectionInterpreter {
    fn name(&self) -> &str {
        "DropConnectionInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_connection_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        // we should do `drop ownership` after actually drop object, and object maybe not exists.
        // drop the ownership
        if self
            .ctx
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            let role_api = UserApiProvider::instance().role_api(&tenant);
            let owner_object = OwnershipObject::Connection {
                name: self.plan.name.clone(),
            };
            role_api.revoke_ownership(&owner_object).await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        user_mgr
            .drop_connection(&tenant, &plan.name, plan.if_exists)
            .await?;

        // Clean up tag associations for this connection.
        // Since Connection does not support UNDROP, we must remove tag bindings
        // to prevent orphaned references that would block tag deletion.
        //
        // Design notes:
        // - This runs regardless of whether the connection existed, so repeated
        //   `DROP CONNECTION IF EXISTS` can clean up any leftover tag references
        //   from a previous failed attempt.
        // - Tag associations are indexed by connection name (not a unique ID), so:
        //   - Only this connection's tags are affected; other connections are safe.
        //   - If a new connection with the same name is created before cleanup,
        //     it may "inherit" orphaned tag associations. Users can fix this
        //     via `ALTER CONNECTION UNSET TAG` or `DROP CONNECTION IF EXISTS`.
        // - Concurrent `DROP CONNECTION` and `DROP TAG` may cause `DROP TAG` to
        //   fail due to existing references; retry will succeed after cleanup.
        let taggable_object = TaggableObject::Connection {
            name: plan.name.clone(),
        };
        let meta_api = user_mgr.get_meta_store_client();
        let tag_values = meta_api.get_object_tags(&tenant, &taggable_object).await?;
        if !tag_values.is_empty() {
            let tag_ids: Vec<u64> = tag_values.iter().map(|v| v.tag_id).collect();
            let req = UnsetObjectTagsReq {
                tenant: tenant.clone(),
                taggable_object,
                tags: tag_ids,
            };
            meta_api.unset_object_tags(req).await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
