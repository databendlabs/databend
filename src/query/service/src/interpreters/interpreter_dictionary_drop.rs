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
use databend_common_meta_app::schema::tenant_dictionary_ident::TenantDictionaryIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_sql::plans::DropDictionaryPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropDictionaryInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropDictionaryPlan,
}

impl DropDictionaryInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropDictionaryPlan) -> Result<Self> {
        Ok(DropDictionaryInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropDictionaryInterpreter {
    fn name(&self) -> &str {
        "DropDictionaryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let tenant = self.ctx.get_tenant();
        let db_id = self.plan.database_id;
        let dict_name = self.plan.dictionary.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let dict_ident = TenantDictionaryIdent::new(
            tenant,
            DictionaryIdentity::new(db_id, dict_name.to_string()),
        );
        let reply = catalog.drop_dictionary(dict_ident.clone()).await?;

        let get_resp = catalog.get_dictionary(dict_ident.clone()).await?;
        let dict_id;
        match get_resp {
            Some(reply) => {
                dict_id = reply.dictionary_id;
            }
            None => {
                return Ok(PipelineBuildResult::create());
            }
        }

        // // drop the ownership
        // let role_api = UserApiProvider::instance().role_api(&self.plan.tenant);
        // let owner_object = OwnershipObject::Dictionary {
        //     catalog_name: catalog_name.to_string(),
        //     db_id,
        //     dict_id,
        // };

        // role_api.revoke_ownership(&owner_object).await?;
        // RoleCacheManager::instance().invalidate_cache(&tenant);

        Ok(PipelineBuildResult::create())
    }
}
