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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::CreateDictionaryPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Clone, Debug)]
pub struct CreateDictionaryInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDictionaryPlan,
}

impl CreateDictionaryInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateDictionaryPlan) -> Result<Self> {
        Ok(CreateDictionaryInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateDictionaryInterpreter {
    fn name(&self) -> &str {
        "CreateDictionaryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;
        let quota_api = UserApiProvider::instance().tenant_quota_api(tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        if quota.max_dictionaries_per_database > 0 {
            let req = ListDictionaryReq {
                tenant: self.plan.tenant.clone(),
                db_id: self.plan.database_id,
            };
            let dictionaries = catalog.list_dictionaries(req).await?;
            if dictionaries.len() >= quota.max_dictionaries_per_database as usize {
                return Err(ErrorCode::TenantQuotaExceeded(format!(
                    "Max dictionaries per database quota exceeded: {}",
                    quota.max_dictionaries_per_database
                )));
            }
        }

        // let dictionary_meta = self.plan.meta.clone();
        // let dict_ident =
        //     DictionaryIdentity::new(self.plan.database_id, self.plan.dictionary.clone());
        // let dictionary_ident = TenantDictionaryIdent::new(tenant, dict_ident);
        // let req = CreateDictionaryReq {
        //     dictionary_ident,
        //     dictionary_meta,
        // };
        // let reply = catalog.create_dictionary(req).await?;

        // // Grant the ownership of the dictionary to the current role.
        // if let Some(current_role) = self.ctx.get_current_role() {
        //     let tenant = self.ctx.get_tenant();
        //     let role_api = UserApiProvider::instance().role_api(&tenant);
        //     role_api
        //         .grant_ownership(
        //             &OwnershipObject::Dictionary {
        //                 catalog_name: self.plan.catalog.clone(),
        //                 db_id: self.plan.database_id,
        //                 dict_id: reply.dictionary_id,
        //             },
        //             &current_role.name,
        //         )
        //         .await?;
        //     RoleCacheManager::instance().invalidate_cache(&tenant);
        // }

        Ok(PipelineBuildResult::create())
    }
}
