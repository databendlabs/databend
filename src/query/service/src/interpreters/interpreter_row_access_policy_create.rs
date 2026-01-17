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
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_sql::plans::CreateRowAccessPolicyPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_row_access_policy_feature::get_row_access_policy_handler;

use crate::interpreters::Interpreter;
use crate::meta_service_error;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct CreateRowAccessPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateRowAccessPolicyPlan,
}

impl CreateRowAccessPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateRowAccessPolicyPlan) -> Result<Self> {
        Ok(CreateRowAccessPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateRowAccessPolicyInterpreter {
    fn name(&self) -> &str {
        "CreateRowAccessPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::RowAccessPolicy)?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_row_access_policy_handler();
        let tenant = self.plan.tenant.clone();
        let res = match handler
            .create_row_access_policy(meta_api, self.plan.clone().into())
            .await
            .map_err(meta_service_error)?
        {
            Ok(reply) => {
                if let Some(current_role) = self.ctx.get_current_role() {
                    let role_api = UserApiProvider::instance().role_api(&tenant);
                    role_api
                        .grant_ownership(
                            &OwnershipObject::RowAccessPolicy {
                                policy_id: reply.id,
                            },
                            &current_role.name,
                        )
                        .await?;
                    RoleCacheManager::instance().invalidate_cache(&tenant);
                }
                Ok(PipelineBuildResult::create())
            }
            Err(_e) => {
                if self.plan.if_not_exists {
                    Ok(PipelineBuildResult::create())
                } else {
                    Err(ErrorCode::RowAccessPolicyAlreadyExists(format!(
                        "Security policy with name '{}' already exists",
                        self.plan.name
                    )))
                }
            }
        };

        res
    }
}
