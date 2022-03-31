//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserInfo;
use common_meta_types::UserOptionFlag;
use common_meta_types::UserPrivilegeSet;
use common_tracing::tracing;

use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

pub struct BootstrapTenantProcedure {}

impl BootstrapTenantProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(BootstrapTenantProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for BootstrapTenantProcedure {
    fn name(&self) -> &str {
        "BOOTSTRAP_TENANT"
    }

    // args:
    // tenant_id: string
    // user_name: string
    // host_name: string
    // auth_type: string
    // password: string
    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .num_arguments(5)
            .management_mode_required(true)
            .user_option_flag(UserOptionFlag::TenantSetting)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant = args[0].clone();
        let user_name = args[1].clone();
        let host_name = args[2].clone();
        let auth_type = args[3].clone();
        let password = args[4].clone();
        let user_mgr = ctx.get_user_manager();

        tracing::info!(
            "BootstrapTenant: tenant={}, user_name={}, host_name={}, auth_type={}",
            tenant,
            user_name,
            host_name,
            auth_type
        );

        // Create account admin role.
        let mut account_admin_role = RoleInfo::new("account_admin".to_string());
        account_admin_role.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
        user_mgr
            .add_role(&tenant, account_admin_role.clone(), true)
            .await?;

        // Create user.
        let auth_info = AuthInfo::create(&Some(auth_type), &Some(password))?;
        let mut user_info = UserInfo::new(user_name.clone(), host_name.clone(), auth_info);
        user_info.grants.grant_role(account_admin_role.identity());
        user_mgr.add_user(&tenant, user_info, true).await?;

        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::empty())
    }
}
