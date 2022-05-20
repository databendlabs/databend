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
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
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
    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .num_arguments(1)
            .management_mode_required(true)
            .user_option_flag(UserOptionFlag::TenantSetting)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant = args[0].clone();
        let user_mgr = ctx.get_user_manager();

        tracing::info!("BootstrapTenantWithAccountAdminRole: tenant={}", tenant);

        // Create account admin role.
        let mut account_admin_role = RoleInfo::new("account_admin");
        account_admin_role.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
        user_mgr
            .add_role(&tenant, account_admin_role.clone(), true)
            .await?;

        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::empty())
    }
}
