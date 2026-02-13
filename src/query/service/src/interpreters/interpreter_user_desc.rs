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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_sql::plans::DescUserPlan;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DescUserInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescUserPlan,
}

impl DescUserInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescUserPlan) -> Result<Self> {
        Ok(DescUserInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescUserInterpreter {
    fn name(&self) -> &str {
        "DescUserInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let user = user_mgr.get_user(&tenant, self.plan.user.clone()).await?;

        let names = vec![user.name.clone()];
        let hostnames = vec![user.hostname.clone()];
        let auth_types = vec![user.auth_info.get_type().to_str().to_string()];
        let default_roles = vec![user.option.default_role().cloned().unwrap_or_default()];
        let default_warehouses = vec![user.option.default_warehouse().cloned().unwrap_or_default()];
        let roles = vec![user.grants.roles().iter().sorted().join(", ").to_string()];
        let disableds = vec![user.option.disabled().cloned().unwrap_or_default()];
        let network_policies = vec![user.option.network_policy().cloned()];
        let password_policies = vec![user.option.password_policy().cloned()];
        let must_change_passwords = vec![user.option.must_change_password().cloned()];

        let workload_group = match user.option.workload_group() {
            None => vec![None],
            Some(w) => {
                let workload_mgr = GlobalInstance::get::<Arc<WorkloadMgr>>();
                workload_mgr
                    .get_by_id(w)
                    .await
                    .map_or(vec![None], |w| vec![Some(w.name)])
            }
        };

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(hostnames),
            StringType::from_data(auth_types),
            StringType::from_data(default_roles),
            StringType::from_data(default_warehouses),
            StringType::from_data(roles),
            BooleanType::from_data(disableds),
            StringType::from_opt_data(network_policies),
            StringType::from_opt_data(password_policies),
            BooleanType::from_opt_data(must_change_passwords),
            StringType::from_opt_data(workload_group),
        ])])
    }
}
