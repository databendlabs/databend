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

use chrono_tz::Tz;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::SettingPlan;
use databend_common_sql::plans::VarValue;
use databend_common_users::UserApiProvider;
use minitrace::func_name;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SettingInterpreter {
    ctx: Arc<QueryContext>,
    set: SettingPlan,
}

impl SettingInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, set: SettingPlan) -> Result<Self> {
        Ok(SettingInterpreter { ctx, set })
    }

    async fn set_setting_by_var(&self, var: &VarValue, value: String) -> Result<()> {
        let settings = self.ctx.get_shared_settings();

        match var.is_global {
            true => {
                settings
                    .set_global_setting(var.variable.clone(), value)
                    .await
            }
            false => settings.set_setting(var.variable.clone(), value),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for SettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.set.clone();
        let mut keys: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut is_globals: Vec<bool> = vec![];
        for var in plan.vars {
            let ok = match var.variable.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => false,
                "timezone" => {
                    // check if the timezone is valid
                    let tz = var.value.trim_matches(|c| c == '\'' || c == '\"');
                    let _ = tz.parse::<Tz>().map_err(|_| {
                        ErrorCode::InvalidTimezone(format!("Invalid Timezone: {}", var.value))
                    })?;
                    self.set_setting_by_var(&var, tz.to_string()).await?;
                    true
                }
                // TODO: if account_admin is built-in meta in future, we need process set sandbox_tenant in there.
                // Like: https://github.com/datafuselabs/databend/pull/14451/files#diff-a26c9dfc9c0a37f5efa19e2b16006732b9023f42ee47cbe37fe461fb46b9dfc0R82-R85
                "sandbox_tenant" => {
                    // only used in sqlogictest, it will create a sandbox tenant on every sqlogictest cases
                    // and switch to it by SET sandbox_tenant = xxx;
                    let config = GlobalConfig::instance();
                    let tenant = var.value.clone();
                    if config.query.internal_enable_sandbox_tenant && !tenant.is_empty() {
                        UserApiProvider::try_create_simple(
                            config.meta.to_meta_grpc_client_conf(),
                            &Tenant::new_or_err(tenant, func_name!())?,
                        )
                        .await?;
                    }

                    self.set_setting_by_var(&var, var.value.clone()).await?;
                    true
                }
                _ => {
                    self.set_setting_by_var(&var, var.value.clone()).await?;
                    true
                }
            };
            if ok {
                keys.push(var.variable.clone());
                values.push(var.value.clone());
                is_globals.push(var.is_global);
            }
        }
        self.ctx.set_affect(QueryAffect::ChangeSettings {
            keys,
            values,
            is_globals,
        });

        Ok(PipelineBuildResult::create())
    }
}
