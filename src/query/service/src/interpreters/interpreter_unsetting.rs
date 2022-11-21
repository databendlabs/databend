// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_settings::ScopeLevel;
use common_sql::plans::UnSettingPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct UnSettingInterpreter {
    ctx: Arc<QueryContext>,
    set: UnSettingPlan,
}

impl UnSettingInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, set: UnSettingPlan) -> Result<Self> {
        Ok(UnSettingInterpreter { ctx, set })
    }
}

#[async_trait::async_trait]
impl Interpreter for UnSettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.set.clone();
        for var in plan.vars {
            let (ok, value) = match var.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => (false, String::from("")),
                setting => {
                    let settings = self.ctx.get_settings();
                    match settings.get_setting_level(setting)? {
                        ScopeLevel::Global => {
                            self.ctx.get_settings().try_drop_setting(setting)?;
                        }
                        ScopeLevel::Session => {}
                    }
                    let default_val = settings.check_and_get_default_value(setting)?.to_string();
                    (true, default_val)
                }
            };
            if ok {
                // reset the system.settings
                self.ctx
                    .get_settings()
                    .set_settings(var.clone(), value.clone(), false)?;
                // reset the ctx
                self.ctx.set_affect(QueryAffect::ChangeSetting {
                    key: var,
                    value,
                    is_global: false,
                })
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
