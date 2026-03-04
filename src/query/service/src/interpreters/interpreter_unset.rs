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

use databend_common_ast::ast::SetType;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::UnsetPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct UnSetInterpreter {
    ctx: Arc<QueryContext>,
    unset: UnsetPlan,
}

impl UnSetInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, set: UnsetPlan) -> Result<Self> {
        Ok(UnSetInterpreter { ctx, unset: set })
    }

    async fn execute_unset_settings(&self) -> Result<()> {
        let plan = self.unset.clone();
        let mut keys: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut is_globals: Vec<bool> = vec![];
        let settings = self.ctx.get_session_settings();
        let session_level = self.unset.unset_type == SetType::SettingsSession;
        settings.load_changes().await?;

        // Fetch global settings asynchronously if necessary
        let global_settings = if !session_level {
            UserApiProvider::instance()
                .setting_api(&self.ctx.get_tenant())
                .get_settings()
                .await?
        } else {
            Vec::new()
        };

        for var in plan.vars {
            let (ok, value) = match var.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => (false, String::from("")),
                setting_key => {
                    // TODO(liyz): why drop the global setting without checking the variable is global or not?
                    if !session_level {
                        settings.try_drop_global_setting(setting_key).await?;
                    }

                    let default_val = match setting_key {
                        "max_memory_usage" => {
                            let conf = GlobalConfig::instance();
                            if conf.query.common.max_server_memory_usage == 0 {
                                settings
                                    .get_default_value(setting_key)?
                                    .map(|v| v.to_string())
                            } else {
                                Some(conf.query.common.max_server_memory_usage.to_string())
                            }
                        }
                        "max_threads" => {
                            let conf = GlobalConfig::instance();
                            if conf.query.common.num_cpus == 0 {
                                settings
                                    .get_default_value(setting_key)?
                                    .map(|v| v.to_string())
                            } else {
                                Some(conf.query.common.num_cpus.to_string())
                            }
                        }
                        _ => settings
                            .get_default_value(setting_key)?
                            .map(|v| v.to_string()),
                    };
                    match default_val {
                        Some(val) => {
                            let final_val = if global_settings.is_empty() {
                                self.ctx.get_session_settings().unset_setting(&var);
                                val.to_string()
                            } else {
                                global_settings
                                    .iter()
                                    .find(|setting| setting.name.to_lowercase() == setting_key)
                                    .map_or(
                                        {
                                            self.ctx.get_session_settings().unset_setting(&var);
                                            val.to_string()
                                        },
                                        |setting| setting.value.to_string(),
                                    )
                            };
                            (true, final_val)
                        }
                        None => {
                            self.ctx
                                .push_warning(format!("Unknown setting: '{}'", setting_key));
                            (false, String::from(""))
                        }
                    }
                }
            };
            if ok {
                // set effect, this can be considered to be removed in the future.
                keys.push(var);
                values.push(value);
                is_globals.push(false);
            }
        }
        self.ctx.set_affect(QueryAffect::ChangeSettings {
            keys,
            values,
            is_globals,
        });
        Ok(())
    }

    async fn execute_unset_variables(&self) -> Result<()> {
        for var in self.unset.vars.iter() {
            self.ctx.unset_variable(var);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for UnSetInterpreter {
    fn name(&self) -> &str {
        "SetInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match self.unset.unset_type {
            SetType::SettingsSession | SetType::SettingsGlobal => {
                self.execute_unset_settings().await?
            }
            SetType::Variable => self.execute_unset_variables().await?,
            SetType::SettingsQuery => {
                return Err(ErrorCode::BadArguments(
                    "Query level setting can not be unset",
                ));
            }
        }
        Ok(PipelineBuildResult::create())
    }
}
