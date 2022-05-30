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

use std::convert::TryFrom;
use std::sync::Arc;

use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::UserIdentity;
use common_meta_types::UserOption;
use common_meta_types::UserOptionFlag;
use common_planners::CreateUserPlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DfAuthOption {
    pub auth_type: Option<String>,
    pub by_value: Option<String>,
}

impl DfAuthOption {
    pub fn no_password() -> Self {
        DfAuthOption {
            auth_type: Some("no_password".to_string()),
            by_value: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DfUserWithOption {
    TenantSetting,
    NoTenantSetting,
    ConfigReload,
    NoConfigReload,
}

impl TryFrom<&str> for DfUserWithOption {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "TENANTSETTING" => Ok(DfUserWithOption::TenantSetting),
            "NOTENANTSETTING" => Ok(DfUserWithOption::NoTenantSetting),
            "CONFIGRELOAD" => Ok(DfUserWithOption::ConfigReload),
            "NOCONFIGRELOAD" => Ok(DfUserWithOption::NoConfigReload),
            _ => Err(format!("Unknown user option: {}", value)),
        }
    }
}

impl DfUserWithOption {
    pub fn apply(&self, option: &mut UserOption) {
        match self {
            Self::TenantSetting => {
                option.set_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::NoTenantSetting => {
                option.unset_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::ConfigReload => {
                option.set_option_flag(UserOptionFlag::ConfigReload);
            }
            Self::NoConfigReload => {
                option.unset_option_flag(UserOptionFlag::ConfigReload);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateUser {
    pub if_not_exists: bool,
    pub user: UserIdentity,
    pub auth_option: DfAuthOption,
    pub with_options: Vec<DfUserWithOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateUser {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let mut user_option = UserOption::default();
        for option in &self.with_options {
            option.apply(&mut user_option);
        }
        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::CreateUser(
            CreateUserPlan {
                user: self.user.clone(),
                auth_info: AuthInfo::create(
                    &self.auth_option.auth_type,
                    &self.auth_option.by_value,
                )?,
                user_option,
                if_not_exists: self.if_not_exists,
            },
        ))))
    }
}
