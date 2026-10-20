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

use databend_common_ast::ast::ExternalAccessStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::UserApiProvider;
use databend_common_users::network_rule_endpoint;
use databend_meta_client::types::MatchSeq;
use futures::future::BoxFuture;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct ExternalAccessInterpreter {
    ctx: Arc<QueryContext>,
    stmt: ExternalAccessStmt,
}

impl ExternalAccessInterpreter {
    pub fn new(ctx: Arc<QueryContext>, stmt: ExternalAccessStmt) -> Self {
        Self { ctx, stmt }
    }
}

#[async_trait::async_trait]
impl Interpreter for ExternalAccessInterpreter {
    fn name(&self) -> &str {
        "ExternalAccessInterpreter"
    }
    fn is_ddl(&self) -> bool {
        true
    }

    fn execute2(&self) -> BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            use ExternalAccessStmt::*;
            let tenant = self.ctx.get_tenant();
            let provider = UserApiProvider::instance();
            let rules_api = provider.network_rule_api(&tenant);
            let integrations_api = provider.external_access_integration_api(&tenant);
            let create_option = |if_not_exists| {
                if if_not_exists {
                    CreateOption::CreateIfNotExists
                } else {
                    CreateOption::Create
                }
            };
            match &self.stmt {
                CreateNetworkRule {
                    name,
                    if_not_exists,
                    rule_type,
                    mode,
                    values,
                } => {
                    if !rule_type.name.eq_ignore_ascii_case("HOST_PORT")
                        || !mode.name.eq_ignore_ascii_case("EGRESS")
                    {
                        return Err(ErrorCode::InvalidArgument(
                            "Only TYPE = HOST_PORT and MODE = EGRESS are supported",
                        ));
                    }
                    for value in values {
                        network_rule_endpoint(value)?;
                    }
                    rules_api
                        .add(
                            NetworkRule {
                                name: name.name.clone(),
                                value_list: values.clone(),
                            },
                            &create_option(*if_not_exists),
                        )
                        .await?;
                }
                AlterNetworkRule { name, values } => {
                    for value in values {
                        network_rule_endpoint(value)?;
                    }
                    rules_api
                        .update(
                            NetworkRule {
                                name: name.name.clone(),
                                value_list: values.clone(),
                            },
                            MatchSeq::GE(1),
                        )
                        .await?;
                }
                DropNetworkRule { name, if_exists } => {
                    if let Err(e) = rules_api.remove(&name.name, MatchSeq::GE(1)).await {
                        if !(*if_exists && e.is_business_error()) {
                            return Err(e.into());
                        }
                    }
                }
                CreateIntegration {
                    name,
                    if_not_exists,
                    rules,
                    enabled,
                } => {
                    for rule in rules {
                        rules_api.get(&rule.name, MatchSeq::GE(1)).await?;
                    }
                    integrations_api
                        .add(
                            ExternalAccessIntegration {
                                name: name.name.clone(),
                                allowed_network_rules: rules
                                    .iter()
                                    .map(|r| r.name.clone())
                                    .collect(),
                                enabled: *enabled,
                            },
                            &create_option(*if_not_exists),
                        )
                        .await?;
                }
                AlterIntegration {
                    name,
                    rules,
                    enabled,
                } => {
                    if let Some(rules) = rules {
                        for rule in rules {
                            rules_api.get(&rule.name, MatchSeq::GE(1)).await?;
                        }
                    }
                    integrations_api
                        .cas_with(&name.name, MatchSeq::GE(1), |old| {
                            let mut integration = old.data;
                            if let Some(rules) = rules {
                                integration.allowed_network_rules =
                                    rules.iter().map(|r| r.name.clone()).collect();
                            }
                            if let Some(enabled) = enabled {
                                integration.enabled = *enabled;
                            }
                            integration
                        })
                        .await?;
                }
                DropIntegration { name, if_exists } => {
                    if let Err(e) = integrations_api.remove(&name.name, MatchSeq::GE(1)).await {
                        if !(*if_exists && e.is_business_error()) {
                            return Err(e.into());
                        }
                    }
                }
            }
            Ok(PipelineBuildResult::create())
        })
    }
}
