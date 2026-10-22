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
use databend_common_users::UserApiProvider;
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
            match &self.stmt {
                CreateNetworkRule {
                    name,
                    create_option,
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
                    provider
                        .add_network_rule(
                            &tenant,
                            NetworkRule {
                                name: name.name.clone(),
                                value_list: values.clone(),
                            },
                            &create_option.clone().into(),
                        )
                        .await?;
                }
                AlterNetworkRule {
                    name,
                    if_exists,
                    values,
                } => {
                    provider
                        .update_network_rule(
                            &tenant,
                            NetworkRule {
                                name: name.name.clone(),
                                value_list: values.clone(),
                            },
                            *if_exists,
                        )
                        .await?;
                }
                DropNetworkRule { name, if_exists } => {
                    provider
                        .drop_network_rule(&tenant, &name.name, *if_exists)
                        .await?;
                }
                CreateIntegration {
                    name,
                    create_option,
                    rules,
                    enabled,
                } => {
                    provider
                        .add_external_access_integration(
                            &tenant,
                            ExternalAccessIntegration {
                                name: name.name.clone(),
                                allowed_network_rules: rules
                                    .iter()
                                    .map(|r| r.name.clone())
                                    .collect(),
                                enabled: *enabled,
                            },
                            &create_option.clone().into(),
                        )
                        .await?;
                }
                AlterIntegration {
                    name,
                    if_exists,
                    rules,
                    enabled,
                } => {
                    provider
                        .update_external_access_integration(
                            &tenant,
                            &name.name,
                            rules
                                .as_ref()
                                .map(|rules| rules.iter().map(|r| r.name.clone()).collect()),
                            *enabled,
                            *if_exists,
                        )
                        .await?;
                }
                DropIntegration { name, if_exists } => {
                    provider
                        .drop_external_access_integration(&tenant, &name.name, *if_exists)
                        .await?;
                }
            }
            Ok(PipelineBuildResult::create())
        })
    }
}
