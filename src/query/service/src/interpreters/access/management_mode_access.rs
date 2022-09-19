// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::PlanNode;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::Plan;

pub struct ManagementModeAccess {
    ctx: Arc<QueryContext>,
}

impl ManagementModeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(ManagementModeAccess { ctx })
    }
}

#[async_trait::async_trait]
impl AccessChecker for ManagementModeAccess {
    // Check what we can do if in management mode.
    fn check(&self, plan: &PlanNode) -> Result<()> {
        // Allows for management-mode.
        if self.ctx.get_config().query.management_mode {
            return match plan {
                PlanNode::Empty(_) => Ok(()),
                _ => Err(ErrorCode::ManagementModePermissionDenied(format!(
                    "Access denied for operation:{:?} in management-mode",
                    plan.name()
                ))),
            };
        };
        Ok(())
    }

    // Check what we can do if in management mode.
    async fn check_new(&self, plan: &Plan) -> Result<()> {
        // Allows for management-mode.
        if self.ctx.get_config().query.management_mode {
            let ok = match plan {
                Plan::Query {rewrite_kind, .. } => {
                    use crate::sql::plans::RewriteKind;
                    match rewrite_kind  {
                        Some(ref v) => matches!(v,
                            RewriteKind::ShowDatabases
                            | RewriteKind::ShowTables
                            | RewriteKind::ShowEngines
                            | RewriteKind::ShowSettings
                            | RewriteKind::ShowFunctions
                            | RewriteKind::ShowUsers
                            | RewriteKind::ShowStages
                            | RewriteKind::DescribeStage
                            | RewriteKind::ShowRoles),
                        _ => false
                    }
                },
                // Show.
                Plan::ShowCreateDatabase(_)
                | Plan::ShowCreateTable(_)
                | Plan::ShowGrants(_)

                 // Database.
                | Plan::CreateDatabase(_)
                | Plan::DropDatabase(_)

                // Table.
                | Plan::DescribeTable(_)
                | Plan::CreateTable(_)
                | Plan::DropTable(_)

                // User.
                | Plan::AlterUser(_)
                | Plan::CreateUser(_)
                | Plan::DropUser(_)
                // Privilege.
                | Plan::GrantPriv(_)
                | Plan::RevokePriv(_)
                | Plan::GrantRole(_)
                | Plan::RevokeRole(_)
                // Stage.
                | Plan::CreateStage(_)
                | Plan::DropStage(_)
                | Plan::ListStage(_)

                // UDF
                | Plan::CreateUDF(_)
                | Plan::AlterUDF(_)
                | Plan::DropUDF(_)
                | Plan::UseDatabase(_)
                | Plan::Call(_) => true,
                _ => false
            };

            if !ok {
                return Err(ErrorCode::ManagementModePermissionDenied(format!(
                    "Access denied for operation:{:?} in management-mode",
                    plan.format_indent()
                )));
            }
        };

        Ok(())
    }
}
