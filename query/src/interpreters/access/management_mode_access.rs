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
use common_planners::PlanNode;

use crate::sessions::QueryContext;

pub struct ManagementModeAccess {
    ctx: Arc<QueryContext>,
}

impl ManagementModeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        ManagementModeAccess { ctx }
    }

    // Check what we can do if in management mode.
    pub fn check(&self, plan: &PlanNode) -> Result<()> {
        // Allows for management-mode.
        if self.ctx.get_config().query.management_mode {
            return match plan {
                PlanNode::Empty(_)
                // Database.
                | PlanNode::CreateDatabase(_)
                | PlanNode::ShowCreateDatabase(_)
                | PlanNode::DropDatabase(_)
                | PlanNode::ShowDatabases(_)

                // Table.
                | PlanNode::CreateTable(_)
                | PlanNode::DropTable(_)
                | PlanNode::DescribeTable(_)
                | PlanNode::ShowCreateTable(_)

                // User.
                | PlanNode::CreateUser(_)
                | PlanNode::DropUser(_)
                | PlanNode::AlterUser(_)
                | PlanNode::GrantPrivilege(_)
                | PlanNode::RevokePrivilege(_)
                | PlanNode::ShowGrants(_)

                // Stage.
                | PlanNode::CreateUserStage(_)
                | PlanNode::DropUserStage(_)
                | PlanNode::DescribeUserStage(_)

                // UDF.
                | PlanNode::CreateUserUDF(_)
                | PlanNode::DropUserUDF(_)
                | PlanNode::AlterUserUDF(_)

                // USE.
                | PlanNode::UseDatabase(_)
                | PlanNode::Select(_)// Allow select from system.* tables, like show tables;

                // Admin.
                | PlanNode::AdminUseTenant(_) => Ok(()),
                _ => Err(ErrorCode::ManagementModePermissionDenied(format!(
                    "Access denied for operation:{:?} in management-mode",
                    plan.name()
                ))),
            };
        } else {
            match plan {
                PlanNode::AdminUseTenant(_) => Err(ErrorCode::ManagementModePermissionDenied(
                    "Access denied:'USE TENANT' only used in management-mode",
                )),
                _ => Ok(()),
            }
        }
    }
}
