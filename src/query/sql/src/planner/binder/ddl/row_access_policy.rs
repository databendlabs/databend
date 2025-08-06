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

use databend_common_ast::ast::CreateRowAccessPolicyStmt;
use databend_common_ast::ast::DescRowAccessPolicyStmt;
use databend_common_ast::ast::DropRowAccessPolicyStmt;
use databend_common_exception::Result;

use crate::normalize_identifier;
use crate::plans::CreateRowAccessPolicyPlan;
use crate::plans::DescRowAccessPolicyPlan;
use crate::plans::DropRowAccessPolicyPlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_row_access(
        &mut self,
        stmt: &CreateRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let CreateRowAccessPolicyStmt {
            create_option,
            name,
            description,
            definition,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let plan = CreateRowAccessPolicyPlan {
            create_option: create_option.clone().into(),
            tenant,
            name: normalize_identifier(name, &self.name_resolution_ctx).to_string(),
            row_access: definition.clone(),
            description: description.clone(),
        };
        Ok(Plan::CreateRowAccessPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_row_access(
        &mut self,
        stmt: &DropRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let DropRowAccessPolicyStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();
        let plan = DropRowAccessPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropRowAccessPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_row_access(
        &mut self,
        stmt: &DescRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let DescRowAccessPolicyStmt { name } = stmt;

        let plan = DescRowAccessPolicyPlan {
            name: name.to_string(),
        };
        Ok(Plan::DescRowAccessPolicy(Box::new(plan)))
    }
}
