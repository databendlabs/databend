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

use common_ast::ast::*;
use common_exception::Result;

use crate::binder::Binder;
use crate::plans::CreateDatamaskPolicyPlan;
use crate::plans::DescDatamaskPolicyPlan;
use crate::plans::DropDatamaskPolicyPlan;
use crate::plans::Plan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_data_mask_policy(
        &mut self,
        stmt: &CreateDatamaskPolicyStmt,
    ) -> Result<Plan> {
        let CreateDatamaskPolicyStmt {
            if_not_exists,
            name,
            policy,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let plan = CreateDatamaskPolicyPlan {
            if_not_exists: *if_not_exists,
            tenant,
            name: name.to_string(),
            policy: policy.clone(),
        };
        Ok(Plan::CreateDatamaskPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_data_mask_policy(
        &mut self,
        stmt: &DropDatamaskPolicyStmt,
    ) -> Result<Plan> {
        let DropDatamaskPolicyStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();
        let plan = DropDatamaskPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropDatamaskPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_data_mask_policy(
        &mut self,
        stmt: &DescDatamaskPolicyStmt,
    ) -> Result<Plan> {
        let DescDatamaskPolicyStmt { name } = stmt;

        let plan = DescDatamaskPolicyPlan {
            name: name.to_string(),
        };
        Ok(Plan::DescDatamaskPolicy(Box::new(plan)))
    }
}
