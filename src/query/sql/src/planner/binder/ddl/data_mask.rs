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

use databend_common_ast::ast::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

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
            create_option,
            name,
            policy,
        } = stmt;

        // check if input type match to the return type
        let return_type = policy.return_type.to_string().to_lowercase();
        let policy_data_type = policy.args[0].arg_type.to_string().to_lowercase();
        if return_type != policy_data_type {
            return Err(ErrorCode::UnmatchMaskPolicyReturnType(format!(
                "arg '{}' data type {} does not match to the mask policy return type {}",
                policy.args[0].arg_name, policy_data_type, return_type,
            )));
        }

        let tenant = self.ctx.get_tenant();
        let plan = CreateDatamaskPolicyPlan {
            create_option: create_option.clone().into(),
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
