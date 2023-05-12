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
use crate::plans::Plan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_or_replace_data_mask_policy(
        &mut self,
        stmt: &CreateDatamaskPolicyStmt,
    ) -> Result<Plan> {
        let CreateDatamaskPolicyStmt {
            create,
            name,
            policy,
        } = stmt;

        let plan = CreateDatamaskPolicyPlan {
            create: *create,
            name: name.to_string(),
            policy: policy.clone(),
        };
        Ok(Plan::CreateDatamaskPolicy(Box::new(plan)))
    }
}
