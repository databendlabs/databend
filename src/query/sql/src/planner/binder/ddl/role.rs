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

use databend_common_ast::ast::SecondaryRolesOption;
use databend_common_exception::Result;

use crate::plans::Plan;
use crate::plans::SetRolePlan;
use crate::plans::SetSecondaryRolesPlan;
use crate::BindContext;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_set_role(
        &mut self,
        _bind_context: &BindContext,
        is_default: bool,
        role_name: &str,
    ) -> Result<Plan> {
        Ok(Plan::SetRole(Box::new(SetRolePlan {
            is_default,
            role_name: role_name.to_string(),
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_set_secondary_roles(
        &mut self,
        _bind_context: &BindContext,
        option: &SecondaryRolesOption,
    ) -> Result<Plan> {
        let plan = match option {
            SecondaryRolesOption::None => SetSecondaryRolesPlan::None,
            SecondaryRolesOption::All => SetSecondaryRolesPlan::All,
        };
        Ok(Plan::SetSecondaryRoles(Box::new(plan)))
    }
}
