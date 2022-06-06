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

use common_ast::ast::CreateUserStmt;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::UserOption;
use common_planners::CreateUserPlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;

impl Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_user(
        &mut self,
        stmt: &CreateUserStmt,
    ) -> Result<Plan> {
        let mut user_option = UserOption::default();
        for option in &stmt.role_options {
            option.apply(&mut user_option);
        }
        let plan = CreateUserPlan {
            user: stmt.user.clone(),
            auth_info: AuthInfo::create2(&stmt.auth_option.auth_type, &stmt.auth_option.password)?,
            user_option,
            if_not_exists: stmt.if_not_exists,
        };
        Ok(Plan::CreateUser(Box::new(plan)))
    }
}
