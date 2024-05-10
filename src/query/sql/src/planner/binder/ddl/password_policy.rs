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
use databend_common_exception::Result;

use crate::binder::show::get_show_options;
use crate::binder::Binder;
use crate::plans::AlterPasswordPolicyPlan;
use crate::plans::CreatePasswordPolicyPlan;
use crate::plans::DescPasswordPolicyPlan;
use crate::plans::DropPasswordPolicyPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_password_policy(
        &mut self,
        stmt: &CreatePasswordPolicyStmt,
    ) -> Result<Plan> {
        let CreatePasswordPolicyStmt {
            create_option,
            name,
            set_options,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = CreatePasswordPolicyPlan {
            create_option: *create_option,
            tenant,
            name: name.to_string(),
            set_options: set_options.clone(),
        };
        Ok(Plan::CreatePasswordPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_password_policy(
        &mut self,
        stmt: &AlterPasswordPolicyStmt,
    ) -> Result<Plan> {
        let AlterPasswordPolicyStmt {
            if_exists,
            name,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = AlterPasswordPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
            action: action.clone(),
        };
        Ok(Plan::AlterPasswordPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_password_policy(
        &mut self,
        stmt: &DropPasswordPolicyStmt,
    ) -> Result<Plan> {
        let DropPasswordPolicyStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = DropPasswordPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropPasswordPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_password_policy(
        &mut self,
        stmt: &DescPasswordPolicyStmt,
    ) -> Result<Plan> {
        let DescPasswordPolicyStmt { name } = stmt;

        let plan = DescPasswordPolicyPlan {
            name: name.to_string(),
        };
        Ok(Plan::DescPasswordPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_password_policies(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT name, comment, options FROM system.password_policies {} order by name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowPasswordPolicies)
            .await
    }
}
