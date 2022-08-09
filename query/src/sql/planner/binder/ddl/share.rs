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

use common_ast::ast::*;
use common_exception::Result;

use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::normalize_identifier;
use crate::sql::plans::AlterShareAccountsPlan;
use crate::sql::plans::CreateSharePlan;
use crate::sql::plans::DropSharePlan;
use crate::sql::plans::GrantShareObjectPlan;
use crate::sql::plans::Plan;
use crate::sql::plans::RevokeShareObjectPlan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_share(
        &mut self,
        stmt: &CreateShareStmt<'a>,
    ) -> Result<Plan> {
        let CreateShareStmt {
            if_not_exists,
            share,
            comment,
        } = stmt;

        let share = normalize_identifier(share, &self.name_resolution_ctx).name;

        let plan = CreateSharePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            share,
            comment: comment.as_ref().cloned(),
        };
        Ok(Plan::CreateShare(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_drop_share(
        &mut self,
        stmt: &DropShareStmt<'a>,
    ) -> Result<Plan> {
        let DropShareStmt { if_exists, share } = stmt;

        let share = normalize_identifier(share, &self.name_resolution_ctx).name;

        let plan = DropSharePlan {
            if_exists: *if_exists,
            tenant: self.ctx.get_tenant(),
            share,
        };
        Ok(Plan::DropShare(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_grant_share_object(
        &mut self,
        stmt: &GrantShareObjectStmt<'a>,
    ) -> Result<Plan> {
        let GrantShareObjectStmt {
            share,
            object,
            privilege,
        } = stmt;

        let share = normalize_identifier(share, &self.name_resolution_ctx).name;

        let plan = GrantShareObjectPlan {
            share,
            object: object.clone(),
            privilege: *privilege,
        };
        Ok(Plan::GrantShareObject(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_revoke_share_object(
        &mut self,
        stmt: &RevokeShareObjectStmt<'a>,
    ) -> Result<Plan> {
        let RevokeShareObjectStmt {
            share,
            object,
            privilege,
        } = stmt;

        let share = normalize_identifier(share, &self.name_resolution_ctx).name;

        let plan = RevokeShareObjectPlan {
            share,
            object: object.clone(),
            privilege: *privilege,
        };
        Ok(Plan::RevokeShareObject(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_share_accounts(
        &mut self,
        stmt: &AlterShareAccountsStmt<'a>,
    ) -> Result<Plan> {
        let AlterShareAccountsStmt {
            share,
            if_exists,
            tenants,
            add,
        } = stmt;

        let share = normalize_identifier(share, &self.name_resolution_ctx).name;
        let mut accounts = vec![];
        for tenant in tenants {
            accounts.push(tenant.to_string());
        }

        let plan = AlterShareAccountsPlan {
            share,
            if_exists: *if_exists,
            add: *add,
            accounts,
        };
        Ok(Plan::AlterShareAccounts(Box::new(plan)))
    }
}
