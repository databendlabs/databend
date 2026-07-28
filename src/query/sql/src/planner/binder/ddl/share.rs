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

use databend_common_ast::ast;
use databend_common_ast::ast::AlterShareAction;
use databend_common_ast::ast::AlterShareStmt;
use databend_common_ast::ast::CreateShareStmt;
use databend_common_ast::ast::DescShareStmt;
use databend_common_ast::ast::DropShareStmt;
use databend_common_ast::ast::GrantShareStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::RevokeShareStmt;
use databend_common_ast::ast::ShareGrantObjectName;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowSharesStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AlterSharePlan;
use crate::plans::AlterSharePlanAction;
use crate::plans::CreateSharePlan;
use crate::plans::DescSharePlan;
use crate::plans::DropSharePlan;
use crate::plans::GrantSharePlan;
use crate::plans::Plan;
use crate::plans::RevokeSharePlan;
use crate::plans::ShareGrantObject;
use crate::plans::ShareGrantObjectPrivilege;
use crate::plans::ShowSharesPlan;

impl Binder {
    pub(in crate::planner::binder) async fn bind_create_share(
        &self,
        stmt: &CreateShareStmt,
    ) -> Result<Plan> {
        let name = self.normalize_share_name(&stmt.name);

        Ok(Plan::CreateShare(Box::new(CreateSharePlan {
            create_option: stmt.create_option.clone().into(),
            tenant: self.ctx.get_tenant(),
            name,
            comment: stmt.comment.clone(),
        })))
    }

    pub(in crate::planner::binder) async fn bind_drop_share(
        &self,
        stmt: &DropShareStmt,
    ) -> Result<Plan> {
        Ok(Plan::DropShare(Box::new(DropSharePlan {
            tenant: self.ctx.get_tenant(),
            name: self.normalize_share_name(&stmt.name),
        })))
    }

    pub(in crate::planner::binder) async fn bind_alter_share(
        &self,
        stmt: &AlterShareStmt,
    ) -> Result<Plan> {
        let action = match &stmt.action {
            AlterShareAction::AddAccounts { accounts } => AlterSharePlanAction::AddAccounts {
                accounts: self.normalize_account_names(accounts),
            },
            AlterShareAction::RemoveAccounts { accounts } => AlterSharePlanAction::RemoveAccounts {
                accounts: self.normalize_account_names(accounts),
            },
            AlterShareAction::Set { accounts, comment } => {
                if accounts.is_none() && comment.is_none() {
                    return Err(ErrorCode::BadArguments(
                        "ALTER SHARE SET requires ACCOUNTS or COMMENT",
                    ));
                }
                AlterSharePlanAction::Set {
                    accounts: accounts
                        .as_ref()
                        .map(|accounts| self.normalize_account_names(accounts)),
                    comment: comment.clone(),
                }
            }
        };

        Ok(Plan::AlterShare(Box::new(AlterSharePlan {
            tenant: self.ctx.get_tenant(),
            if_exists: stmt.if_exists,
            name: self.normalize_share_name(&stmt.name),
            action,
        })))
    }

    pub(in crate::planner::binder) async fn bind_grant_share(
        &self,
        stmt: &GrantShareStmt,
    ) -> Result<Plan> {
        Ok(Plan::GrantShare(Box::new(GrantSharePlan {
            tenant: self.ctx.get_tenant(),
            share: self.normalize_share_name(&stmt.share),
            privilege: self.bind_share_privilege(&stmt.privilege, &stmt.object)?,
            object: self.bind_share_object(&stmt.object),
        })))
    }

    pub(in crate::planner::binder) async fn bind_revoke_share(
        &self,
        stmt: &RevokeShareStmt,
    ) -> Result<Plan> {
        Ok(Plan::RevokeShare(Box::new(RevokeSharePlan {
            tenant: self.ctx.get_tenant(),
            share: self.normalize_share_name(&stmt.share),
            privilege: self.bind_share_privilege(&stmt.privilege, &stmt.object)?,
            object: self.bind_share_object(&stmt.object),
        })))
    }

    pub(in crate::planner::binder) async fn bind_show_shares(
        &self,
        stmt: &ShowSharesStmt,
    ) -> Result<Plan> {
        let mut like = None;
        let mut limit = None;

        if let Some(show_options) = &stmt.show_options {
            limit = show_options.limit;
            match &show_options.show_limit {
                Some(ShowLimit::Like { pattern }) => like = Some(pattern.clone()),
                Some(ShowLimit::Where { .. }) => {
                    return Err(ErrorCode::Unimplemented(
                        "SHOW SHARES only supports LIKE and LIMIT",
                    ));
                }
                None => {}
            }
        }

        Ok(Plan::ShowShares(Box::new(ShowSharesPlan {
            tenant: self.ctx.get_tenant(),
            like,
            limit,
        })))
    }

    pub(in crate::planner::binder) async fn bind_desc_share(
        &self,
        stmt: &DescShareStmt,
    ) -> Result<Plan> {
        Ok(Plan::DescShare(Box::new(DescSharePlan {
            tenant: self.ctx.get_tenant(),
            provider_tenant: stmt
                .name
                .tenant
                .as_ref()
                .map(|tenant| normalize_identifier(tenant, &self.name_resolution_ctx).name),
            share: self.normalize_share_name(&stmt.name.share),
        })))
    }

    fn normalize_share_name(&self, name: &Identifier) -> String {
        normalize_identifier(name, &self.name_resolution_ctx).name
    }

    fn normalize_account_names(&self, names: &[Identifier]) -> Vec<String> {
        names
            .iter()
            .map(|name| normalize_identifier(name, &self.name_resolution_ctx).name)
            .collect()
    }

    fn bind_share_privilege(
        &self,
        privilege: &ast::ShareGrantObjectPrivilege,
        object: &ShareGrantObjectName,
    ) -> Result<ShareGrantObjectPrivilege> {
        match (privilege, object) {
            (ast::ShareGrantObjectPrivilege::Usage, ShareGrantObjectName::Database(_)) => {
                Ok(ShareGrantObjectPrivilege::Usage)
            }
            (ast::ShareGrantObjectPrivilege::Select, ShareGrantObjectName::Table(_, _)) => {
                Ok(ShareGrantObjectPrivilege::Select)
            }
            _ => Err(ErrorCode::BadArguments(
                "Only USAGE ON DATABASE and SELECT ON TABLE can be granted to a share",
            )),
        }
    }

    fn bind_share_object(&self, object: &ShareGrantObjectName) -> ShareGrantObject {
        match object {
            ShareGrantObjectName::Database(database) => ShareGrantObject::Database {
                database: normalize_identifier(database, &self.name_resolution_ctx).name,
            },
            ShareGrantObjectName::Table(database, table) => ShareGrantObject::Table {
                database: database
                    .as_ref()
                    .map(|database| normalize_identifier(database, &self.name_resolution_ctx).name),
                table: normalize_identifier(table, &self.name_resolution_ctx).name,
            },
        }
    }
}
