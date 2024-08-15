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

use std::collections::BTreeMap;

use databend_common_ast::ast::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::share::ShareCredential;
use databend_common_meta_app::share::ShareCredentialHmac;
use databend_common_meta_app::share::ShareEndpointIdent;
use itertools::Itertools;

use crate::binder::Binder;
use crate::plans::AlterShareTenantsPlan;
use crate::plans::CreateShareEndpointPlan;
use crate::plans::CreateSharePlan;
use crate::plans::DescSharePlan;
use crate::plans::DropShareEndpointPlan;
use crate::plans::DropSharePlan;
use crate::plans::GrantShareObjectPlan;
use crate::plans::Plan;
use crate::plans::RevokeShareObjectPlan;
use crate::plans::ShowGrantTenantsOfSharePlan;
use crate::plans::ShowObjectGrantPrivilegesPlan;
use crate::plans::ShowShareEndpointPlan;
use crate::plans::ShowSharesPlan;

fn try_convert_share_credential(p: &BTreeMap<String, String>) -> Result<Option<ShareCredential>> {
    let typ = p.get("TYPE");
    if let Some(typ) = typ {
        if typ == "HMAC" {
            if let Some(key) = p.get("KEY") {
                Ok(Some(ShareCredential::HMAC(ShareCredentialHmac {
                    key: key.clone(),
                })))
            } else {
                Err(ErrorCode::ErrorShareEndpointCredential(
                    "HMAC Credential miss key",
                ))
            }
        } else {
            Err(ErrorCode::ErrorShareEndpointCredential(format!(
                "Unsupported Credential type {}",
                typ
            )))
        }
    } else {
        Ok(None)
    }
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_share_endpoint(
        &mut self,
        stmt: &CreateShareEndpointStmt,
    ) -> Result<Plan> {
        let CreateShareEndpointStmt {
            create_option,
            endpoint,
            url,
            credential_options,
            args,
            comment,
        } = stmt;

        let endpoint = endpoint.name();

        let credential = try_convert_share_credential(credential_options)?;

        let plan = CreateShareEndpointPlan {
            create_option: create_option.clone().into(),
            endpoint: ShareEndpointIdent::new(self.ctx.get_tenant(), endpoint),
            credential,
            url: format!("{}://{}{}", url.protocol, url.name, url.path),
            args: args.clone(),
            comment: comment.as_ref().cloned(),
        };
        Ok(Plan::CreateShareEndpoint(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_share_endpoint(
        &mut self,
        _stmt: &ShowShareEndpointStmt,
    ) -> Result<Plan> {
        let plan = ShowShareEndpointPlan {
            tenant: self.ctx.get_tenant(),
        };
        Ok(Plan::ShowShareEndpoint(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_share_endpoint(
        &mut self,
        stmt: &DropShareEndpointStmt,
    ) -> Result<Plan> {
        let DropShareEndpointStmt {
            if_exists,
            endpoint,
        } = stmt;
        let plan = DropShareEndpointPlan {
            if_exists: *if_exists,
            tenant: self.ctx.get_tenant(),
            endpoint: endpoint.to_string(),
        };
        Ok(Plan::DropShareEndpoint(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_share(
        &mut self,
        stmt: &CreateShareStmt,
    ) -> Result<Plan> {
        let CreateShareStmt {
            if_not_exists,
            share,
            comment,
        } = stmt;

        let share = share.name();

        let plan = CreateSharePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            share,
            comment: comment.as_ref().cloned(),
        };
        Ok(Plan::CreateShare(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_share(
        &mut self,
        stmt: &DropShareStmt,
    ) -> Result<Plan> {
        let DropShareStmt { if_exists, share } = stmt;

        let share = share.name();

        let plan = DropSharePlan {
            if_exists: *if_exists,
            tenant: self.ctx.get_tenant(),
            share,
        };
        Ok(Plan::DropShare(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_grant_share_object(
        &mut self,
        stmt: &GrantShareObjectStmt,
    ) -> Result<Plan> {
        let GrantShareObjectStmt {
            share,
            object,
            privilege,
        } = stmt;

        let share = share.name();

        let plan = GrantShareObjectPlan {
            share,
            object: object.clone().into(),
            privilege: privilege.clone().into(),
        };
        Ok(Plan::GrantShareObject(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_revoke_share_object(
        &mut self,
        stmt: &RevokeShareObjectStmt,
    ) -> Result<Plan> {
        let RevokeShareObjectStmt {
            share,
            object,
            privilege,
        } = stmt;

        let share = share.name();

        let plan = RevokeShareObjectPlan {
            share,
            object: object.clone().into(),
            privilege: privilege.clone().into(),
        };
        Ok(Plan::RevokeShareObject(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_share_accounts(
        &mut self,
        stmt: &AlterShareTenantsStmt,
    ) -> Result<Plan> {
        let AlterShareTenantsStmt {
            share,
            if_exists,
            tenants,
            is_add,
        } = stmt;

        let share = share.name();

        let plan = AlterShareTenantsPlan {
            share,
            if_exists: *if_exists,
            is_add: *is_add,
            accounts: tenants.iter().map(|v| v.to_string()).collect_vec(),
        };
        Ok(Plan::AlterShareTenants(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_share(
        &mut self,
        stmt: &DescShareStmt,
    ) -> Result<Plan> {
        let DescShareStmt { share } = stmt;

        let share = share.name();

        let plan = DescSharePlan { share };
        Ok(Plan::DescShare(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_shares(
        &mut self,
        _stmt: &ShowSharesStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowShares(Box::new(ShowSharesPlan {})))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_object_grant_privileges(
        &mut self,
        stmt: &ShowObjectGrantPrivilegesStmt,
    ) -> Result<Plan> {
        let ShowObjectGrantPrivilegesStmt { object } = stmt;

        let plan = ShowObjectGrantPrivilegesPlan {
            object: object.clone().into(),
        };
        Ok(Plan::ShowObjectGrantPrivileges(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_grants_of_share(
        &mut self,
        stmt: &ShowGrantsOfShareStmt,
    ) -> Result<Plan> {
        let ShowGrantsOfShareStmt { share_name } = stmt;

        let plan = ShowGrantTenantsOfSharePlan {
            share_name: share_name.clone(),
        };
        Ok(Plan::ShowGrantTenantsOfShare(Box::new(plan)))
    }
}
