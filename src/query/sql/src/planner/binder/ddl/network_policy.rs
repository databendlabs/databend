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

use cidr::Ipv4Cidr;
use databend_common_ast::ast::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::plans::AlterNetworkPolicyPlan;
use crate::plans::CreateNetworkPolicyPlan;
use crate::plans::DescNetworkPolicyPlan;
use crate::plans::DropNetworkPolicyPlan;
use crate::plans::Plan;
use crate::plans::ShowNetworkPoliciesPlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_network_policy(
        &mut self,
        stmt: &CreateNetworkPolicyStmt,
    ) -> Result<Plan> {
        let CreateNetworkPolicyStmt {
            create_option,
            name,
            allowed_ip_list,
            blocked_ip_list,
            comment,
        } = stmt;

        for ip in allowed_ip_list {
            if ip.parse::<Ipv4Cidr>().is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid ip address {}",
                    ip
                )));
            }
        }
        if let Some(blocked_ip_list) = blocked_ip_list {
            for ip in blocked_ip_list {
                if ip.parse::<Ipv4Cidr>().is_err() {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid ip address {}",
                        ip
                    )));
                }
            }
        }

        let tenant = self.ctx.get_tenant();

        let plan = CreateNetworkPolicyPlan {
            create_option: *create_option,
            tenant,
            name: name.to_string(),
            allowed_ip_list: allowed_ip_list.clone(),
            blocked_ip_list: blocked_ip_list.clone().unwrap_or_default(),
            comment: comment.clone().unwrap_or_default(),
        };
        Ok(Plan::CreateNetworkPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_network_policy(
        &mut self,
        stmt: &AlterNetworkPolicyStmt,
    ) -> Result<Plan> {
        let AlterNetworkPolicyStmt {
            if_exists,
            name,
            allowed_ip_list,
            blocked_ip_list,
            comment,
        } = stmt;

        if let Some(allowed_ip_list) = allowed_ip_list {
            for ip in allowed_ip_list {
                if ip.parse::<Ipv4Cidr>().is_err() {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid ip address {}",
                        ip
                    )));
                }
            }
        }
        if let Some(blocked_ip_list) = blocked_ip_list {
            for ip in blocked_ip_list {
                if ip.parse::<Ipv4Cidr>().is_err() {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid ip address {}",
                        ip
                    )));
                }
            }
        }

        let tenant = self.ctx.get_tenant();

        let plan = AlterNetworkPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
            allowed_ip_list: allowed_ip_list.clone(),
            blocked_ip_list: blocked_ip_list.clone(),
            comment: comment.clone(),
        };
        Ok(Plan::AlterNetworkPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_network_policy(
        &mut self,
        stmt: &DropNetworkPolicyStmt,
    ) -> Result<Plan> {
        let DropNetworkPolicyStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = DropNetworkPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropNetworkPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_network_policy(
        &mut self,
        stmt: &DescNetworkPolicyStmt,
    ) -> Result<Plan> {
        let DescNetworkPolicyStmt { name } = stmt;

        let plan = DescNetworkPolicyPlan {
            name: name.to_string(),
        };
        Ok(Plan::DescNetworkPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_network_policies(&mut self) -> Result<Plan> {
        let plan = ShowNetworkPoliciesPlan {};
        Ok(Plan::ShowNetworkPolicies(Box::new(plan)))
    }
}
