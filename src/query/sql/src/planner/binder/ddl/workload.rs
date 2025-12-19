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

use databend_common_ast::ast::CreateWorkloadGroupStmt;
use databend_common_ast::ast::DropWorkloadGroupStmt;
use databend_common_ast::ast::RenameWorkloadGroupStmt;
use databend_common_ast::ast::SetWorkloadGroupQuotasStmt;
use databend_common_ast::ast::ShowWorkloadGroupsStmt;
use databend_common_ast::ast::UnsetWorkloadGroupQuotasStmt;
use databend_common_exception::Result;

use crate::Binder;
use crate::plans::CreateWorkloadGroupPlan;
use crate::plans::DropWorkloadGroupPlan;
use crate::plans::Plan;
use crate::plans::RenameWorkloadGroupPlan;
use crate::plans::SetWorkloadGroupQuotasPlan;
use crate::plans::UnsetWorkloadGroupQuotasPlan;

impl Binder {
    pub(in crate::planner::binder) fn bind_show_workload_groups(
        &mut self,
        _stmt: &ShowWorkloadGroupsStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowWorkloadGroups)
    }

    pub(in crate::planner::binder) fn bind_create_workload_group(
        &mut self,
        stmt: &CreateWorkloadGroupStmt,
    ) -> Result<Plan> {
        Ok(Plan::CreateWorkloadGroup(Box::new(
            CreateWorkloadGroupPlan {
                name: stmt.name.to_string(),
                if_not_exists: stmt.if_not_exists,
                quotas: stmt.quotas.clone(),
            },
        )))
    }

    pub(in crate::planner::binder) fn bind_drop_workload_group(
        &mut self,
        stmt: &DropWorkloadGroupStmt,
    ) -> Result<Plan> {
        Ok(Plan::DropWorkloadGroup(Box::new(DropWorkloadGroupPlan {
            name: stmt.name.to_string(),
            if_exists: stmt.if_exists,
        })))
    }

    pub(in crate::planner::binder) fn bind_rename_workload_group(
        &mut self,
        stmt: &RenameWorkloadGroupStmt,
    ) -> Result<Plan> {
        Ok(Plan::RenameWorkloadGroup(Box::new(
            RenameWorkloadGroupPlan {
                name: stmt.name.to_string(),
                new_name: stmt.new_name.to_string(),
            },
        )))
    }

    pub(in crate::planner::binder) fn bind_set_workload_group_quotas(
        &mut self,
        stmt: &SetWorkloadGroupQuotasStmt,
    ) -> Result<Plan> {
        Ok(Plan::SetWorkloadGroupQuotas(Box::new(
            SetWorkloadGroupQuotasPlan {
                name: stmt.name.to_string(),
                quotas: stmt.quotas.clone(),
            },
        )))
    }

    pub(in crate::planner::binder) fn bind_unset_workload_group_quotas(
        &mut self,
        stmt: &UnsetWorkloadGroupQuotasStmt,
    ) -> Result<Plan> {
        let mut unset_names = Vec::with_capacity(stmt.quotas.len());

        for quota in &stmt.quotas {
            unset_names.push(quota.name.clone());
        }

        Ok(Plan::UnsetWorkloadGroupQuotas(Box::new(
            UnsetWorkloadGroupQuotasPlan {
                name: stmt.name.to_string(),
                quotas: unset_names,
            },
        )))
    }
}
