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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use databend_common_ast::ast::CreateWarehouseStmt;
use databend_common_ast::ast::DropWarehouseClusterStmt;
use databend_common_ast::ast::DropWarehouseStmt;
use databend_common_ast::ast::InspectWarehouseStmt;
use databend_common_ast::ast::RenameWarehouseClusterStmt;
use databend_common_ast::ast::RenameWarehouseStmt;
use databend_common_ast::ast::ResumeWarehouseStmt;
use databend_common_ast::ast::ShowWarehousesStmt;
use databend_common_ast::ast::SuspendWarehouseStmt;
use databend_common_exception::Result;

use crate::plans::CreateWarehousePlan;
use crate::plans::DropWarehouseClusterPlan;
use crate::plans::DropWarehousePlan;
use crate::plans::InspectWarehousePlan;
use crate::plans::Plan;
use crate::plans::RenameWarehouseClusterPlan;
use crate::plans::RenameWarehousePlan;
use crate::plans::ResumeWarehousePlan;
use crate::plans::SuspendWarehousePlan;
use crate::Binder;

impl Binder {
    pub(in crate::planner::binder) fn bind_show_warehouses(
        &mut self,
        _stmt: &ShowWarehousesStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowWarehouses)
    }

    pub(in crate::planner::binder) fn bind_create_warehouse(
        &mut self,
        stmt: &CreateWarehouseStmt,
    ) -> Result<Plan> {
        let mut nodes = HashMap::with_capacity(stmt.node_list.len());

        for (group, nodes_num) in stmt.node_list {
            match nodes.entry(group) {
                Entry::Vacant(mut v) => {
                    v.insert(nodes_num);
                }
                Entry::Occupied(mut v) => {
                    *v.get_mut() += nodes_num;
                }
            }
        }

        Ok(Plan::CreateWarehouse(Box::new(CreateWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
            nodes: Default::default(),
            options: stmt.options,
        })))
    }

    pub(in crate::planner::binder) fn bind_drop_warehouse(
        &mut self,
        stmt: &DropWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::DropWarehouse(Box::new(DropWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_resume_warehouse(
        &mut self,
        stmt: &ResumeWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::ResumeWarehouse(Box::new(ResumeWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_suspend_warehouse(
        &mut self,
        stmt: &SuspendWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::SuspendWarehouse(Box::new(SuspendWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_inspect_warehouse(
        &mut self,
        stmt: &InspectWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::InspectWarehouse(Box::new(InspectWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_rename_warehouse(
        &mut self,
        stmt: &RenameWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::RenameWarehouse(Box::new(RenameWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
            new_warehouse: stmt.new_warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_drop_warehouse_cluster(
        &mut self,
        stmt: &DropWarehouseClusterStmt,
    ) -> Result<Plan> {
        Ok(Plan::DropWarehouseCluster(Box::new(
            DropWarehouseClusterPlan {
                warehouse: stmt.warehouse.to_string(),
                cluster: stmt.cluster.to_string(),
            },
        )))
    }

    pub(in crate::planner::binder) fn bind_rename_warehouse_cluster(
        &mut self,
        stmt: &RenameWarehouseClusterStmt,
    ) -> Result<Plan> {
        Ok(Plan::RenameWarehouseCluster(Box::new(
            RenameWarehouseClusterPlan {
                warehouse: stmt.warehouse.to_string(),
                cluster: stmt.cluster.to_string(),
                new_cluster: stmt.new_cluster.to_string(),
            },
        )))
    }
}
