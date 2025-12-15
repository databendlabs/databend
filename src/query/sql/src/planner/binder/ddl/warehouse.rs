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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use databend_common_ast::ast::AddWarehouseClusterStmt;
use databend_common_ast::ast::AssignWarehouseNodesStmt;
use databend_common_ast::ast::CreateWarehouseStmt;
use databend_common_ast::ast::DropWarehouseClusterStmt;
use databend_common_ast::ast::DropWarehouseStmt;
use databend_common_ast::ast::InspectWarehouseStmt;
use databend_common_ast::ast::RenameWarehouseClusterStmt;
use databend_common_ast::ast::RenameWarehouseStmt;
use databend_common_ast::ast::ResumeWarehouseStmt;
use databend_common_ast::ast::ShowOnlineNodesStmt;
use databend_common_ast::ast::ShowWarehousesStmt;
use databend_common_ast::ast::SuspendWarehouseStmt;
use databend_common_ast::ast::UnassignWarehouseNodesStmt;
use databend_common_ast::ast::UseWarehouseStmt;
use databend_common_exception::Result;

use crate::Binder;
use crate::plans::AddWarehouseClusterPlan;
use crate::plans::AssignWarehouseNodesPlan;
use crate::plans::CreateWarehousePlan;
use crate::plans::DropWarehouseClusterPlan;
use crate::plans::DropWarehousePlan;
use crate::plans::InspectWarehousePlan;
use crate::plans::Plan;
use crate::plans::RenameWarehouseClusterPlan;
use crate::plans::RenameWarehousePlan;
use crate::plans::ResumeWarehousePlan;
use crate::plans::SuspendWarehousePlan;
use crate::plans::UnassignWarehouseNodesPlan;
use crate::plans::UseWarehousePlan;

impl Binder {
    pub(in crate::planner::binder) fn bind_show_online_nodes(
        &mut self,
        _stmt: &ShowOnlineNodesStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowOnlineNodes)
    }

    pub(in crate::planner::binder) fn bind_show_warehouses(
        &mut self,
        _stmt: &ShowWarehousesStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowWarehouses)
    }

    pub(in crate::planner::binder) fn bind_use_warehouse(
        &mut self,
        stmt: &UseWarehouseStmt,
    ) -> Result<Plan> {
        Ok(Plan::UseWarehouse(Box::new(UseWarehousePlan {
            warehouse: stmt.warehouse.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_create_warehouse(
        &mut self,
        stmt: &CreateWarehouseStmt,
    ) -> Result<Plan> {
        let mut nodes = HashMap::with_capacity(stmt.node_list.len());

        for (group, nodes_num) in &stmt.node_list {
            match nodes.entry(group.clone()) {
                Entry::Vacant(v) => {
                    v.insert(*nodes_num);
                }
                Entry::Occupied(mut v) => {
                    *v.get_mut() += *nodes_num;
                }
            }
        }

        Ok(Plan::CreateWarehouse(Box::new(CreateWarehousePlan {
            nodes,
            warehouse: stmt.warehouse.to_string(),
            options: stmt.options.clone(),
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

    pub(in crate::planner::binder) fn bind_add_warehouse_cluster(
        &mut self,
        stmt: &AddWarehouseClusterStmt,
    ) -> Result<Plan> {
        let mut nodes = HashMap::with_capacity(stmt.node_list.len());

        for (group, nodes_num) in &stmt.node_list {
            match nodes.entry(group.clone()) {
                Entry::Vacant(v) => {
                    v.insert(*nodes_num);
                }
                Entry::Occupied(mut v) => {
                    *v.get_mut() += *nodes_num;
                }
            }
        }

        Ok(Plan::AddWarehouseCluster(Box::new(
            AddWarehouseClusterPlan {
                nodes,
                options: stmt.options.clone(),
                cluster: stmt.cluster.to_string(),
                warehouse: stmt.warehouse.to_string(),
            },
        )))
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

    pub(in crate::planner::binder) fn bind_assign_warehouse_nodes(
        &mut self,
        stmt: &AssignWarehouseNodesStmt,
    ) -> Result<Plan> {
        let mut assign_clusters = HashMap::with_capacity(stmt.node_list.len());
        for (cluster, group, num) in &stmt.node_list {
            match assign_clusters.entry(cluster.to_string()) {
                Entry::Vacant(v) => {
                    v.insert(HashMap::from([(group.clone(), *num as usize)]));
                }
                Entry::Occupied(mut v) => match v.get_mut().entry(group.clone()) {
                    Entry::Vacant(v) => {
                        v.insert(*num as usize);
                    }
                    Entry::Occupied(mut v) => {
                        *v.get_mut() += *num as usize;
                    }
                },
            }
        }

        Ok(Plan::AssignWarehouseNodes(Box::new(
            AssignWarehouseNodesPlan {
                assign_clusters,
                warehouse: stmt.warehouse.to_string(),
            },
        )))
    }

    pub(in crate::planner::binder) fn bind_unassign_warehouse_nodes(
        &mut self,
        stmt: &UnassignWarehouseNodesStmt,
    ) -> Result<Plan> {
        let mut unassign_clusters = HashMap::with_capacity(stmt.node_list.len());
        for (cluster, group, num) in &stmt.node_list {
            match unassign_clusters.entry(cluster.to_string()) {
                Entry::Vacant(v) => {
                    v.insert(HashMap::from([(group.clone(), *num as usize)]));
                }
                Entry::Occupied(mut v) => match v.get_mut().entry(group.clone()) {
                    Entry::Vacant(v) => {
                        v.insert(*num as usize);
                    }
                    Entry::Occupied(mut v) => {
                        *v.get_mut() += *num as usize;
                    }
                },
            }
        }

        Ok(Plan::UnassignWarehouseNodes(Box::new(
            UnassignWarehouseNodesPlan {
                unassign_clusters,
                warehouse: stmt.warehouse.to_string(),
            },
        )))
    }
}
