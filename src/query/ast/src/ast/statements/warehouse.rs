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
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowOnlineNodesStmt {}

impl Display for ShowOnlineNodesStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW ONLINE NODES")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowWarehousesStmt {}

impl Display for ShowWarehousesStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW WAREHOUSES")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UseWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for UseWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "USE WAREHOUSE {}", self.warehouse)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateWarehouseStmt {
    pub warehouse: Identifier,
    pub node_list: Vec<(Option<String>, u64)>,
    pub options: BTreeMap<String, String>,
}

impl Display for CreateWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE WAREHOUSE {}", self.warehouse)?;

        if !self.node_list.is_empty() {
            write!(f, "(")?;

            for (idx, (node_group, nodes)) in self.node_list.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " ASSIGN {} NODES", nodes)?;

                if let Some(node_group) = node_group {
                    write!(f, " FROM '{}'", node_group)?;
                }
            }

            write!(f, ")")?;
        }

        if !self.options.is_empty() {
            write!(f, " WITH ")?;

            for (idx, (key, value)) in self.options.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " {} = '{}'", key, value)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for DropWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP WAREHOUSE {}", self.warehouse)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct SuspendWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for SuspendWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUSPEND WAREHOUSE {}", self.warehouse)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ResumeWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for ResumeWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RESUME WAREHOUSE {}", self.warehouse)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RenameWarehouseStmt {
    pub warehouse: Identifier,
    pub new_warehouse: Identifier,
}

impl Display for RenameWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RENAME WAREHOUSE {} TO {}",
            self.warehouse, self.new_warehouse
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InspectWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for InspectWarehouseStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSPECT WAREHOUSE {}", self.warehouse)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AddWarehouseClusterStmt {
    pub warehouse: Identifier,
    pub cluster: Identifier,
    pub node_list: Vec<(Option<String>, u64)>,
    pub options: BTreeMap<String, String>,
}

impl Display for AddWarehouseClusterStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER WAREHOUSE {} ADD CLUSTER {}",
            self.warehouse, self.cluster
        )?;

        if !self.node_list.is_empty() {
            write!(f, "(")?;

            for (idx, (node_group, nodes)) in self.node_list.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " ASSIGN {} NODES", nodes)?;

                if let Some(node_group) = node_group {
                    write!(f, " FROM '{}'", node_group)?;
                }
            }

            write!(f, ")")?;
        }

        if !self.options.is_empty() {
            write!(f, " WITH ")?;

            for (idx, (key, value)) in self.options.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " {} = '{}'", key, value)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseClusterStmt {
    pub cluster: Identifier,
    pub warehouse: Identifier,
}

impl Display for DropWarehouseClusterStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER WAREHOUSE {} DROP CLUSTER {}",
            self.warehouse, self.cluster
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RenameWarehouseClusterStmt {
    pub warehouse: Identifier,
    pub cluster: Identifier,
    pub new_cluster: Identifier,
}

impl Display for RenameWarehouseClusterStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER WAREHOUSE {} RENAME CLUSTER {} TO {}",
            self.warehouse, self.cluster, self.new_cluster
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AssignWarehouseNodesStmt {
    pub warehouse: Identifier,
    pub node_list: Vec<(Identifier, Option<String>, u64)>,
}

impl Display for AssignWarehouseNodesStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER WAREHOUSE {} ASSIGN NODES (", self.warehouse)?;

        for (idx, (cluster, group, nodes)) in self.node_list.iter().enumerate() {
            if idx != 0 {
                write!(f, ", ")?;
            }

            write!(f, "ASSIGN {} NODES", nodes)?;

            if let Some(group) = group {
                write!(f, " FROM '{}'", group)?;
            }

            write!(f, " FOR {}", cluster)?;
        }

        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UnassignWarehouseNodesStmt {
    pub warehouse: Identifier,
    pub node_list: Vec<(Identifier, Option<String>, u64)>,
}

impl Display for UnassignWarehouseNodesStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER WAREHOUSE {} UNASSIGN NODES (", self.warehouse)?;

        for (idx, (cluster, group, nodes)) in self.node_list.iter().enumerate() {
            if idx != 0 {
                write!(f, ", ")?;
            }

            write!(f, "UNASSIGN {} NODES", nodes)?;

            if let Some(group) = group {
                write!(f, " FROM '{}'", group)?;
            }

            write!(f, " FOR {}", cluster)?;
        }

        write!(f, ")")?;

        Ok(())
    }
}
