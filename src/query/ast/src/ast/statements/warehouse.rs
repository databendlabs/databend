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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowWarehousesStmt {}

impl Display for ShowWarehousesStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW WAREHOUSES")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateWarehouseStmt {
    pub warehouse: Identifier,
}

impl Display for CreateWarehouseStmt {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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
pub struct AddWarehouseClusterStmt {}

impl Display for AddWarehouseClusterStmt {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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
pub struct AddWarehouseClusterNodeStmt {}

impl Display for AddWarehouseClusterNodeStmt {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseClusterNodeStmt {
    // warehouse:String,
    // cluster: String,
}

impl Display for DropWarehouseClusterNodeStmt {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
