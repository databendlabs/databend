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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowWarehousesStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct SuspendWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ResumeWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RenameWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InspectWarehouseStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AddWarehouseClusterStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseClusterStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RenameWarehouseClusterStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AddWarehouseClusterNodeStmt {}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWarehouseClusterNodeStmt {}
