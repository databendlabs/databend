// Copyright 2021 Datafuse Labs.
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

use crate::procedures::admins::reload_config::ReloadConfigProcedure;
use crate::procedures::admins::CreateWarehouseMetaProcedure;
use crate::procedures::admins::DropWarehouseMetaProcedure;
use crate::procedures::admins::GetWarehouseMetaFunction;
use crate::procedures::admins::ListWarehouseMetaProcedure;
use crate::procedures::admins::UpdateWarehouseSizeProcedure;
use crate::procedures::ProcedureFactory;

pub struct AdminProcedure;

impl AdminProcedure {
    pub fn register(factory: &mut ProcedureFactory) {
        factory.register(
            "admin$create_warehouse_meta",
            CreateWarehouseMetaProcedure::desc(),
        );
        factory.register(
            "admin$update_warehouse_meta_size",
            UpdateWarehouseSizeProcedure::desc(),
        );
        factory.register("admin$get_warehouse_meta", GetWarehouseMetaFunction::desc());
        factory.register(
            "admin$list_warehouse_meta",
            ListWarehouseMetaProcedure::desc(),
        );
        factory.register(
            "admin$drop_warehouse_meta",
            DropWarehouseMetaProcedure::desc(),
        );
        factory.register("admin$reload_config", ReloadConfigProcedure::desc());
    }
}
