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

use crate::functions::admins::CreateWarehouseMetaFunction;
use crate::functions::admins::DropWarehouseMetaFunction;
use crate::functions::admins::GetWarehouseMetaFunction;
use crate::functions::admins::ListWarehouseMetaFunction;
use crate::functions::admins::UpdateWarehouseSizeFunction;
use crate::functions::FunctionFactory;

pub struct AdminFunction;

impl AdminFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register(
            "admin$create_warehouse_meta",
            CreateWarehouseMetaFunction::desc(),
        );
        factory.register(
            "admin$update_warehouse_meta_size",
            UpdateWarehouseSizeFunction::desc(),
        );
        factory.register("admin$get_warehouse_meta", GetWarehouseMetaFunction::desc());
        factory.register(
            "admin$list_warehouse_meta",
            ListWarehouseMetaFunction::desc(),
        );
        factory.register(
            "admin$drop_warehouse_meta",
            DropWarehouseMetaFunction::desc(),
        )
    }
}
