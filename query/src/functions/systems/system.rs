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

use super::fuse_history::FuseHistoryFunction;
use crate::functions::systems::warehouse_metadata::CreateWarehouseMetaFunction;
use crate::functions::systems::warehouse_metadata::DropWarehouseMetaFunction;
use crate::functions::systems::warehouse_metadata::GetWarehouseMetaFunction;
use crate::functions::systems::warehouse_metadata::ListWarehouseMetaFunction;
use crate::functions::systems::warehouse_metadata::UpdateWarehouseInstanceFunction;
use crate::functions::systems::FunctionFactory;

pub struct SystemFunction;

impl SystemFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("system$fuse_history", FuseHistoryFunction::desc());
        factory.register(
            "system$create_warehouse_meta",
            CreateWarehouseMetaFunction::desc(),
        );
        factory.register(
            "system$update_warehouse_meta_instance",
            UpdateWarehouseInstanceFunction::desc(),
        );
        factory.register(
            "system$get_warehouse_meta",
            GetWarehouseMetaFunction::desc(),
        );
        factory.register(
            "system$list_warehouse_meta",
            ListWarehouseMetaFunction::desc(),
        );
        factory.register(
            "system$drop_warehouse_meta",
            DropWarehouseMetaFunction::desc(),
        )
    }
}
