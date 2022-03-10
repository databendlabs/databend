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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::WarehouseInfo;

use crate::sessions::QueryContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct WarehousesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for WarehousesTable {
    const NAME: &'static str = "system.warehouses";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let warehouses = ctx
            .get_user_manager()
            .get_warehouses(tenant.as_str())
            .await?;
        WarehousesTable::warehouse_infos_to_datablock(warehouses)
    }
}

impl WarehousesTable {
    pub fn warehouse_infos_to_datablock(warehouses: Vec<WarehouseInfo>) -> Result<DataBlock> {
        let warehouse_ids: Vec<&str> = warehouses.iter().map(|x| x.warehouse_id.as_str()).collect();
        let tenants: Vec<&str> = warehouses.iter().map(|x| x.tenant.as_str()).collect();
        let warehouse_names: Vec<&str> = warehouses
            .iter()
            .map(|x| x.meta.warehouse_name.as_str())
            .collect();
        let warehouse_sizes: Vec<&str> = warehouses.iter().map(|x| x.meta.size.as_str()).collect();
        let warehouse_creation_time: Vec<u32> = warehouses
            .iter()
            .map(|x| (x.meta.created_on.timestamp_millis() / 1000) as u32)
            .collect();
        Ok(DataBlock::create(Self::schema(), vec![
            Series::from_data(warehouse_names),
            Series::from_data(tenants),
            Series::from_data(warehouse_ids),
            Series::from_data(warehouse_sizes),
            Series::from_data(warehouse_creation_time),
        ]))
    }
    pub fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("tenant_id", Vu8::to_data_type()),
            DataField::new("id", Vu8::to_data_type()),
            DataField::new("size", Vu8::to_data_type()),
            DataField::new("creation_time", DateTime32Type::arc(None)),
        ])
    }
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = Self::schema();

        let table_info = TableInfo {
            desc: "'system'.'warehouses'".to_string(),
            name: "warehouses".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemWarehouses".to_string(),
                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(WarehousesTable { table_info })
    }
}
