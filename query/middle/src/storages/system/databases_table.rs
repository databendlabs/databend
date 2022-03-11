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

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct DatabasesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for DatabasesTable {
    const NAME: &'static str = "system.databases";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog();
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let db_names: Vec<&[u8]> = databases
            .iter()
            .map(|database| database.name().as_bytes())
            .collect();

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(db_names),
        ]))
    }
}

impl DatabasesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![DataField::new("name", Vu8::to_data_type())]);

        let table_info = TableInfo {
            desc: "'system'.'databases'".to_string(),
            name: "databases".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(DatabasesTable { table_info })
    }
}
