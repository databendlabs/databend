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

use common_catalog::catalog::Catalog;
use common_catalog::catalog::CatalogManager;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct DatabasesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for DatabasesTable {
    const NAME: &'static str = "system.databases";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalogs = CatalogManager::instance();
        let catalogs: Vec<(String, Arc<dyn Catalog>)> = catalogs
            .catalogs
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        let mut catalog_names = vec![];
        let mut database_names = vec![];
        for (ctl_name, catalog) in catalogs.into_iter() {
            let databases = catalog.list_databases(tenant.as_str()).await?;

            for db in databases {
                catalog_names.push(ctl_name.clone().into_bytes());
                let db_name = db.name().to_string().into_bytes();
                database_names.push(db_name);
            }
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(catalog_names),
            Series::from_data(database_names),
        ]))
    }
}

impl DatabasesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("catalog", Vu8::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'databases'".to_string(),
            name: "databases".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(DatabasesTable { table_info })
    }
}
