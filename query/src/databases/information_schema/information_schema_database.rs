//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;

use crate::catalogs::InMemoryMetas;
use crate::databases::Database;
use crate::storages::information_schema::ColumnsTable;
use crate::storages::information_schema::KeywordsTable;
use crate::storages::information_schema::SchemataTable;
use crate::storages::information_schema::TablesTable;
use crate::storages::information_schema::ViewsTable;
use crate::storages::Table;

#[derive(Clone)]
pub struct InformationSchemaDatabase {
    db_info: DatabaseInfo,
}

impl InformationSchemaDatabase {
    pub fn create(sys_db_meta: &mut InMemoryMetas) -> Self {
        let table_list: Vec<Arc<dyn Table>> = vec![
            ColumnsTable::create(sys_db_meta.next_table_id()),
            TablesTable::create(sys_db_meta.next_table_id()),
            KeywordsTable::create(sys_db_meta.next_table_id()),
            ViewsTable::create(sys_db_meta.next_table_id()),
            SchemataTable::create(sys_db_meta.next_table_id()),
        ];

        let db = "INFORMATION_SCHEMA";

        for tbl in table_list.into_iter() {
            sys_db_meta.insert(db, tbl);
        }

        let db_info = DatabaseInfo {
            database_id: sys_db_meta.next_db_id(),
            db: db.to_string(),
            meta: DatabaseMeta {
                engine: "SYSTEM".to_string(),
                ..Default::default()
            },
        };

        Self { db_info }
    }
}

#[async_trait::async_trait]
impl Database for InformationSchemaDatabase {
    fn name(&self) -> &str {
        "INFORMATION_SCHEMA"
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }
}
