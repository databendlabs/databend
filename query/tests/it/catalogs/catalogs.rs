// Copyright 2022 Datafuse Labs.
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
use chrono::Utc;
use common_base::tokio;
use common_datavalues::{DataField, DataSchema};
use common_exception::Result;
use common_meta_types::{CreateDatabaseReq, CreateTableReq, DatabaseMeta, DropDatabaseReq, TableMeta};
use common_datavalues::DataType;

use databend_query::catalogs::Catalog;

use crate::tests::create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_get_database() -> Result<()>{
    let catalog = create_catalog().unwrap();
    // get system database
    let database= catalog.get_database("test", "system").await.unwrap();
    assert_eq!(database.name(), "system");
    let db_list = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list.len(), 2);
    // get default database
    let db_2 = catalog.get_database("", "default").await.unwrap();
    assert_eq!(db_2.name(), "default");
    // get non-exist database
    let db_3 = catalog.get_database("test", "test").await;
    assert_eq!(db_3.is_err(), true);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_create_and_drop_databse() -> Result<()> {
    let catalog = create_catalog().unwrap();
    let db_list = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list.len(), 2);
    let req = CreateDatabaseReq {
        if_not_exists: false,
        tenant: "test".to_string(),
        db: "db1".to_string(),
        meta: DatabaseMeta {
            engine: "".to_string(),
            ..Default::default()
        },
    };
    let rep = catalog.create_database(req).await.unwrap();
    assert_eq!(rep.database_id, 2);
    let db_list_1 = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list_1.len(), 3);
    let drop_req = DropDatabaseReq {
        if_exists: false,
        tenant: "test".to_string(),
        db: "db1".to_string(),
    };
    let _= catalog.drop_database(drop_req).await;
    let db_list_drop = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list_drop.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_table() -> Result<()> {
    let catalog = create_catalog().unwrap();
    let db_list = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list.len(), 2);
    let db_list_1 = catalog.list_tables("test", "system").await.unwrap();
    assert_eq!(db_list_1.len(), 15);
    let table_list_2 = catalog.list_tables("test", "default").await;
    assert_eq!(table_list_2.is_err(), true);
    let db_list_3 = catalog.list_tables("", "default").await.unwrap();
    assert_eq!(db_list_3.len(), 0);
    // create table
    // Table schema with metadata(due to serde issue).
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));

    let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

    let created_on = Utc::now();

    let req = CreateTableReq {
        if_not_exists: false,
        tenant: "".to_string(),
        db: "default".to_string(),
        table: "test_table".to_string(),
        table_meta: TableMeta {
            schema: schema.clone(),
            engine: "MEMORY".to_string(),
            options: options.clone(),
            created_on,
            ..TableMeta::default()
        },
    };
    let create_table_rep = catalog.create_table(req.clone()).await;
    assert_eq!(create_table_rep.is_ok(), true);
    let db_list_3 = catalog.list_tables("", "default").await.unwrap();
    assert_eq!(db_list_3.len(), 1);
    Ok(())
}


