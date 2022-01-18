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
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReq;
use common_meta_types::TableMeta;
use databend_query::catalogs::Catalog;

use crate::tests::create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_get_database() -> Result<()> {
    let catalog = create_catalog().unwrap();
    // get system database
    let database = catalog.get_database("test", "system").await.unwrap();
    assert_eq!(database.name(), "system");
    let db_list = catalog.list_databases("").await.unwrap();
    assert_eq!(db_list.len(), 2);
    // get default database
    let db_2 = catalog.get_database("", "default").await.unwrap();
    assert_eq!(db_2.name(), "default");
    // get non-exist database
    let db_3 = catalog.get_database("test", "test").await;
    assert!(db_3.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_create_and_drop_databse() -> Result<()> {
    let catalog = create_catalog().unwrap();
    let db_list = catalog.list_databases("test").await.unwrap();
    let db_count = db_list.len();
    let req = CreateDatabaseReq {
        if_not_exists: false,
        tenant: "test".to_string(),
        db: "db1".to_string(),
        meta: DatabaseMeta {
            engine: "".to_string(),
            ..Default::default()
        },
    };
    let rep = catalog.create_database(req).await;
    assert!(rep.is_ok());
    let db_list_1 = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list_1.len(), db_count + 1);
    let drop_req = DropDatabaseReq {
        if_exists: false,
        tenant: "test".to_string(),
        db: "db1".to_string(),
    };
    let _ = catalog.drop_database(drop_req).await;
    let db_list_drop = catalog.list_databases("test").await.unwrap();
    assert_eq!(db_list_drop.len(), db_count);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_catalogs_table() -> Result<()> {
    let catalog = create_catalog().unwrap();
    catalog.list_databases("test").await.unwrap();
    let table_list = catalog.list_tables("test", "system").await.unwrap();
    assert_eq!(table_list.len(), 15);
    let table_list_1 = catalog.list_tables("test", "default").await;
    assert!(table_list_1.is_err());
    let table_list_2 = catalog.list_tables("", "default").await.unwrap();
    assert_eq!(table_list_2.len(), 0);
    // create table
    // Table schema with metadata(due to serde issue).
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));

    let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

    let created_on = Utc::now();

    // create table
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
    assert!(create_table_rep.is_ok());
    // list tables
    let table_list_3 = catalog.list_tables("", "default").await.unwrap();
    assert_eq!(table_list_3.len(), 1);
    let table = catalog
        .get_table("", "default", "test_table")
        .await
        .unwrap();
    assert_eq!(table.name(), "test_table");
    let table = catalog.get_table_by_info(table.get_table_info()).unwrap();
    assert_eq!(table.name(), "test_table");

    // drop table
    let dop_table_req = DropTableReq {
        if_exists: false,
        tenant: "".to_string(),
        db: "default".to_string(),
        table: "test_table".to_string(),
    };
    let drop_table_rep = catalog.drop_table(dop_table_req).await;
    assert!(drop_table_rep.is_ok());
    let table_list_4 = catalog.list_tables("", "default").await.unwrap();
    assert_eq!(table_list_4.len(), 0);
    Ok(())
}
