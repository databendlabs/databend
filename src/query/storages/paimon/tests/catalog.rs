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

mod common;

use std::sync::Arc;

use common::TestWarehouse;
use common::filesystem_catalog;
use common::paimon_catalog;
use common::setup_append_table;
use databend_common_catalog::catalog::Catalog as DatabendCatalog;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::tenant::Tenant;
use paimon::Catalog;

#[tokio::test]
async fn test_filesystem_catalog_list_and_read_only() {
    let wh = TestWarehouse::new();
    let _ = setup_append_table(&wh.warehouse).await;
    let catalog = paimon_catalog(&wh.warehouse).await;
    let tenant = Tenant::new_literal("test");

    let db_names: Vec<_> = catalog
        .list_databases(&tenant)
        .await
        .expect("list databases")
        .iter()
        .map(|db| db.name().to_string())
        .collect();
    assert_eq!(db_names, vec!["db".to_string()]);

    let db = catalog.get_database(&tenant, "db").await.expect("get db");
    assert_eq!(db.list_tables_names().await.expect("list tables"), vec![
        "append_t".to_string()
    ]);
    let table = db.get_table("append_t").await.expect("get table");
    assert_eq!(table.engine(), "PAIMON");
    assert_eq!(table.schema().fields().len(), 2);

    let err = db
        .rename_table(RenameTableReq {
            if_exists: false,
            name_ident: TableNameIdent::new(tenant.clone(), "db", "append_t"),
            new_db_name: "db".to_string(),
            new_table_name: "x".to_string(),
        })
        .await
        .expect_err("read-only rename");
    assert!(err.message().contains("read-only"));

    let catalog_arc: Arc<dyn DatabendCatalog> = Arc::new(catalog);
    let err = catalog_arc
        .create_database(databend_common_meta_app::schema::CreateDatabaseReq {
            override_existing: false,
            catalog_name: None,
            name_ident:
                databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent::new(
                    tenant.clone(),
                    "new_db",
                ),
            meta: databend_common_meta_app::schema::DatabaseMeta::default(),
        })
        .await
        .expect_err("read-only create database");
    assert!(err.message().contains("read-only"));
}

#[tokio::test]
async fn test_adapter_matches_filesystem_catalog() {
    let wh = TestWarehouse::new();
    let _ = setup_append_table(&wh.warehouse).await;
    let inner = filesystem_catalog(&wh.warehouse);
    let adapter = paimon_catalog(&wh.warehouse).await;
    let tenant = Tenant::new_literal("test");

    assert_eq!(
        inner.list_databases().await.expect("inner dbs"),
        adapter
            .list_databases(&tenant)
            .await
            .expect("adapter dbs")
            .iter()
            .map(|db| db.name().to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        inner.list_tables("db").await.expect("inner tables"),
        adapter
            .get_database(&tenant, "db")
            .await
            .expect("adapter db")
            .list_tables_names()
            .await
            .expect("adapter tables")
    );
}
