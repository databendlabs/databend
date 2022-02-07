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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::DropDatabaseReq;
use databend_query::catalogs::Catalog;
use databend_query::catalogs::ImmutableCatalog;

use crate::tests::create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_immutable_catalogs_database() -> Result<()> {
    let tenant = "test";
    let conf = crate::tests::ConfigBuilder::create().config();
    let catalog = ImmutableCatalog::try_create_with_config(&conf).await?;

    // get system database
    let database = catalog.get_database(tenant, "system").await?;
    assert_eq!(database.name(), "system");

    // get default database
    let db_2 = catalog.get_database(tenant, "default").await;
    assert!(db_2.is_err());

    // get non-exist database
    let db_3 = catalog.get_database(tenant, "test").await;
    assert!(db_3.is_err());

    // create database should failed
    let create_db_req = CreateDatabaseReq {
        if_not_exists: false,
        tenant: tenant.to_string(),
        db: "system".to_string(),
        meta: Default::default(),
    };
    let create_db_req = catalog.create_database(create_db_req).await;
    assert!(create_db_req.is_err());

    let drop_db_req = DropDatabaseReq {
        if_exists: false,
        tenant: tenant.to_string(),
        db: "system".to_string(),
    };
    let drop_db_req = catalog.drop_database(drop_db_req).await;
    assert!(drop_db_req.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_immutable_catalogs_table() -> Result<()> {
    let tenant = "test";
    let catalog = create_catalog()?;

    let db_list_1 = catalog.list_tables(tenant, "system").await?;
    assert!(!db_list_1.is_empty());

    let table_list = catalog.list_tables(tenant, "default").await?;
    assert!(table_list.is_empty());

    Ok(())
}
