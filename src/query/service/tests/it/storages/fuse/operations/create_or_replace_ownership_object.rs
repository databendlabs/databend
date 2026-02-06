//  Copyright 2021 Datafuse Labs.
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

use databend_common_config::MetaConfig;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_storages_fuse::TableContext;
use databend_meta_runtime::DatabendRuntime;
use databend_query::test_kits::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_db_table_create_replace_clean_ownership_key() -> anyhow::Result<()> {
    let meta_config = MetaConfig::default();
    let meta = {
        let config = meta_config.to_meta_grpc_client_conf();
        let provider = Arc::new(MetaStoreProvider::new(config));
        provider.create_meta_store::<DatabendRuntime>().await?
    };

    // Extracts endpoints to communicate with meta service
    let MetaStore::L(local) = &meta else {
        panic!("MetaStore should not be local");
    };

    let endpoints = local.endpoints.clone();

    // Modify config to use local meta store
    let mut config = ConfigBuilder::create().config();
    config.meta.endpoints = endpoints.clone();
    // 2. Setup test fixture by using local meta store
    let fixture = TestFixture::setup_with_custom(OSSSetup { config }).await?;

    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();

    // test create or replace table
    {
        fixture.create_default_database().await?;

        let db_name = fixture.default_db_name();
        let tbl_name = fixture.default_table_name();
        let catalog_name = fixture.default_catalog_name();

        fixture
            .execute_command(&format!(
                "create table {}.{}.{} (id int)",
                catalog_name, db_name, tbl_name
            ))
            .await?;

        let db = ctx
            .get_default_catalog()?
            .get_database(&tenant, &db_name)
            .await?;

        let old_tbl_id = db
            .get_table(&tbl_name)
            .await?
            .get_table_info()
            .ident
            .table_id;
        let table_ownership = OwnershipObject::Table {
            catalog_name: catalog_name.clone(),
            db_id: db.get_db_info().database_id.db_id,
            table_id: old_tbl_id,
        };
        let table_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), table_ownership);
        let v = meta.get_pb(&table_ownership_key).await?;
        assert!(v.is_some());

        fixture
            .execute_command(&format!(
                "create or replace table {}.{}.{} (id int)",
                catalog_name, db_name, tbl_name
            ))
            .await?;
        let v = meta.get_pb(&table_ownership_key).await?;
        assert!(v.is_none());

        let tbl_name = "ownership_table_test";
        fixture
            .execute_command(&format!(
                "create or replace table {}.{}.{} (id int)",
                catalog_name, db_name, tbl_name
            ))
            .await?;
        let tbl = db.get_table(tbl_name).await?;
        let first_create_id = tbl.get_table_info().ident.table_id;
        let table_ownership = OwnershipObject::Table {
            catalog_name: catalog_name.clone(),
            db_id: db.get_db_info().database_id.db_id,
            table_id: first_create_id,
        };
        let table_ownership_key =
            TenantOwnershipObjectIdent::new(tenant.clone(), table_ownership.clone());
        let v = meta.get_pb(&table_ownership_key).await?;
        assert!(v.is_some());

        fixture
            .execute_command(&format!(
                "create or replace table {}.{}.{} (id int)",
                catalog_name, db_name, tbl_name
            ))
            .await?;
        let tbl = db.get_table(tbl_name).await?;
        let second_create_id = tbl.get_table_info().ident.table_id;
        assert_ne!(second_create_id, first_create_id);
        let db_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), table_ownership);
        let v = meta.get_pb(&db_ownership_key).await?;
        assert!(v.is_none());

        let table_ownership = OwnershipObject::Table {
            catalog_name: catalog_name.clone(),
            db_id: db.get_db_info().database_id.db_id,
            table_id: second_create_id,
        };
        let db_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), table_ownership);
        let v = meta.get_pb(&db_ownership_key).await?;
        assert!(v.is_some());
    }

    // test create or replace database
    {
        let db_name = "db1";
        fixture
            .execute_command(&format!("create or replace database {}", db_name))
            .await?;
        let db = ctx
            .get_default_catalog()?
            .get_database(&tenant, db_name)
            .await?;
        let first_create_db_id = db.get_db_info().database_id;
        let catalog_name = fixture.default_catalog_name();
        let db_ownership = OwnershipObject::Database {
            catalog_name: catalog_name.clone(),
            db_id: *first_create_db_id,
        };
        let db_ownership_key =
            TenantOwnershipObjectIdent::new(tenant.clone(), db_ownership.clone());
        let v = meta.get_pb(&db_ownership_key).await?;
        assert!(v.is_some());

        fixture
            .execute_command(&format!("create or replace database {}", db_name))
            .await?;
        let db = ctx
            .get_default_catalog()?
            .get_database(&tenant, db_name)
            .await?;
        let second_create_db_id = db.get_db_info().database_id;
        assert_ne!(second_create_db_id, first_create_db_id);

        let db_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), db_ownership);
        let v = meta.get_pb(&db_ownership_key).await?;
        assert!(v.is_none());

        let db_ownership = OwnershipObject::Database {
            catalog_name: catalog_name.clone(),
            db_id: *second_create_db_id,
        };
        let db_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), db_ownership);
        let v = meta.get_pb(&db_ownership_key).await?;
        assert!(v.is_some());
    }

    Ok(())
}
