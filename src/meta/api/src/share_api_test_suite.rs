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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::share::share_end_point_ident::ShareEndpointIdentRaw;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::share::share_name_ident::ShareNameIdentRaw;
use databend_common_meta_app::share::*;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use enumflags2::BitFlags;
use log::info;

use crate::get_object_shared_by_share_ids;
use crate::get_share_account_meta_or_err;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::get_share_or_err;
use crate::kv_app_error::KVAppError;
use crate::testing::get_kv_data;
use crate::SchemaApi;
use crate::ShareApi;

/// Test suite of `ShareApi`.
///
/// It is not used by this crate, but is used by other crate that impl `ShareApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct ShareApiTestSuite {}

async fn if_share_object_data_exists(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    entry: &ShareGrantEntry,
) -> Result<bool, KVAppError> {
    if let Ok((_seq, _share_ids)) = get_object_shared_by_share_ids(kv_api, &entry.object).await {
        return Ok(false);
    }
    Ok(true)
}

// Return true if all the share data has been removed.
async fn is_all_share_data_removed(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &ShareNameIdent,
    share_id: u64,
    share_meta: &ShareMeta,
) -> Result<bool, KVAppError> {
    let res = get_share_or_err(kv_api, share_name, "").await;
    if res.is_ok() {
        return Ok(false);
    }

    let res = get_share_id_to_name_or_err(kv_api, share_id, "").await;
    if res.is_ok() {
        return Ok(false);
    }

    for account in share_meta.get_accounts() {
        let share_account_key = ShareConsumerIdent::new(
            Tenant::new_or_err(account, "is_all_share_data_removed")?,
            share_id,
        );
        let res = get_share_account_meta_or_err(kv_api, &share_account_key, "").await;
        if res.is_ok() {
            return Ok(false);
        }
    }

    if let Some(database) = &share_meta.database {
        if if_share_object_data_exists(kv_api, database).await? {
            return Ok(false);
        }
    }

    for (_key, entry) in share_meta.entries.iter() {
        if if_share_object_data_exists(kv_api, entry).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

impl ShareApiTestSuite {
    /// Test ShareApi on a single node
    pub async fn test_single_node_share<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    {
        let suite = ShareApiTestSuite {};

        suite.test_share_table(&b.build().await).await?;
        suite.test_share_database(&b.build().await).await?;
        suite.share_create_show_drop(&b.build().await).await?;
        suite
            .share_endpoint_create_show_drop(&b.build().await)
            .await?;
        suite.share_add_remove_account(&b.build().await).await?;
        suite.share_grant_revoke_object(&b.build().await).await?;
        suite.get_share_grant_objects(&b.build().await).await?;
        suite
            .get_grant_privileges_of_object(&b.build().await)
            .await?;
        suite
            .drop_share_database_and_table(&b.build().await)
            .await?;

        Ok(())
    }

    #[minitrace::trace]
    async fn test_share_table<MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "share1";
        let share2 = "share2";
        let db_name = "db1";
        let table_name = "table";

        let share_name1 = ShareNameIdent::new(&tenant, share1);
        let share_name2 = ShareNameIdent::new(&tenant, share2);
        let share_id1: u64;
        let share_id2: u64;
        let db_id: u64;
        let mut table_id: u64;

        let create_on = Utc::now();
        info!("test replace table");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name1.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id1 = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id1, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(ShareNameIdentRaw::from(share_name1.clone()), share_name_ret);

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name2.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await?;
            share_id2 = res.share_id;

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
            assert!(res.share_specs.is_none());
            db_id = res.db_id;

            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, *share_name1.name());
            assert_eq!(share_spec.database.unwrap().name, db_name.to_string());
            assert_eq!(
                share_spec.db_privileges,
                Some(BitFlags::from(ShareGrantObjectPrivilege::Usage))
            );
            assert!(res.grant_share_table.is_none());

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: table_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
            assert!(res.spec_vec.is_none());
            table_id = res.table_id;

            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let (share_db_id, grant_share_table) = res.grant_share_table.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(grant_share_table.name, table_name.to_string());

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let (share_db_id, grant_share_table) = res.grant_share_table.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(grant_share_table.name, table_name.to_string());

            // check TableMeta.shared_by contain share id1 and id2
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert_eq!(table_meta.shared_by.len(), 2);
            assert!(table_meta.shared_by.contains(&share_id1));
            assert!(table_meta.shared_by.contains(&share_id2));

            // when replace table MUST return table spec
            let req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: table_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
            let (share_db_id, share_table_id, share_specs) = res.spec_vec.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(share_table_id, table_id);
            assert_eq!(share_specs.len(), 2);
            let share_names: Vec<String> = share_specs
                .iter()
                .map(|share_spec| share_spec.name.clone())
                .collect();
            assert!(share_names.contains(&share1.to_string()));
            assert!(share_names.contains(&share2.to_string()));
            assert_eq!(share_specs[0].database.as_ref().unwrap().id, db_id);
            assert_eq!(share_specs[0].tables.len(), 0);
            assert_eq!(share_specs[1].database.as_ref().unwrap().id, db_id);
            assert_eq!(share_specs[1].tables.len(), 0);
            table_id = res.table_id;
        }

        info!("test drop table");
        {
            // grant the table again
            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let (share_db_id, grant_share_table) = res.grant_share_table.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(grant_share_table.name, table_name.to_string());

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let (share_db_id, grant_share_table) = res.grant_share_table.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(grant_share_table.name, table_name.to_string());

            // drop table MUST return share table info
            let plan = DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                table_name: table_name.to_string(),
                tb_id: table_id,
                db_id,
            };
            let res = mt.drop_table_by_id(plan).await?;
            let (share_db_id, share_specs) = res.spec_vec.unwrap();
            assert_eq!(share_db_id, db_id);
            assert_eq!(share_specs.len(), 2);
            let share_names: Vec<String> = share_specs
                .iter()
                .map(|share_spec| share_spec.name.clone())
                .collect();
            assert!(share_names.contains(&share1.to_string()));
            assert!(share_names.contains(&share2.to_string()));
            assert_eq!(share_specs[0].database.as_ref().unwrap().id, db_id);
            assert_eq!(share_specs[0].tables.len(), 0);
            assert_eq!(share_specs[1].database.as_ref().unwrap().id, db_id);
            assert_eq!(share_specs[1].tables.len(), 0);
        }

        info!("test update table meta");
        {
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: table_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            table_id = res.table_id;

            // grant the table again
            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert_eq!(table_meta.shared_by.len(), 2);
            assert!(table_meta.shared_by.contains(&share_id1));
            assert!(table_meta.shared_by.contains(&share_id2));
            let old_shared_by = table_meta.shared_by.clone();

            let mut new_table_meta = TableMeta::default();
            new_table_meta
                .options
                .insert("key".to_string(), "value".to_string());

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta,
            };
            let table = mt
                .get_table((tenant_name, db_name, table_name).into())
                .await
                .unwrap();
            let res = mt
                .update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    ..Default::default()
                })
                .await?;
            let share_vec_table_infos = res.unwrap().share_vec_table_infos.unwrap();
            let (share_names, share_db_id, table_info) = &share_vec_table_infos[0];
            assert_eq!(*share_db_id, db_id);
            assert_eq!(share_names.len(), 2);
            assert!(share_names.contains(&share_name1.share_name().to_string()));
            assert!(share_names.contains(&share_name2.share_name().to_string()));
            assert_eq!(
                table_info.options().get("key").unwrap(),
                &"value".to_string()
            );

            // make sure `update_table_meta` does not change `shared_by`
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert_eq!(table_meta.shared_by, old_shared_by);
        }

        info!("test rename table");
        // rename share table MUST remove share table from share
        {
            // first check TableMeta.shared_by is not empty
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert!(table_meta.shared_by.contains(&share_id1));
            assert!(table_meta.shared_by.contains(&share_id2));
            assert_eq!(table_meta.shared_by.len(), 2);

            let new_tbl_name = "new_tbl_name".to_string();
            let req = RenameTableReq {
                if_exists: true,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: table_name.to_string(),
                },
                new_db_name: db_name.to_string(),
                new_table_name: new_tbl_name.to_string(),
            };

            let res = mt.rename_table(req).await?;
            let (share_specs, share_object) = res.share_table_info.unwrap();
            assert_eq!(share_specs.len(), 2);
            let share_names: Vec<String> = share_specs
                .iter()
                .map(|share_spec| share_spec.name.clone())
                .collect();
            assert!(share_names.contains(&share1.to_string()));
            assert!(share_names.contains(&share2.to_string()));
            assert_eq!(share_specs[0].tables.len(), 0);
            assert_eq!(share_specs[1].tables.len(), 0);
            if let ShareObject::Table((share_db_id, share_table_id, share_table_name)) =
                share_object
            {
                assert_eq!(table_id, share_table_id);
                assert_eq!(db_id, share_db_id);
                assert_eq!(table_name.to_string(), share_table_name);
            } else {
                unreachable!()
            }

            // check TableMeta.shared_by is empty
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert!(table_meta.shared_by.is_empty());
        }
        Ok(())
    }

    #[minitrace::trace]
    async fn test_share_database<MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "share1";
        let share2 = "share2";
        let db_name = "db1";
        let table_name = "table";

        let share_name1 = ShareNameIdent::new(&tenant, share1);
        let share_name2 = ShareNameIdent::new(&tenant, share2);
        let share_id1: u64;
        let share_id2: u64;
        let table_id: u64;
        let mut db_id: u64;

        let create_on = Utc::now();
        info!("test replace shared database");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name1.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await?;
            info!("create share res: {:?}", res);
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id1 = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id1, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(ShareNameIdentRaw::from(share_name1.clone()), share_name_ret);

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name2.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await?;
            share_id2 = res.share_id;

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
            assert!(res.share_specs.is_none());

            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, *share_name1.name());
            assert_eq!(share_spec.database.unwrap().name, db_name.to_string());
            assert_eq!(
                share_spec.db_privileges,
                Some(BitFlags::from(ShareGrantObjectPrivilege::Usage))
            );
            assert!(res.grant_share_table.is_none());

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            // when replace shared database MUST return share db spec
            let plan = CreateDatabaseReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            db_id = res.db_id;
            info!("create database res: {:?}", res);

            let share_specs = res.share_specs.unwrap();
            assert_eq!(share_specs.len(), 2);
            let share_names: Vec<String> = share_specs
                .iter()
                .map(|share_spec| share_spec.name.clone())
                .collect();
            assert!(share_names.contains(&share1.to_string()));
            assert!(share_names.contains(&share2.to_string()));
            assert!(share_specs[0].database.is_none());
            assert!(share_specs[1].database.is_none());
        }

        info!("test drop shared database");
        {
            // grant the database again
            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.database.as_ref().unwrap().id, db_id);

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.database.as_ref().unwrap().id, db_id);

            // drop database show return share database spec
            let plan = DropDatabaseReq {
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                if_exists: true,
            };

            let res = mt.drop_database(plan).await?;
            info!("drop database res: {:?}", res);
            let share_specs = res.share_specs.unwrap();
            assert_eq!(share_specs.len(), 2);
            let share_names: Vec<String> = share_specs
                .iter()
                .map(|share_spec| share_spec.name.clone())
                .collect();
            assert!(share_names.contains(&share1.to_string()));
            assert!(share_names.contains(&share2.to_string()));
            assert!(share_specs[0].database.is_none());
            assert!(share_specs[1].database.is_none());
        }

        info!("test revoke shared database");
        {
            // first check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name1.clone(),
            };

            let res = mt.get_share_grant_objects(req).await?;
            assert!(res.objects.is_empty());

            let req = GetShareGrantObjectReq {
                share_name: share_name2.clone(),
            };

            let res = mt.get_share_grant_objects(req).await?;
            assert!(res.objects.is_empty());

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            db_id = res.db_id;
            info!("create database res: {:?}", res);
            assert!(res.share_specs.is_none());

            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, *share_name1.name());
            assert_eq!(share_spec.database.unwrap().name, db_name.to_string());
            assert_eq!(
                share_spec.db_privileges,
                Some(BitFlags::from(ShareGrantObjectPrivilege::Usage))
            );
            assert!(res.grant_share_table.is_none());

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            // check database meta
            let dbid_key = DatabaseId { db_id };
            let database_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &dbid_key).await?;
            assert!(database_meta.shared_by.contains(&share_id1));
            assert!(database_meta.shared_by.contains(&share_id2));

            // after grant database check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name1.clone(),
            };

            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 1);

            let req = GetShareGrantObjectReq {
                share_name: share_name2.clone(),
            };
            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 1);

            // create a table and grant share to it
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: table_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
            assert!(res.spec_vec.is_none());
            table_id = res.table_id;

            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), table_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            // after grant table check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name1.clone(),
            };

            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 2);

            let req = GetShareGrantObjectReq {
                share_name: share_name2.clone(),
            };
            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 2);

            // check database meta
            let dbid_key = DatabaseId { db_id };
            let database_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &dbid_key).await?;
            assert_eq!(database_meta.shared_by.len(), 2);
            assert!(database_meta.shared_by.contains(&share_id1));
            assert!(database_meta.shared_by.contains(&share_id2));

            // check table meta
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert_eq!(table_meta.shared_by.len(), 2);
            assert!(table_meta.shared_by.contains(&share_id1));
            assert!(table_meta.shared_by.contains(&share_id2));

            // when revoke database priviledge, table priviledge MUST be revoked too.
            let req = RevokeShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                update_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.revoke_share_object(req).await?;
            let share_spec = res.clone().share_spec.unwrap();
            assert_eq!(res.share_id, share_id1);
            assert!(share_spec.database.is_none());
            assert!(share_spec.tables.is_empty());

            // after grant table check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name1.clone(),
            };

            // check share_grant_objects
            let res = mt.get_share_grant_objects(req).await?;
            assert!(res.objects.is_empty());

            // check database meta
            let dbid_key = DatabaseId { db_id };
            let database_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &dbid_key).await?;
            assert_eq!(database_meta.shared_by.len(), 1);
            assert!(database_meta.shared_by.contains(&share_id2));

            // check table meta
            let tbid = TableId { table_id };
            let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &tbid).await?;
            assert_eq!(table_meta.shared_by.len(), 1);
            assert!(table_meta.shared_by.contains(&share_id2));
        }

        info!("test rename shared database");
        {
            // check database meta
            let dbid_key = DatabaseId { db_id };
            let database_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &dbid_key).await?;
            assert!(database_meta.shared_by.contains(&share_id2));

            // check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name2.clone(),
            };
            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 2);

            let db2_name = "db2_name";
            // rename database
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: db2_name.to_string(),
            };
            let res = mt.rename_database(req).await?;
            info!("rename database res: {:?}", res);
            let (share_specs, object) = res.share_spec.unwrap();
            if let ShareObject::Db(old_db_id) = object {
                assert_eq!(old_db_id, db_id);
            } else {
                unreachable!()
            }
            assert_eq!(share_specs.len(), 1);
            let share_spec = &share_specs[0];
            assert_eq!(share_spec.name, share2.to_string());
            assert!(share_spec.database.is_none());
            assert!(share_spec.tables.is_empty());

            // after rename database check database meta
            let dbid_key = DatabaseId { db_id };
            let database_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &dbid_key).await?;
            assert!(database_meta.shared_by.is_empty());

            // after rename database check share objects
            let req = GetShareGrantObjectReq {
                share_name: share_name2.clone(),
            };
            let res = mt.get_share_grant_objects(req).await?;
            assert_eq!(res.objects.len(), 0);
        }
        Ok(())
    }

    #[minitrace::trace]
    async fn share_create_show_drop<MT: ShareApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let tenant = Tenant::new_literal(tenant);

        let share1 = "share1";
        let share_name = ShareNameIdent::new(&tenant, share1);
        let share_id: u64;

        info!("--- show share when there are no share");
        {
            let req = ShowSharesReq {
                tenant: tenant.clone(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert!(resp.outbound_accounts.is_empty());
        }

        info!("--- create share1");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            assert!(res.share_spec.is_some());
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(ShareNameIdentRaw::from(share_name.clone()), share_name_ret)
        }

        info!("--- show share again");
        {
            let req = ShowSharesReq {
                tenant: tenant.clone(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert_eq!(resp.outbound_accounts.len(), 1);
        }

        Ok(())
    }

    #[minitrace::trace]
    async fn share_endpoint_create_show_drop<MT: ShareApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name1 = "tenant1";
        let endpoint1 = "endpoint1";
        let endpoint2 = "endpoint2";

        let tenant1 = Tenant::new_literal(tenant_name1);

        info!("--- create share endpoints");
        let create_on = Utc::now();
        {
            let req = CreateShareEndpointReq {
                create_option: CreateOption::Create,
                endpoint: ShareEndpointIdent::new(&tenant1, endpoint1),
                url: "http://127.0.0.1:22222".to_string(),
                credential: None,
                comment: None,
                create_on,
                args: BTreeMap::new(),
            };

            let res = mt.create_share_endpoint(req).await;
            info!("create create_share_endpoint res: {:?}", res);
            assert!(res.is_ok());

            let req = CreateShareEndpointReq {
                create_option: CreateOption::Create,
                endpoint: ShareEndpointIdent::new(&tenant1, endpoint1),
                url: "http://127.0.0.1:21111".to_string(),
                credential: None,
                comment: None,
                args: BTreeMap::new(),
                create_on,
            };

            let res = mt.create_share_endpoint(req).await;
            info!("create create_share_endpoint res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::SHARE_ENDPOINT_ALREADY_EXISTS,
                ErrorCode::from(err).code()
            );

            let req = CreateShareEndpointReq {
                create_option: CreateOption::Create,
                endpoint: ShareEndpointIdent::new(&tenant1, endpoint2),
                url: "http://127.0.0.1:21111".to_string(),
                credential: None,
                comment: None,
                create_on,
                args: BTreeMap::new(),
            };

            let res = mt.create_share_endpoint(req).await;
            info!("create create_share_endpoint res: {:?}", res);
            assert!(res.is_ok());
        }

        info!("--- upsert share endpoints");
        {
            let upsert_tenant_name = "upsert_tenant";
            let upsert_tenant = Tenant::new_literal(upsert_tenant_name);

            let upsert_req = UpsertShareEndpointReq {
                endpoint: ShareEndpointIdent::new(&upsert_tenant, endpoint2),
                url: "http://127.0.0.1:21111".to_string(),
                credential: None,
                create_on,
                args: BTreeMap::new(),
            };
            let res = mt.upsert_share_endpoint(upsert_req.clone()).await;
            assert!(res.is_ok());
            let upsert_share_endpoint_id = res.unwrap().share_endpoint_id;

            let req = GetShareEndpointReq {
                tenant: upsert_tenant.clone(),
                endpoint: None,
            };
            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.clone().unwrap().share_endpoint_meta_vec.len(), 1);
            assert_eq!(
                res.unwrap().share_endpoint_meta_vec[0].1.url,
                "http://127.0.0.1:21111".to_string()
            );

            let res = mt.upsert_share_endpoint(upsert_req).await;
            assert!(res.is_ok());
            assert_eq!(upsert_share_endpoint_id, res.unwrap().share_endpoint_id);

            let upsert_req = UpsertShareEndpointReq {
                endpoint: ShareEndpointIdent::new(&upsert_tenant, endpoint2),
                url: "http://127.0.0.1:22222".to_string(),
                credential: None,
                create_on,
                args: BTreeMap::new(),
            };
            let res = mt.upsert_share_endpoint(upsert_req).await;
            assert!(res.is_ok());
            assert_eq!(upsert_share_endpoint_id, res.unwrap().share_endpoint_id);

            let req = GetShareEndpointReq {
                tenant: upsert_tenant.clone(),
                endpoint: None,
            };
            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.clone().unwrap().share_endpoint_meta_vec.len(), 1);
            assert_eq!(
                res.unwrap().share_endpoint_meta_vec[0].1.url,
                "http://127.0.0.1:22222".to_string()
            );
        }
        info!("--- get share endpoints");
        {
            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: None,
            };

            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap().share_endpoint_meta_vec.len(), 2);

            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: Some(endpoint1.to_string()),
            };

            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap().share_endpoint_meta_vec.len(), 1);
        }

        info!("--- drop share endpoints");
        {
            let req = DropShareEndpointReq {
                if_exists: true,
                endpoint: ShareEndpointIdent::new(&tenant1, endpoint1),
            };
            let res = mt.drop_share_endpoint(req).await;
            assert!(res.is_ok());

            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: None,
            };

            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap().share_endpoint_meta_vec.len(), 1);

            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: Some(endpoint1.to_string()),
            };

            let res = mt.get_share_endpoint(req).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap().share_endpoint_meta_vec.len(), 0);
        }

        {
            info!("--- create or replace share endpoints");
            let endpoint_name = "replace_endpoint";
            let endpoint = ShareEndpointIdent::new(&tenant1, endpoint_name);

            let url = "http://127.0.0.1:22222".to_string();
            let req = CreateShareEndpointReq {
                create_option: CreateOption::Create,
                endpoint: endpoint.clone(),
                url: url.clone(),
                credential: None,
                comment: None,
                create_on,
                args: BTreeMap::new(),
            };

            let res = mt.create_share_endpoint(req).await?;

            let old_share_endpoint_id = res.share_endpoint_id;
            let old_id_key = ShareEndpointId {
                share_endpoint_id: old_share_endpoint_id,
            };
            let oldid_to_name_key = ShareEndpointIdToName {
                share_endpoint_id: old_share_endpoint_id,
            };
            let meta: ShareEndpointMeta = get_kv_data(mt.as_kv_api(), &old_id_key).await?;
            assert_eq!(meta.url, url);
            let name_key: ShareEndpointIdentRaw =
                get_kv_data(mt.as_kv_api(), &oldid_to_name_key).await?;
            assert_eq!(name_key, endpoint.clone().into());

            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: Some(endpoint_name.to_string()),
            };

            let res = mt.get_share_endpoint(req).await?;
            assert_eq!(res.share_endpoint_meta_vec.len(), 1);
            assert_eq!(res.share_endpoint_meta_vec[0].1.url, url);

            let url = "http://192.168.0.1".to_string();
            let req = CreateShareEndpointReq {
                create_option: CreateOption::CreateOrReplace,
                endpoint: endpoint.clone(),
                url: url.clone(),
                credential: None,
                comment: None,
                create_on,
                args: BTreeMap::new(),
            };

            let res = mt.create_share_endpoint(req).await?;
            let share_endpoint_id = res.share_endpoint_id;

            let req = GetShareEndpointReq {
                tenant: tenant1.clone(),
                endpoint: Some(endpoint_name.to_string()),
            };

            let res = mt.get_share_endpoint(req).await?;
            assert_eq!(res.share_endpoint_meta_vec.len(), 1);
            assert_eq!(res.share_endpoint_meta_vec[0].1.url, url);

            // assert old id key has been deleted
            let meta: Result<ShareEndpointMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &old_id_key).await;
            assert!(meta.is_err());
            let name_key: Result<ShareEndpointIdentRaw, KVAppError> =
                get_kv_data(mt.as_kv_api(), &oldid_to_name_key).await;
            assert!(name_key.is_err());

            // assert new id key has been created
            let id_key = ShareEndpointId { share_endpoint_id };
            let id_to_name_key = ShareEndpointIdToName { share_endpoint_id };
            let meta: ShareEndpointMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
            assert_eq!(meta.url, url);
            let name_key: ShareEndpointIdentRaw =
                get_kv_data(mt.as_kv_api(), &id_to_name_key).await?;
            assert_eq!(name_key, endpoint.clone().into());
        }
        Ok(())
    }

    #[minitrace::trace]
    async fn share_add_remove_account<MT: ShareApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name1 = "tenant1";
        let tenant_name2 = "tenant2";

        let tenant1 = Tenant::new_literal(tenant_name1);
        let tenant2 = Tenant::new_literal(tenant_name2);

        let share1 = "share1";
        let share2 = "share2";
        let account = "account1";
        let account2 = "account2";
        let share_name = ShareNameIdent::new(&tenant1, share1);
        let share_name2 = ShareNameIdent::new(&tenant1, share2);
        let share_name3 = ShareNameIdent::new(&tenant2, share2);
        let comment1 = "comment1";
        let comment2 = "comment2";
        let comment3 = "comment3";
        let share_id: u64;
        let share_on = Utc::now();
        let create_on = Utc::now();
        let if_exists = true;

        info!("--- add and remove account with not exist share");
        {
            let req = AddShareAccountsReq {
                share_name: share_name.clone(),
                share_on,
                if_exists: false,
                accounts: vec![account.to_string()],
            };

            // get share meta and check account has been added
            let res = mt.add_share_tenants(req).await;
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_SHARE, ErrorCode::from(err).code());

            let req = RemoveShareAccountsReq {
                share_name: share_name.clone(),
                if_exists: false,
                accounts: vec![account2.to_string()],
            };

            let res = mt.remove_share_tenants(req).await;
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_SHARE, ErrorCode::from(err).code());
        }

        info!("--- prepare share1 share2 share3");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: Some(comment1.to_string()),
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name2.clone(),
                comment: Some(comment2.to_string()),
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("add share account res: {:?}", res);
            assert!(res.is_ok());
            let res = res.unwrap();
            assert!(res.share_spec.is_some());

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name3.clone(),
                comment: Some(comment3.to_string()),
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("add share account res: {:?}", res);
            assert!(res.is_ok());
            let res = res.unwrap();
            assert!(res.share_spec.is_some());
        }

        info!("--- add account account1");
        {
            let req = AddShareAccountsReq {
                share_name: share_name.clone(),
                share_on,
                if_exists,
                accounts: vec![account.to_string()],
            };

            // get share meta and check account has been added
            let res = mt.add_share_tenants(req).await?;
            info!("add share account res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(&share_spec.name, share_name.share_name());
            assert_eq!(share_spec.tenants.len(), 1);
            assert_eq!(share_spec.tenants[0], account.to_string());

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account.to_string()));

            // get and check share account meta
            let share_account_name = ShareConsumerIdent::new(
                Tenant::new_or_err(account, "share_add_remove_account")?,
                share_id,
            );
            let (_share_account_meta_seq, share_account_meta) =
                get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await?;
            assert_eq!(share_account_meta.share_id, share_id);
            assert_eq!(share_account_meta.account, account.to_string());
            assert_eq!(share_account_meta.share_on, share_on);

            // get_grant_tenants_of_share
            let req = GetShareGrantTenantsReq {
                share_name: share_name.clone(),
            };
            let resp = mt.get_grant_tenants_of_share(req).await;
            assert!(resp.is_ok());
            let resp = resp.unwrap();
            assert_eq!(resp.accounts.len(), 1);
            assert_eq!(resp.accounts[0].account, account.to_string());
        }

        info!("--- share tenant2.share2 to tenant1");
        {
            let req = AddShareAccountsReq {
                share_name: share_name3.clone(),
                share_on,
                if_exists,
                accounts: vec![tenant_name1.to_string()],
            };

            // get share meta and check account has been added
            let res = mt.add_share_tenants(req).await?;
            let share_spec = res.share_spec.unwrap();
            assert_eq!(&share_spec.name, share_name3.share_name());
            assert_eq!(share_spec.tenants.len(), 1);
            assert_eq!(share_spec.tenants[0], tenant_name1.to_string());
        }

        // test show share api
        info!("--- show share check account information");
        {
            let req = ShowSharesReq {
                tenant: tenant1.clone(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();

            assert_eq!(resp.outbound_accounts.len(), 2);
            assert_eq!(resp.outbound_accounts[0].share_name, share_name.clone());
            assert_eq!(resp.outbound_accounts[0].create_on, create_on.clone());
            assert_eq!(
                resp.outbound_accounts[0].comment,
                Some(comment1.to_string())
            );
            assert_eq!(resp.outbound_accounts[1].share_name, share_name2.clone());
            assert_eq!(resp.outbound_accounts[1].create_on, create_on.clone());
            assert_eq!(
                resp.outbound_accounts[1].comment,
                Some(comment2.to_string())
            );
            assert!(resp.outbound_accounts[0].accounts.is_some());
            assert!(resp.outbound_accounts[1].accounts.is_some());
            let accounts = resp.outbound_accounts[0].accounts.as_ref().unwrap();
            assert_eq!(accounts.len(), 1);
            assert_eq!(accounts[0], account.to_string());
            assert_eq!(
                resp.outbound_accounts[1].accounts.as_ref().unwrap().len(),
                0
            );
        }

        info!("--- add account account1 again");
        {
            let req = AddShareAccountsReq {
                share_name: share_name.clone(),
                share_on,
                if_exists,
                accounts: vec![account.to_string()],
            };

            let res = mt.add_share_tenants(req).await;
            info!("add share account res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::SHARE_ACCOUNTS_ALREADY_EXISTS,
                ErrorCode::from(err).code()
            );
        }

        info!("--- add account account2");
        {
            let req = AddShareAccountsReq {
                share_name: share_name.clone(),
                share_on,
                if_exists,
                accounts: vec![account2.to_string()],
            };

            let res = mt.add_share_tenants(req).await?;
            let share_spec = res.share_spec.unwrap();
            assert_eq!(&share_spec.name, share_name.share_name());
            assert_eq!(share_spec.tenants.len(), 2);
            assert!(share_spec.tenants.contains(&account2.to_string()));

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account2.to_string()));
        }

        info!("--- remove account account2");
        {
            let req = RemoveShareAccountsReq {
                share_name: share_name.clone(),
                if_exists,
                accounts: vec![account2.to_string()],
            };

            let res = mt.remove_share_tenants(req).await?;
            info!("remove share account res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(&share_spec.name, share_name.share_name());
            assert_eq!(share_spec.tenants.len(), 1);
            assert!(!share_spec.tenants.contains(&account2.to_string()));

            // check account2 has been removed from share_meta
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(!share_meta.has_account(&account2.to_string()));

            // check share account meta has been removed
            let share_account_name = ShareConsumerIdent::new(
                Tenant::new_or_err(account2, "share_add_remove_account")?,
                share_id,
            );
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UNKNOWN_SHARE_ACCOUNTS,
                ErrorCode::from(err).code()
            );
        }

        info!("--- drop share1 with if_exists=true");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await?;
            let share_spec = res.share_spec.unwrap();
            assert_eq!(&share_spec.name, share_name.share_name());

            // check share account meta has been removed
            let share_account_name = ShareConsumerIdent::new(
                Tenant::new_or_err(account, "share_add_remove_account")?,
                share_id,
            );
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UNKNOWN_SHARE_ACCOUNTS,
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }

    #[minitrace::trace]
    async fn share_grant_revoke_object<
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "share1";
        let db_name = "db1";
        let tbl_name = "table1";
        let db2_name = "db2";
        let tbl2_name = "table2";

        let share_name = ShareNameIdent::new(&tenant, share1);
        let share_id: u64;
        let db_id: u64;
        let table_id: u64;

        info!("--- create share1,db1,table1,table2");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(ShareNameIdentRaw::from(share_name.clone()), share_name_ret);

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
            db_id = res.db_id;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
            table_id = res.table_id;

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db2_name),
                meta: DatabaseMeta::default(),
            };

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl2_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table2 res: {:?}", res);

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db2_name.to_string(),
                    table_name: tbl2_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
        }

        info!("--- grant unknown db2,table2");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database("unknown_db".to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());

            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(
                    db_name.to_string(),
                    "unknown_table".to_string(),
                ),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_TABLE, ErrorCode::from(err).code());
        }

        info!("--- grant unknown share2");
        {
            let req = GrantShareObjectReq {
                share_name: ShareNameIdent::new(&tenant, "share2"),
                object: ShareGrantObjectName::Database("db2".to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_SHARE, ErrorCode::from(err).code());
        }

        info!("--- grant table2 on a unbound database share");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db2_name.to_string(), tbl2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::WRONG_SHARE_OBJECT, ErrorCode::from(err).code());
        }

        info!("--- grant db object and table object");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, *share_name.name());
            assert_eq!(share_spec.database.unwrap().name, db_name.to_string());
            assert_eq!(
                share_spec.db_privileges,
                Some(BitFlags::from(ShareGrantObjectPrivilege::Usage))
            );
            assert!(res.grant_share_table.is_none());

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: tbl_ob_name.clone(),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, *share_name.name());
            assert_eq!(share_spec.tables.len(), 1);
            assert_eq!(share_spec.tables[0].name, tbl_name.to_string());
            assert_eq!(share_spec.tables[0].database_id, db_id);
            assert_eq!(share_spec.tables[0].table_id, table_id);
            assert_eq!(res.grant_share_table.unwrap().1.name, tbl_name.to_string());

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;

            match share_meta.database {
                Some(entry) => match entry.object {
                    ShareGrantObject::Database(obj_db_id) => {
                        assert_eq!(obj_db_id, db_id);

                        assert_eq!(entry.grant_on, create_on);
                        assert_eq!(
                            entry.privileges,
                            BitFlags::from(ShareGrantObjectPrivilege::Usage)
                        );
                    }
                    _ => {
                        panic!("MUST has database entry!")
                    }
                },
                None => {
                    panic!("MUST has database entry!")
                }
            }

            let object = ShareGrantObject::Table(table_id);
            if let Some(entry) = share_meta.entries.get(&object.to_string()) {
                assert_eq!(entry.object, object);
                assert_eq!(entry.grant_on, create_on);
                assert_eq!(
                    entry.privileges,
                    BitFlags::from(ShareGrantObjectPrivilege::Usage)
                );
            } else {
                panic!("MUST has table entry!")
            }
        }

        info!("--- grant db2, table2 on another bounded database share");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::WRONG_SHARE_OBJECT, ErrorCode::from(err).code());

            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db2_name.to_string(), tbl2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::WRONG_SHARE_OBJECT, ErrorCode::from(err).code());
        }

        info!("--- check db and table shared_by field");
        {
            let mut shared_by = BTreeSet::new();
            shared_by.insert(share_id);

            {
                let id_key = DatabaseId { db_id };

                let db_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(db_meta.shared_by, shared_by);
            }

            {
                let id_key = TableId { table_id };

                let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(table_meta.shared_by, shared_by);
            }
        }

        info!("--- revoke share of table");
        {
            let req = RevokeShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
                update_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.revoke_share_object(req).await?;
            info!("revoke object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, share_name.share_name().to_string());
            assert_eq!(
                res.revoke_object,
                Some(ShareObject::Table((db_id, table_id, tbl_name.to_string())))
            );

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;

            match share_meta.database {
                Some(entry) => match entry.object {
                    ShareGrantObject::Database(obj_db_id) => {
                        assert_eq!(obj_db_id, db_id);

                        assert_eq!(entry.grant_on, create_on);
                        assert_eq!(
                            entry.privileges,
                            BitFlags::from(ShareGrantObjectPrivilege::Usage)
                        );
                    }
                    _ => {
                        panic!("MUST has database entry!")
                    }
                },
                None => {
                    panic!("MUST has database entry!")
                }
            }

            let object = ShareGrantObject::Table(table_id);
            assert!(share_meta.entries.get(&object.to_string()).is_none());
        }

        info!("--- check db and table shared_by field");
        {
            let mut shared_by = BTreeSet::new();
            shared_by.insert(share_id);

            {
                let id_key = DatabaseId { db_id };

                let db_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(db_meta.shared_by, shared_by);
            }

            {
                let id_key = TableId { table_id };

                let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(table_meta.shared_by, BTreeSet::new());
            }
        }

        info!("--- grant share of table again, and revoke the database");
        {
            // first grant share table again
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            let share_spec = res.share_spec.unwrap();
            assert_eq!(share_spec.name, share_name.share_name().to_string());
            assert_eq!(share_spec.tables.len(), 1);
            assert_eq!(share_spec.tables[0].name, tbl_name.to_string());
            assert_eq!(share_spec.tables[0].database_id, db_id);
            assert_eq!(share_spec.tables[0].table_id, table_id);
            assert_eq!(res.grant_share_table.unwrap().1.name, tbl_name.to_string());

            // assert table share exists
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            let object = ShareGrantObject::Table(table_id);
            assert!(share_meta.entries.get(&object.to_string()).is_some());

            // then revoke the database
            let req = RevokeShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                update_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.revoke_share_object(req).await?;
            info!("revoke object res: {:?}", res);
            assert_eq!(res.share_spec.unwrap().name, *share_name.name());
            assert_eq!(res.revoke_object, Some(ShareObject::Db(db_id)));

            // assert share_meta.database is none, and share_meta.entries is empty
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.database.is_none());
            assert!(share_meta.entries.is_empty());
        }

        info!("--- check db and table shared_by field");
        {
            {
                let id_key = DatabaseId { db_id };

                let db_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(db_meta.shared_by, BTreeSet::new());
            }

            {
                let id_key = TableId { table_id };

                let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(table_meta.shared_by, BTreeSet::new());
            }
        }

        Ok(())
    }

    #[minitrace::trace]
    async fn get_share_grant_objects<
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "share1";
        let db_name = "db1";
        let tbl_name = "table1";

        let share_name = ShareNameIdent::new(&tenant, share1);

        info!("--- get unknown share");
        {
            let req = GetShareGrantObjectReq {
                share_name: share_name.clone(),
            };

            let res = mt.get_share_grant_objects(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_SHARE, ErrorCode::from(err).code());
        }

        info!("--- create share1");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
        }

        info!("--- get share");
        {
            let req = GetShareGrantObjectReq {
                share_name: share_name.clone(),
            };

            let res = mt.get_share_grant_objects(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let res = res.unwrap();
            assert!(res.objects.is_empty());
        }

        info!("--- create db1,table1");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
        }

        info!("--- share db1 and table1");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: tbl_ob_name.clone(),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
            assert_eq!(res.share_spec.unwrap().name, share1.to_string());
        }

        info!("--- get all share objects");
        {
            let req = GetShareGrantObjectReq {
                share_name: share_name.clone(),
            };

            let res = mt.get_share_grant_objects(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(res.objects.len(), 2);
        }

        Ok(())
    }

    #[minitrace::trace]
    async fn get_grant_privileges_of_object<
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "share1";
        let share2 = "share2";
        let share3 = "share3";
        let db_name = "db1";
        let tbl_name = "table1";
        let share_id;

        let share_name1 = ShareNameIdent::new(&tenant, share1);
        let share_name2 = ShareNameIdent::new(&tenant, share2);
        let share_name3 = ShareNameIdent::new(&tenant, share3);

        info!("--- get unknown object");
        {
            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Database("db".to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Table("db".to_string(), "table".to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
        }

        info!("--- create share1 and share2");
        let create_on = Utc::now();
        let grant_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name1.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            assert!(res.is_ok());
            share_id = res.unwrap().share_id;

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name2.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            assert!(res.is_ok());
        }

        info!("--- create db1,table1");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
        }

        info!("--- share db1 and table1");
        {
            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name1.clone(),
                object: tbl_ob_name.clone(),
                grant_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
        }

        info!("--- get_grant_privileges_of_object of db and table");
        {
            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 2);
            assert_eq!(&res.privileges[0].share_name, share1);
            assert_eq!(res.privileges[0].grant_on, grant_on);

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 1);
            assert_eq!(&res.privileges[0].share_name, share1);
            assert_eq!(res.privileges[0].grant_on, grant_on);
        }

        info!("--- drop share1 and check objects");
        {
            let tenant_name2 = "tenant1";
            let tenant2 = Tenant::new_literal(tenant_name2);
            let db2 = "db2";

            let db_name2 = DatabaseNameIdent::new(&tenant2, db2);

            // first grant account tenant2
            let req = AddShareAccountsReq {
                share_name: share_name1.clone(),
                share_on: Utc::now(),
                if_exists: false,
                accounts: vec![tenant_name2.to_string()],
            };
            let res = mt.add_share_tenants(req).await;
            assert!(res.is_ok());

            // tenant2 create a database from share1
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name2.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name1.clone().into()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            // cannot share a database created from a share
            {
                let req = CreateShareReq {
                    if_not_exists: false,
                    share_name: share_name3.clone(),
                    comment: None,
                    create_on,
                };

                let res = mt.create_share(req).await;
                assert!(res.is_ok());

                let req = GrantShareObjectReq {
                    share_name: share_name3.clone(),
                    object: ShareGrantObjectName::Database(db2.to_string()),
                    grant_on,
                    privilege: ShareGrantObjectPrivilege::Usage,
                };

                let res = mt.grant_share_object(req).await;
                assert!(res.is_err());
                let err = res.unwrap_err();
                assert_eq!(
                    ErrorCode::CANNOT_SHARE_DATABASE_CREATED_FROM_SHARE,
                    ErrorCode::from(err).code()
                );
            }

            let req = DropShareReq {
                if_exists: true,
                share_name: share_name1.clone(),
            };

            // get share meta
            let share_id_key = ShareId { share_id };
            let share_meta: ShareMeta = get_kv_data(mt.as_kv_api(), &share_id_key).await?;

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            // check if all the share data has been removed
            let res =
                is_all_share_data_removed(mt.as_kv_api(), &share_name1, share_id, &share_meta)
                    .await?;
            assert!(res);

            // get_grant_privileges_of_object of db and table again
            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 1);

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 0);
        }

        Ok(())
    }

    #[minitrace::trace]
    async fn drop_share_database_and_table<
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let share1 = "drop_share_database_and_table_share";
        let share2 = "drop_share_database_and_table_share2";
        let db_name = "drop_share_database_and_table_db";
        let tbl_name = "drop_share_database_and_table_table";
        let share_id: u64;
        let share_id2: u64;
        let db_id: u64;
        let table_id: u64;

        let share_name = ShareNameIdent::new(&tenant, share1);
        let share_name2 = ShareNameIdent::new(&tenant, share2);

        info!("--- create share1 and share2");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first share id is 1");
            share_id = res.share_id;

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name2.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            let res = res.unwrap();
            share_id2 = res.share_id;
        }

        info!("--- create db1,table1");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
            db_id = res.db_id;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            info!("create table res: {:?}", res);
            table_id = res.table_id;
        }

        info!("--- share db1 and table1 to share1 and share2");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: tbl_ob_name.clone(),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);

            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _res = mt.grant_share_object(req).await?;

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name2.clone(),
                object: tbl_ob_name.clone(),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_share_object(req).await?;
            info!("grant object res: {:?}", res);
        }

        info!("--- check db and table shared_by field");
        {
            let mut shared_by = BTreeSet::new();
            shared_by.insert(share_id);
            shared_by.insert(share_id2);

            {
                let id_key = DatabaseId { db_id };

                let db_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(db_meta.shared_by, shared_by);
            }

            {
                let id_key = TableId { table_id };

                let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(table_meta.shared_by, shared_by);
            }
        }

        info!("--- drop share2 and check db\table shared_by field");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name2.clone(),
            };

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            let mut shared_by = BTreeSet::new();
            shared_by.insert(share_id);

            {
                let id_key = DatabaseId { db_id };

                let db_meta: DatabaseMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(db_meta.shared_by, shared_by);
            }

            {
                let id_key = TableId { table_id };

                let table_meta: TableMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
                assert_eq!(table_meta.shared_by, shared_by);
            }
        }

        info!("--- drop share table");
        {
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            let table_key_name = ShareGrantObject::Table(table_id).to_string();
            assert!(share_meta.entries.contains_key(&table_key_name));

            let plan = DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                table_name: tbl_name.to_string(),
                tb_id: table_id,
                db_id,
            };
            let _res = mt.drop_table_by_id(plan).await;

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(!share_meta.entries.contains_key(&table_key_name));
        }

        info!("--- drop share database");
        {
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.database.is_some());

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
            })
            .await?;

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.database.is_none());
        }

        Ok(())
    }
}
