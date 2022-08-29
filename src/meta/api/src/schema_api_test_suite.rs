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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Duration;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdToName;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DbIdList;
use common_meta_app::schema::DbIdListKey;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdList;
use common_meta_app::schema::TableIdListKey;
use common_meta_app::schema::TableIdToName;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_app::share::AddShareAccountsReq;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::GrantShareObjectReq;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use common_meta_app::share::ShareId;
use common_meta_app::share::ShareMeta;
use common_meta_app::share::ShareNameIdent;
use common_meta_types::GCDroppedDataReq;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::UpsertKVReq;
use tracing::debug;
use tracing::info;

use crate::get_kv_data;
use crate::is_all_db_data_removed;
use crate::serialize_struct;
use crate::ApiBuilder;
use crate::AsKVApi;
use crate::KVApi;
use crate::KVApiKey;
use crate::SchemaApi;
use crate::ShareApi;

/// Test suite of `SchemaApi`.
///
/// It is not used by this crate, but is used by other crate that impl `SchemaApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct SchemaApiTestSuite {}

#[derive(PartialEq, Default, Debug)]
struct DroponInfo {
    pub name: String,
    pub desc: String,
    pub drop_on_cnt: i32,
    pub non_drop_on_cnt: i32,
}

macro_rules! assert_meta_eq_without_updated {
    ($a: expr, $b: expr, $msg: expr) => {
        let mut aa = $a.clone();
        aa.meta.updated_on = $b.meta.updated_on;
        assert_eq!(aa, $b, $msg);
    };
}

fn calc_and_compare_drop_on_db_result(result: Vec<Arc<DatabaseInfo>>, expected: Vec<DroponInfo>) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut get = BTreeMap::new();
    for item in result.iter() {
        let name = item.name_ident.to_string();
        let mut drop_on_info = match get.get_mut(&name) {
            Some(drop_on_info) => drop_on_info,
            None => {
                let info = DroponInfo {
                    name: name.clone(),
                    desc: "".to_string(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 0,
                };
                get.insert(name.clone(), info);
                get.get_mut(&name).expect("")
            }
        };
        if item.meta.drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(get, expected_map);
}

fn calc_and_compare_drop_on_table_result(result: Vec<Arc<TableInfo>>, expected: Vec<DroponInfo>) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut get = BTreeMap::new();
    for item in result.iter() {
        let name = item.name.clone();
        let mut drop_on_info = match get.get_mut(&name) {
            Some(drop_on_info) => drop_on_info,
            None => {
                let info = DroponInfo {
                    name: item.name.clone(),
                    desc: item.desc.clone(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 0,
                };
                get.insert(name.clone(), info);
                get.get_mut(&name).expect("")
            }
        };
        if item.meta.drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(get, expected_map);
}

async fn upsert_test_data(
    kv_api: &(impl KVApi + ?Sized),
    key: &impl KVApiKey,
    value: Vec<u8>,
) -> Result<u64, MetaError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: key.to_key(),
            seq: MatchSeq::Any,
            value: Operation::Update(value),
            value_meta: None,
        })
        .await?;

    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

async fn delete_test_data(
    kv_api: &(impl KVApi + ?Sized),
    key: &impl KVApiKey,
) -> Result<(), MetaError> {
    let _res = kv_api
        .upsert_kv(UpsertKVReq {
            key: key.to_key(),
            seq: MatchSeq::Any,
            value: Operation::Delete,
            value_meta: None,
        })
        .await?;

    Ok(())
}

impl SchemaApiTestSuite {
    /// Test SchemaAPI on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: ApiBuilder<MT>,
        MT: ShareApi + AsKVApi + SchemaApi,
    {
        let suite = SchemaApiTestSuite {};

        suite.database_and_table_rename(&b.build().await).await?;
        suite.database_create_get_drop(&b.build().await).await?;
        suite
            .database_create_from_share_and_drop(&b.build().await)
            .await?;
        suite
            .database_create_get_drop_in_diff_tenant(&b.build().await)
            .await?;
        suite.database_list(&b.build().await).await?;
        suite.database_list_in_diff_tenant(&b.build().await).await?;
        suite.database_rename(&b.build().await).await?;
        suite
            .database_drop_undrop_list_history(&b.build().await)
            .await?;
        suite
            .database_drop_out_of_retention_time_history(&b.build().await)
            .await?;

        suite.table_create_get_drop(&b.build().await).await?;
        suite.table_rename(&b.build().await).await?;
        suite.table_update_meta(&b.build().await).await?;
        suite.table_upsert_option(&b.build().await).await?;
        suite.table_list(&b.build().await).await?;
        suite
            .table_drop_undrop_list_history(&b.build().await)
            .await?;
        suite
            .database_gc_out_of_retention_time(&b.build().await)
            .await?;
        suite
            .table_gc_out_of_retention_time(&b.build().await)
            .await?;
        suite
            .table_drop_out_of_retention_time_history(&b.build().await)
            .await?;
        suite.get_table_by_id(&b.build().await).await?;
        Ok(())
    }

    /// Test SchemaAPI on cluster
    pub async fn test_cluster<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: ApiBuilder<MT>,
        MT: SchemaApi,
    {
        let suite = SchemaApiTestSuite {};

        // leader-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[0], &cluster[1]).await?;
        }

        // follower-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[2], &cluster[1]).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_and_table_rename<MT: SchemaApi + AsKVApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let db2_name = "db2";
        let db3_name = "db3";
        let table_name = "table";
        let table2_name = "table2";
        let table3_name = "table3";
        let db_id;
        let db3_id;
        let table_id;

        let db_name_ident = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };
        let db2_name_ident = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: db2_name.to_string(),
        };

        let db_table_name_ident = TableNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        };

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        let req = CreateTableReq {
            if_not_exists: false,
            name_ident: db_table_name_ident.clone(),
            table_meta: table_meta(created_on),
        };

        {
            info!("--- prepare db1,db3 and table");
            // prepare db1
            let res = self.create_database(mt, tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_id = res.db_id;

            let res = self.create_database(mt, tenant, "db3", "eng1").await?;
            db3_id = res.db_id;

            let res = mt.create_table(req).await?;
            table_id = res.table_id;

            let db_id_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdent =
                get_kv_data(mt.as_kv_api(), &db_id_name_key).await?;
            assert_eq!(ret_db_name_ident, db_name_ident);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table_name.to_string()
            });
        }

        {
            info!("--- rename exists db db1 to not exists mutable db2");
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
                new_db_name: db2_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let db_id_2_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdent =
                get_kv_data(mt.as_kv_api(), &db_id_2_name_key).await?;
            assert_eq!(ret_db_name_ident, db2_name_ident);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table_name.to_string()
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable table2");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db2_name.to_string(),
                        table_name: table_name.to_string(),
                    },
                    new_db_name: db2_name.to_string(),
                    new_table_name: table2_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table2_name.to_string()
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable db3.table3");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db2_name.to_string(),
                        table_name: table2_name.to_string(),
                    },
                    new_db_name: db3_name.to_string(),
                    new_table_name: table3_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: db3_id,
                table_name: table3_name.to_string()
            });
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_create_get_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        info!("--- create db1");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create db1 again with if_not_exists=false");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::DatabaseAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        info!("--- create db1 again with if_not_exists=true");
        {
            let req = CreateDatabaseReq {
                if_not_exists: true,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "db1 id is 1");
        }

        info!("--- get db1");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db1")).await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1".to_string(), res.name_ident.db_name, "db1.db is db1");
        }

        info!("--- create db2");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db2".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                6, res.db_id,
                "second database id is 4: seq increment but no used"
            );
        }

        info!("--- get db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db2")).await?;
            assert_eq!("db2".to_string(), res.name_ident.db_name, "db1.db is db1");
        }

        info!("--- get absent db");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "absent")).await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err_code = ErrorCode::from(err);

            assert_eq!(1003, err_code.code());
            assert!(err_code.message().contains("absent"));
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db2".to_string(),
                },
            })
            .await?;
        }

        info!("--- get db2 should not found");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db2")).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );
        }

        info!("--- drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db2".to_string(),
                },
            })
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_create_from_share_and_drop<MT: ShareApi + AsKVApi + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = "tenant1";
        let tenant2 = "tenant2";
        let db1 = "db1";
        let db2 = "db2";
        let share = "share";
        let share_name = ShareNameIdent {
            tenant: tenant2.to_string(),
            share_name: share.to_string(),
        };
        let db_name1 = DatabaseNameIdent {
            tenant: tenant1.to_string(),
            db_name: db1.to_string(),
        };
        let db_name2 = DatabaseNameIdent {
            tenant: tenant2.to_string(),
            db_name: db2.to_string(),
        };
        let db_id;
        let share_id;

        info!("--- tenant1 create db1 from an unknown share");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name1.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name.clone()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
        };

        info!("--- create a share and tenant1 create db1 from a share");
        {
            // first create the share but not grant the tenant
            let create_on = Utc::now();
            let share_on = Utc::now();
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("create share res: {:?}", res);
            assert!(res.is_ok());
            // save the share id
            share_id = res.unwrap().share_id;

            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name1.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name.clone()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareAccounts("").code(),
                ErrorCode::from(err).code()
            );

            // grant the tenant to access the share
            let req = AddShareAccountsReq {
                share_name: share_name.clone(),
                share_on,
                if_exists: false,
                accounts: vec![tenant1.to_string()],
            };

            let res = mt.add_share_tenants(req).await;
            assert!(res.is_ok());

            // try again create database from the share
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name1.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name.clone()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::ShareHasNoGrantedDatabase("").code(),
                ErrorCode::from(err).code()
            );

            // create database of tenant2
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name2.clone(),
                meta: DatabaseMeta {
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            // and grant access to database from the share
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db2.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let _ = mt.grant_share_object(req).await?;

            // after grant access to database, create database from share MUST succeeds
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name1.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name.clone()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());
            // save the db id
            db_id = res.unwrap().db_id;
        };

        // drop database created from share
        {
            // check that share db id contains the db id
            let share_id_key = ShareId { share_id };
            let share_meta: ShareMeta = get_kv_data(mt.as_kv_api(), &share_id_key).await?;
            assert!(share_meta.has_share_from_db_id(db_id));

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name1.clone(),
            })
            .await?;

            // check that share db id has removed the db id
            let share_id_key = ShareId { share_id };
            let share_meta: ShareMeta = get_kv_data(mt.as_kv_api(), &share_id_key).await?;
            assert!(!share_meta.has_share_from_db_id(db_id));

            // check that DatabaseMeta has been removed
            let res = is_all_db_data_removed(mt.as_kv_api(), db_id).await?;
            assert!(res);

            // db has been removed, so undrop_database MUST return error
            let res = mt
                .undrop_database(UndropDatabaseReq {
                    name_ident: db_name1.clone(),
                })
                .await;
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UndropDbHasNoHistory("").code(),
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_create_get_drop_in_diff_tenant<MT: SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = "tenant1";
        let tenant2 = "tenant2";
        info!("--- tenant1 create db1");
        let db_id_1 = {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
            res.db_id
        };

        info!("--- tenant1 create db2");
        let db_id_2 = {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: "db2".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert!(res.db_id > db_id_1, "second database id is > {}", db_id_1);
            res.db_id
        };

        info!("--- tenant2 create db1");
        let _db_id_3 = {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant2.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert!(res.db_id > db_id_2, "third database id > {}", db_id_2);
            res.db_id
        };

        info!("--- tenant1 get db1");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant1, "db1")).await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1".to_string(), res.name_ident.db_name, "db1.db is db1");
        }

        info!("--- tenant1 get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1, "absent"))
                .await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);

            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert!(err.message().contains("absent"));
        }

        info!("--- tenant2 get tenant1's db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant2, "db2")).await;
            debug!("=== get other tenant's database res: {:?}", res);
            assert!(res.is_err());
            let res = res.unwrap_err();
            let err = ErrorCode::from(res);

            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert_eq!("Unknown database 'db2'".to_string(), err.message());
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: "db2".to_string(),
                },
            })
            .await?;
        }

        info!("--- tenant1 get db2 should not found");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant1, "db2")).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );
        }

        info!("--- tenant1 drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: "db2".to_string(),
                },
            })
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_list<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let mut db_ids = vec![];
        let db_names = vec!["db1", "db2"];
        let engines = vec!["eng1", "eng2"];
        let tenant = "tenant1";
        {
            let res = self.create_database(mt, tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_ids.push(res.db_id);

            let res = self.create_database(mt, tenant, "db2", "eng2").await?;
            assert!(res.db_id > 1);
            db_ids.push(res.db_id);
        }

        info!("--- list_databases");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;

            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids, got);

            for (i, db_info) in dbs.iter().enumerate() {
                assert_eq!(tenant, db_info.name_ident.tenant);
                assert_eq!(db_names[i], db_info.name_ident.db_name);
                assert_eq!(db_ids[i], db_info.ident.db_id);
                assert_eq!(engines[i], db_info.meta.engine);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_list_in_diff_tenant<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let tenant1 = "tenant1";
        let tenant2 = "tenant2";

        let mut db_ids = vec![];
        {
            let res = self.create_database(mt, tenant1, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_ids.push(res.db_id);

            let res = self.create_database(mt, tenant1, "db2", "eng2").await?;
            assert!(res.db_id > 1);
            db_ids.push(res.db_id);
        }

        let db_id_3 = {
            let res = self.create_database(mt, tenant2, "db3", "eng1").await?;
            res.db_id
        };

        info!("--- get_databases by tenant1");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant1.to_string(),
                })
                .await?;
            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids, got)
        }

        info!("--- get_databases by tenant2");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant2.to_string(),
                })
                .await?;
            let want: Vec<u64> = vec![db_id_3];
            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_rename<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let db2_name = "db2";
        let new_db_name = "db3";

        info!("--- rename not exists db1 to not exists db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                new_db_name: new_db_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare db1 and db2");
        {
            // prepare db2
            let res = self.create_database(mt, tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);

            info!("--- rename not exists db4 to exists db1");
            {
                let req = RenameDatabaseReq {
                    if_exists: false,
                    name_ident: DatabaseNameIdent {
                        tenant: tenant.to_string(),
                        db_name: "db4".to_string(),
                    },
                    new_db_name: db_name.to_string(),
                };

                let res = mt.rename_database(req).await;
                info!("rename database res: {:?}", res);
                assert!(res.is_err());
                assert_eq!(
                    ErrorCode::UnknownDatabase("").code(),
                    ErrorCode::from(res.unwrap_err()).code()
                );
            }

            // prepare db2
            let res = self.create_database(mt, tenant, "db2", "eng1").await?;
            assert!(res.db_id > 1);
        }

        info!("--- rename exists db db1 to exists db db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::DatabaseAlreadyExists("").code(),
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- rename exists db db1 to not exists mutable db");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },

                new_db_name: new_db_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let res = mt
                .get_database(GetDatabaseReq::new(tenant, new_db_name))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db3 id is 1");
            assert_eq!("db3".to_string(), res.name_ident.db_name, "db3.db is db3");

            info!("--- get old database after rename");
            {
                let res = mt.get_database(GetDatabaseReq::new(tenant, db_name)).await;
                let err = res.err().unwrap();
                assert_eq!(
                    ErrorCode::UnknownDatabase("").code(),
                    ErrorCode::from(err).code()
                );
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_drop_undrop_list_history<MT: SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1_database_drop_undrop_list_history";
        let db_name = "db1_database_drop_undrop_list_history";
        let new_db_name = "db2_database_drop_undrop_list_history";
        let db_name_ident = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };
        let new_db_name_ident = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: new_db_name.to_string(),
        };

        info!("--- create and drop db1");
        {
            // first create database
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string(),
                desc: "".to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);

            // then drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // undrop db1
            mt.undrop_database(UndropDatabaseReq {
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string(),
                desc: "".to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create db1");
        {
            // first drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create database
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let _res = mt.create_database(req).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- create and rename db2");
        {
            // first create db2
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: new_db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let _res = mt.create_database(req).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            // then drop db2
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: new_db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // rename db1 to db2
            mt.rename_database(RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                new_db_name: new_db_name.to_string(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_create_get_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";
        let mut expected_tb_count: u64 = 0;

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };

        let unknown_database_code = ErrorCode::UnknownDatabase("").code();

        info!("--- create or get table on unknown db");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },

                table_meta: table_meta(created_on),
            };
            // test create table
            {
                let res = mt.create_table(req).await;
                debug!("create table on unknown db res: {:?}", res);

                assert!(res.is_err());
                let err = res.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            };
            // test get table
            {
                let got = mt.get_table((tenant, db_name, tbl_name).into()).await;
                debug!("get table on unknown db got: {:?}", got);

                assert!(got.is_err());
                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            }
        }

        info!("--- drop table on unknown db");
        {
            // casually create a drop table plan
            // should be not vunerable?
            let plan = DropTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.into(),
                    db_name: db_name.into(),
                    table_name: tbl_name.into(),
                },
            };

            let got = mt.drop_table(plan).await;
            debug!("--- drop table on unknown database got: {:?}", got);

            assert!(got.is_err());
            let code = ErrorCode::from(got.unwrap_err()).code();
            assert_eq!(unknown_database_code, code);
        }

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- create tb2 and get table");
        let created_on = Utc::now();

        let mut req = CreateTableReq {
            if_not_exists: false,
            name_ident: TableNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: table_meta(created_on),
        };
        let tb_ident_2 = {
            {
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");

                let tb_id = res.table_id;

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident: ident.clone(),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            }
        };

        expected_tb_count += 1;
        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- create table again with if_not_exists = true");
        {
            req.if_not_exists = true;
            let res = mt.create_table(req.clone()).await?;
            assert_eq!(
                tb_ident_2.table_id, res.table_id,
                "new table id is still the same"
            );

            let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
            let want = TableInfo {
                ident: tb_ident_2.clone(),
                desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
        }

        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- create table again with if_not_exists = false");
        {
            req.if_not_exists = false;

            let res = mt.create_table(req).await;
            info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(
                format!(
                    "Code: 2302, displayText = Table '{}' already exists.",
                    tbl_name
                ),
                err_code.to_string()
            );

            // get_table returns the old table

            let got = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
            let want = TableInfo {
                ident: tb_ident_2.clone(),
                desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get old table");
        }

        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- create another table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: "tb3".to_string(),
                },
                table_meta: table_meta(created_on),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(
                res.table_id > tb_ident_2.table_id,
                "table id > {}",
                tb_ident_2.table_id
            );
        }

        expected_tb_count += 1;
        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- drop table");
        {
            info!("--- drop table with if_exists = false");
            {
                let plan = DropTableReq {
                    if_exists: false,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                        table_name: tbl_name.to_string(),
                    },
                };
                mt.drop_table(plan.clone()).await?;

                info!("--- get table after drop");
                {
                    let res = mt.get_table((tenant, db_name, tbl_name).into()).await;
                    let status = res.err().unwrap();
                    let err_code = ErrorCode::from(status);

                    assert_eq!(
                        format!("Code: 1025, displayText = Unknown table '{:}'.", tbl_name),
                        err_code.to_string(),
                        "get dropped table {}",
                        tbl_name
                    );
                }
            }

            info!("--- drop table with if_exists = false again, error");
            {
                let plan = DropTableReq {
                    if_exists: false,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                        table_name: tbl_name.to_string(),
                    },
                };
                let res = mt.drop_table(plan.clone()).await;
                let err = res.unwrap_err();
                assert_eq!(
                    ErrorCode::UnknownTable("").code(),
                    ErrorCode::from(err).code(),
                    "drop table {} with if_exists=false again",
                    tbl_name
                );
            }

            info!("--- drop table with if_exists = true again, ok");
            {
                let plan = DropTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                        table_name: tbl_name.to_string(),
                    },
                };
                mt.drop_table(plan.clone()).await?;
            }

            expected_tb_count -= 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_rename<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db1_name = "db1";
        let tb2_name = "tb2";
        let db2_name = "db2";
        let tb3_name = "tb3";

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let rename_db1tb2_to_db1tb3 = |if_exists| RenameTableReq {
            if_exists,
            name_ident: TableNameIdent {
                tenant: tenant.to_string(),
                db_name: db1_name.to_string(),
                table_name: tb2_name.to_string(),
            },
            new_db_name: db1_name.to_string(),
            new_table_name: tb3_name.to_string(),
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- rename table on unknown db");
        {
            let got = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            debug!("--- rename table on unknown database got: {:?}", got);

            assert!(got.is_err());
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(got.unwrap_err()).code()
            );
        }

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db1_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
        }

        let created_on = Utc::now();
        let create_tb2_req = CreateTableReq {
            if_not_exists: false,
            name_ident: TableNameIdent {
                tenant: tenant.to_string(),
                db_name: db1_name.to_string(),
                table_name: tb2_name.to_string(),
            },
            table_meta: table_meta(created_on),
        };

        info!("--- create table for rename");
        let tb_ident = {
            let old_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            mt.create_table(create_tb2_req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            let got = mt.get_table((tenant, db1_name, tb2_name).into()).await?;
            got.ident.clone()
        };

        info!("--- rename table, ok");
        {
            let old_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            mt.rename_table(rename_db1tb2_to_db1tb3(false)).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let got = mt.get_table((tenant, db1_name, tb3_name).into()).await?;
            let want = TableInfo {
                ident: tb_ident.clone(),
                desc: format!("'{}'.'{}'.'{}'", tenant, db1_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
            };

            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");

            info!("--- get old table after rename");
            {
                let res = mt.get_table((tenant, db1_name, tb2_name).into()).await;
                let err = res.err().unwrap();
                assert_eq!(
                    ErrorCode::UnknownTable("").code(),
                    ErrorCode::from(err).code()
                );
            }
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownTable("").code(),
                ErrorCode::from(err).code(),
                "rename table {} again",
                tb2_name
            );
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), with if_exist=true, OK");
        {
            mt.rename_table(rename_db1tb2_to_db1tb3(true)).await?;
        }

        info!("--- create db1,db2, ok");
        let tb_ident2 = {
            let old_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            mt.create_table(create_tb2_req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let got = mt.get_table((tenant, db1_name, tb2_name).into()).await?;
            assert_ne!(tb_ident.table_id, got.ident.table_id);
            assert_ne!(tb_ident.seq, got.ident.seq);
            got.ident.clone()
        };

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TableAlreadyExists("").code(),
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), if_exists=true, error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(true)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TableAlreadyExists("").code(),
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- rename table to unknown db, error");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let res = mt.rename_table(req.clone()).await;
            debug!("--- rename table to other db got: {:?}", res);

            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare other db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db2_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            mt.create_database(plan).await?;
        }

        info!("--- rename table to other db, ok");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let old_db1 = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            let old_db2 = mt.get_database(Self::req_get_db(tenant, db2_name)).await?;
            mt.rename_table(req.clone()).await?;
            let cur_db1 = mt.get_database(Self::req_get_db(tenant, db1_name)).await?;
            let cur_db2 = mt.get_database(Self::req_get_db(tenant, db2_name)).await?;
            assert!(old_db1.ident.seq < cur_db1.ident.seq);
            assert!(old_db2.ident.seq < cur_db2.ident.seq);

            let got = mt.get_table((tenant, db2_name, tb3_name).into()).await?;
            let want = TableInfo {
                ident: tb_ident2,
                desc: format!("'{}'.'{}'.'{}'", tenant, db2_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_update_meta<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: Default::default(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident: ident.clone(),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- update table meta");
        {
            info!("--- update table meta, normal case");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };
                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                mt.update_table_meta(UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                })
                .await?;

                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta: version mismatch");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let new_table_meta = table.meta.clone();
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let res = mt
                    .update_table_meta(UpdateTableMetaReq {
                        table_id,
                        seq: MatchSeq::Exact(table_version + 1),
                        new_table_meta: new_table_meta.clone(),
                    })
                    .await;

                let err = res.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::table_version_mismatched_code(), err.code());
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_upsert_option<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident: ident.clone(),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- upsert table options");
        {
            info!("--- upsert table options with key1=val1");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                mt.upsert_table_option(UpsertTableOptionReq::new(&table.ident, "key1", "val1"))
                    .await?;

                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with key1=val1");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: table.ident.table_id,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::TableVersionMismatched("").code(), err.code());

                // table is not affected.
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with not exist table id");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: 1024,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UnknownTableId("").code(), err.code());

                // table is not affected.
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_drop_out_of_retention_time_history<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1_database_drop_out_of_retention_time_history";
        let db_name = "db1_database_drop_out_of_retention_time_history";
        let db_name_ident = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        info!("--- create and drop db1");
        {
            let drop_on = Some(Utc::now() - Duration::days(1));

            // first create database
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    // drop_on,
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;
            info!("create database res: {:?}", res);

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 1);

            let drop_data = DatabaseMeta {
                engine: "github".to_string(),
                drop_on,
                ..Default::default()
            };
            let id_key = DatabaseId { db_id };
            let data = serialize_struct(&drop_data)?;
            upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 0);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn create_out_of_retention_time_db<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
        db_name: DatabaseNameIdent,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<()> {
        let req = CreateDatabaseReq {
            if_not_exists: false,
            name_ident: db_name.clone(),
            meta: DatabaseMeta {
                engine: "github".to_string(),
                // drop_on,
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        let db_id = res.db_id;
        info!("create database res: {:?}", res);

        let drop_data = DatabaseMeta {
            engine: "github".to_string(),
            drop_on,
            ..Default::default()
        };
        let id_key = DatabaseId { db_id };
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

        if delete {
            delete_test_data(mt.as_kv_api(), &db_name).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn database_gc_out_of_retention_time<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1_database_gc_out_of_retention_time";
        let db_name = "db1_database_gc_out_of_retention_time";
        let db_name_ident1 = DatabaseNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        let dbid_idlist1 = DbIdListKey {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        let tenant2 = "tenant2_database_gc_out_of_retention_time";
        let db_name2 = "db2_database_gc_out_of_retention_time";
        let db_name_ident2 = DatabaseNameIdent {
            tenant: tenant2.to_string(),
            db_name: db_name2.to_string(),
        };

        let dbid_idlist2 = DbIdListKey {
            tenant: tenant2.to_string(),
            db_name: db_name2.to_string(),
        };

        let drop_on = Some(Utc::now() - Duration::days(1));

        // create db_name_ident1 with two dropped value
        self.create_out_of_retention_time_db(mt, db_name_ident1.clone(), drop_on, true)
            .await?;
        self.create_out_of_retention_time_db(
            mt,
            db_name_ident1.clone(),
            Some(Utc::now() - Duration::days(2)),
            false,
        )
        .await?;
        self.create_out_of_retention_time_db(mt, db_name_ident2.clone(), drop_on, false)
            .await?;

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist1).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 1);

        let req = GCDroppedDataReq {
            tenant: tenant.to_string(),
            table_at_least: 0,
            db_at_least: 1,
        };
        let res = mt.gc_dropped_data(req).await?;
        assert_eq!(res.gc_db_count, 2);

        // assert db id list has been cleaned
        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist1).await?;
        assert_eq!(id_list.len(), 0);

        // assert old db meta and id to name mapping has been removed
        for db_id in old_id_list.iter() {
            let id_key = DatabaseId { db_id: *db_id };
            let id_mapping = DatabaseIdToName { db_id: *db_id };
            let meta_res: Result<DatabaseMeta, MetaError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            let mapping_res: Result<DatabaseNameIdent, MetaError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 1);

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn create_out_of_retention_time_table<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
        name_ident: TableNameIdent,
        dbid_tbname: DBIdTableName,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<()> {
        let created_on = Utc::now();
        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            ..TableMeta::default()
        };

        let req = CreateTableReq {
            if_not_exists: false,
            name_ident,
            table_meta: create_table_meta.clone(),
        };

        let res = mt.create_table(req).await?;
        let table_id = res.table_id;
        info!("create table res: {:?}", res);

        let drop_data = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            drop_on,
            ..TableMeta::default()
        };

        let id_key = TableId { table_id };
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

        if delete {
            delete_test_data(mt.as_kv_api(), &dbid_tbname).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_gc_out_of_retention_time<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = "tenant1_table_gc_out_of_retention_time";
        let db1_name = "db1_table_gc_out_of_retention_time";
        let tb1_name = "tb1_table_gc_out_of_retention_time";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant1.to_string(),
            db_name: db1_name.to_string(),
            table_name: tb1_name.to_string(),
        };

        let plan = CreateDatabaseReq {
            if_not_exists: false,
            name_ident: DatabaseNameIdent {
                tenant: tenant1.to_string(),
                db_name: db1_name.to_string(),
            },
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let res = mt.create_database(plan).await?;
        info!("create database res: {:?}", res);

        assert_eq!(1, res.db_id, "first database id is 1");
        let drop_on = Some(Utc::now() - Duration::days(1));
        self.create_out_of_retention_time_table(
            mt,
            tbl_name_ident.clone(),
            DBIdTableName {
                db_id: res.db_id,
                table_name: tb1_name.to_string(),
            },
            drop_on,
            true,
        )
        .await?;
        self.create_out_of_retention_time_table(
            mt,
            tbl_name_ident.clone(),
            DBIdTableName {
                db_id: res.db_id,
                table_name: tb1_name.to_string(),
            },
            Some(Utc::now() - Duration::days(2)),
            false,
        )
        .await?;

        let table_id_idlist = TableIdListKey {
            db_id: res.db_id,
            table_name: tb1_name.to_string(),
        };

        // save old id list
        let id_list: TableIdList = get_kv_data(mt.as_kv_api(), &table_id_idlist).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        let req = GCDroppedDataReq {
            tenant: tenant1.to_string(),
            table_at_least: 1,
            db_at_least: 2,
        };
        let res = mt.gc_dropped_data(req).await?;
        assert_eq!(res.gc_table_count, 2);

        let id_list: TableIdList = get_kv_data(mt.as_kv_api(), &table_id_idlist).await?;
        assert_eq!(id_list.len(), 0);

        // assert old table meta and id to name mapping has been removed
        for table_id in old_id_list.iter() {
            let id_key = TableId {
                table_id: *table_id,
            };
            let id_mapping = TableIdToName {
                table_id: *table_id,
            };
            let meta_res: Result<DatabaseMeta, MetaError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            let mapping_res: Result<DBIdTableName, MetaError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_drop_out_of_retention_time_history<MT: SchemaApi + AsKVApi>(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant_table_drop_history";
        let db_name = "table_table_drop_history_db1";
        let tbl_name = "table_table_drop_history_tb1";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        let created_on = Utc::now();
        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            // drop_on: Some(created_on - Duration::days(1)),
            ..TableMeta::default()
        };
        info!("--- create and get table");
        {
            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            let table_id = res.table_id;
            assert!(table_id >= 1, "table id >= 1");

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;

            assert_eq!(res.len(), 1);

            let tbid = TableId { table_id };
            let create_drop_table_meta = TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                created_on,
                drop_on: Some(created_on - Duration::days(1)),
                ..TableMeta::default()
            };
            let data = serialize_struct(&create_drop_table_meta)?;

            upsert_test_data(mt.as_kv_api(), &tbid, data).await?;
            // assert not return out of retention time data
            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;

            assert_eq!(res.len(), 0);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_drop_undrop_list_history<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant_drop_undrop_list_history_db1";
        let db_name = "table_drop_undrop_list_history_db1";
        let tbl_name = "table_drop_undrop_list_history_tb2";
        let new_tbl_name = "new_table_drop_undrop_list_history_tb2";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        let new_tbl_name_ident = TableNameIdent {
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
            table_name: new_tbl_name.to_string(),
        };
        let mut expected_tb_count: u64 = 0;

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        let created_on = Utc::now();
        let create_table_meta = table_meta(created_on);
        info!("--- create and get table");
        {
            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(res.table_id >= 1, "table id >= 1");

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        expected_tb_count += 1;
        // check table count
        info!("--- check table count of tenant1");
        let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
        assert_eq!(expected_tb_count, tb_count.count);

        info!("--- drop and undrop table");
        {
            // first drop table
            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            mt.drop_table(DropTableReq {
                if_exists: false,
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count -= 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then undrop table
            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let plan = UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            };
            mt.undrop_table(plan).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count += 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create table");
        {
            // first drop table
            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            mt.drop_table(DropTableReq {
                if_exists: false,
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count -= 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create table
            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let res = mt
                .create_table(CreateTableReq {
                    if_not_exists: false,
                    name_ident: tbl_name_ident.clone(),
                    table_meta: create_table_meta.clone(),
                })
                .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(res.table_id >= 1, "table id >= 1");

            expected_tb_count += 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            // then drop table
            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            mt.drop_table(DropTableReq {
                if_exists: false,
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count -= 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 2,
                non_drop_on_cnt: 0,
            }]);

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            mt.undrop_table(UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count += 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: tbl_name_ident.to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            let res = mt
                .undrop_table(UndropTableReq {
                    name_ident: tbl_name_ident.clone(),
                })
                .await;
            assert!(res.is_err());
            let code = ErrorCode::from(res.unwrap_err()).code();
            let undrop_table_already_exists = ErrorCode::UndropTableAlreadyExists("").code();
            assert_eq!(undrop_table_already_exists, code);
        }

        info!("--- rename table");
        {
            // first create drop table2
            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: new_tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let _res = mt.create_table(req.clone()).await?;
            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count += 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: tbl_name_ident.to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: new_tbl_name_ident.to_string(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            // then drop drop table2
            let drop_plan = DropTableReq {
                if_exists: false,
                name_ident: new_tbl_name_ident.clone(),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            mt.drop_table(drop_plan.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            expected_tb_count -= 1;
            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: tbl_name_ident.to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: new_tbl_name_ident.to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // then rename table to table2
            let rename_dbtb_to_dbtb1 = |if_exists| RenameTableReq {
                if_exists,
                name_ident: tbl_name_ident.clone(),
                new_db_name: db_name.to_string(),
                new_table_name: new_tbl_name.to_string(),
            };

            let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            let _got = mt.rename_table(rename_dbtb_to_dbtb1(false)).await;
            let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            // check table count
            info!("--- check table count of tenant1");
            let tb_count = mt.count_tables(Self::req_count_table(tenant)).await?;
            assert_eq!(expected_tb_count, tb_count.count);

            let res = mt
                .get_table_history(ListTableReq::new(tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: tbl_name_ident.to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: new_tbl_name_ident.to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_table_by_id<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident: ident.clone(),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- get_table_by_id ");
        {
            info!("--- get_table_by_id ");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let (table_id, table_meta) = mt.get_table_by_id(table.ident.table_id).await?;

                assert_eq!(table_meta.options.get("opt‐1"), Some(&"val-1".into()));
                assert_eq!(table_id.table_id, table.ident.table_id);
            }

            info!("--- get_table_by_id with not exists table_id");
            {
                let got = mt.get_table_by_id(1024).await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UnknownTableId("").code(), err.code());
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn table_list<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";

        info!("--- list table on unknown db");
        {
            let res = mt.list_tables(ListTableReq::new(tenant, db_name)).await;
            debug!("list table on unknown db res: {:?}", res);
            assert!(res.is_err());

            let code = ErrorCode::from(res.unwrap_err()).code();
            assert_eq!(ErrorCode::UnknownDatabase("").code(), code);
        }

        info!("--- prepare db");
        {
            let res = self.create_database(mt, tenant, db_name, "eng1").await?;
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create 2 tables: tb1 tb2");
        {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};

            let mut req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: "tb1".to_string(),
                },
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
            };

            let tb_ids = {
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");

                let tb_id1 = res.table_id;

                req.name_ident.table_name = "tb2".to_string();
                let old_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id > tb_id1, "table id > tb_id1: {}", tb_id1);
                let tb_id2 = res.table_id;

                vec![tb_id1, tb_id2]
            };

            info!("--- get_tables");
            {
                let res = mt.list_tables(ListTableReq::new(tenant, db_name)).await?;
                assert_eq!(tb_ids.len(), res.len());
                assert_eq!(tb_ids[0], res[0].ident.table_id);
                assert_eq!(tb_ids[1], res[1].ident.table_id);
            }
        }

        Ok(())
    }

    // pub async fn share_create_get_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
    //     let tenant1 = "tenant1";
    //     let share_name1 = "share1";
    //     let share_name2 = "share2";
    //     info!("--- create {}", share_name1);
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "first share id is 1");
    //     }
    //
    //     info!("--- get share1");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name1)).await;
    //         debug!("get present share res: {:?}", res);
    //         let res = res?;
    //         assert_eq!(1, res.id, "db1 id is 1");
    //         assert_eq!(
    //             share_name1.to_string(),
    //             res.name,
    //             "share1.db is {}",
    //             share_name1
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=false");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::ShareAlreadyExists("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=true");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create database res: {:?}", res);
    //
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "share1 id is 1");
    //     }
    //
    //     info!("--- create share2");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(2, res.share_id, "second share id is 2 ");
    //     }
    //
    //     info!("--- get share2");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await?;
    //         assert_eq!(2, res.id, "share2 id is 2");
    //         assert_eq!(
    //             share_name2.to_string(),
    //             res.name,
    //             "share2.name is {}",
    //             share_name2
    //         );
    //     }
    //
    //     info!("--- get absent share");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, "absent")).await;
    //         debug!("=== get absent share res: {:?}", res);
    //         assert!(res.is_err());
    //         let err = res.unwrap_err();
    //         let err_code = ErrorCode::from(err);
    //
    //         assert_eq!(ErrorCode::unknown_share_code(), err_code.code());
    //         assert!(err_code.message().contains("absent"));
    //     }
    //
    //     info!("--- drop share2");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     info!("--- get share2 should not found");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await;
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::UnknownShare("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- drop share2 with if_exists=true returns no error");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     Ok(())
    // }
    //
}

/// Supporting utils
impl SchemaApiTestSuite {
    fn req_get_db(tenant: impl ToString, db_name: impl ToString) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.to_string(),
            },
        }
    }

    fn req_count_table(tenant: impl ToString) -> CountTablesReq {
        CountTablesReq {
            tenant: tenant.to_string(),
        }
    }

    async fn create_database<MT: SchemaApi>(
        &self,
        mt: &MT,
        tenant: &str,
        db_name: &str,
        engine: &str,
    ) -> anyhow::Result<CreateDatabaseReply> {
        info!("--- create database {}", db_name);

        let req = CreateDatabaseReq {
            if_not_exists: false,
            name_ident: DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.to_string(),
            },
            meta: DatabaseMeta {
                engine: engine.to_string(),
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        info!("create database res: {:?}", res);
        Ok(res)
    }
}

// Test write and read meta on different nodes
// This is meant for testing distributed SchemaApi impl, to ensure a read-after-write consistency.
impl SchemaApiTestSuite {
    /// Create db one node, get db on another
    pub async fn database_get_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 on node_a");
        let tenant = "tenant1";
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: "db1".to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- get db1 on node_b");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant, "db1"))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.db_name, "db1.db is db1");
        }

        info!("--- get nonexistent-db on node_b, expect correct error");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant, "nonexistent"))
                .await;
            debug!("get present database res: {:?}", res);
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert_eq!("Unknown database 'nonexistent'", err.message());
            assert_eq!(
                "Code: 1003, displayText = Unknown database 'nonexistent'.",
                err.to_string()
            );
        }

        Ok(())
    }

    /// Create dbs on node_a, list dbs on node_b
    pub async fn list_database_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and db3 on node_a");
        let tenant = "tenant1";

        let mut db_ids = vec![];
        {
            let dbs = vec!["db1", "db3"];
            for db_name in dbs {
                let req = CreateDatabaseReq {
                    if_not_exists: false,
                    name_ident: DatabaseNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                    },
                    meta: DatabaseMeta {
                        engine: "github".to_string(),
                        ..Default::default()
                    },
                };
                let res = node_a.create_database(req).await?;
                db_ids.push(res.db_id);
            }
        }

        info!("--- list databases from node_b");
        {
            let res = node_b
                .list_databases(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await;
            debug!("get database list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "database list len is 2");
            assert_eq!(db_ids[0], res[0].ident.db_id, "db1 id");
            assert_eq!("db1", res[0].name_ident.db_name, "db1.name is db1");
            assert_eq!(db_ids[1], res[1].ident.db_id, "db3 id");
            assert_eq!("db3", res[1].name_ident.db_name, "db3.name is db3");
        }

        Ok(())
    }

    /// Create table on node_a, list table on node_b
    pub async fn list_table_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and tb1, tb2 on node_a");
        let tenant = "tenant1";
        let db_name = "db1";

        let mut tb_ids = vec![];

        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let tables = vec!["tb1", "tb2"];
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::btreemap! {"opt-1".into() => "val-1".into()};
            for tb in tables {
                let req = CreateTableReq {
                    if_not_exists: false,
                    name_ident: TableNameIdent {
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                        table_name: tb.to_string(),
                    },
                    table_meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_string(),
                        options: options.clone(),
                        ..Default::default()
                    },
                };
                let old_db = node_a
                    .get_database(Self::req_get_db(tenant, db_name))
                    .await?;
                let res = node_a.create_table(req).await?;
                let cur_db = node_a
                    .get_database(Self::req_get_db(tenant, db_name))
                    .await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                tb_ids.push(res.table_id);
            }
        }

        info!("--- list tables from node_b");
        {
            let res = node_b.list_tables(ListTableReq::new(tenant, db_name)).await;
            debug!("get table list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "table list len is 2");
            assert_eq!(tb_ids[0], res[0].ident.table_id, "tb1 id");
            assert_eq!("tb1", res[0].name, "tb1.name is tb1");
            assert_eq!(tb_ids[1], res[1].ident.table_id, "tb2 id");
            assert_eq!("tb2", res[1].name, "tb2.name is tb2");
        }

        Ok(())
    }

    /// Create table on node_a, get table on node_b
    pub async fn table_get_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create table tb1 on node_a");
        let tenant = "tenant1";
        let db_name = "db1";
        let tb_id = {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: "tb1".to_string(),
                },
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
            };

            let old_db = node_a
                .get_database(Self::req_get_db(tenant, db_name))
                .await?;
            let res = node_a.create_table(req).await?;
            let cur_db = node_a
                .get_database(Self::req_get_db(tenant, db_name))
                .await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            res.table_id
        };

        info!("--- get tb1 on node_b");
        {
            let res = node_b
                .get_table(GetTableReq::new(tenant, "db1", "tb1"))
                .await;
            debug!("get present table res: {:?}", res);
            let res = res?;
            assert_eq!(tb_id, res.ident.table_id, "tb1 id is 1");
            assert_eq!("tb1", res.name, "tb1.name is tb1");
        }

        info!("--- get nonexistent-table on node_b, expect correct error");
        {
            let res = node_b
                .get_table(GetTableReq::new(tenant, "db1", "nonexistent"))
                .await;
            debug!("get present table res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownTable("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }
}
