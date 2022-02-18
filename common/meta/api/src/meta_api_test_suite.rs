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

use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::MetaApi;

/// Test suite of `MetaApi`.
///
/// It is not used by this crate, but is used by other crate that impl `MetaApi`,
/// to ensure an impl works as expected,
/// such as `common/meta/embedded` and `metasrv`.
pub struct MetaApiTestSuite {}

impl MetaApiTestSuite {
    pub async fn database_create_get_drop<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        tracing::info!("--- create db1");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create db1 again with if_not_exists=false");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::DatabaseAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- create db1 again with if_not_exists=true");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::DatabaseAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- get db1");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db1")).await;
            tracing::debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id, "db1 id is 1");
            assert_eq!("db1".to_string(), res.db, "db1.db is db1");
        }

        tracing::info!("--- create db2");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: "db2".to_string(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                4, res.database_id,
                "second database id is 4: seq increment but no used"
            );
        }

        tracing::info!("--- get db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db2")).await?;
            assert_eq!("db2".to_string(), res.db, "db1.db is db1");
        }

        tracing::info!("--- get absent db");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "absent")).await;
            tracing::debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err_code = ErrorCode::from(err);

            assert_eq!(1003, err_code.code());
            assert!(err_code.message().contains("absent"));
        }

        tracing::info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                tenant: tenant.to_string(),
                db: "db2".to_string(),
            })
            .await?;
        }

        tracing::info!("--- get db2 should not found");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant, "db2")).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                tenant: tenant.to_string(),
                db: "db2".to_string(),
            })
            .await?;
        }

        Ok(())
    }

    pub async fn database_create_get_drop_in_diff_tenant<MT: MetaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = "tenant1";
        let tenant2 = "tenant2";
        tracing::info!("--- tenant1 create db1");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant1.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- tenant1 create db2");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant1.to_string(),
                db: "db2".to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(2, res.database_id, "second database id is 2");
        }

        tracing::info!("--- tenant2 create db1");
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant2.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(3, res.database_id, "third database id is 3");
        }

        tracing::info!("--- tenant1 get db1");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant1, "db1")).await;
            tracing::debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id, "db1 id is 1");
            assert_eq!("db1".to_string(), res.db, "db1.db is db1");
        }

        tracing::info!("--- tenant1 get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1, "absent"))
                .await;
            tracing::debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);

            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert!(err.message().contains("absent"));
        }

        tracing::info!("--- tenant2 get tenant1's db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant2, "db2")).await;
            tracing::debug!("=== get other tenant's database res: {:?}", res);
            assert!(res.is_err());
            let res = res.unwrap_err();
            let err = ErrorCode::from(res);

            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert_eq!("db2".to_string(), err.message());
        }

        tracing::info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                tenant: tenant1.to_string(),
                db: "db2".to_string(),
            })
            .await?;
        }

        tracing::info!("--- tenant1 get db2 should not found");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant1, "db2")).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- tenant1 drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                tenant: tenant1.to_string(),
                db: "db2".to_string(),
            })
            .await?;
        }

        Ok(())
    }

    pub async fn database_list<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        tracing::info!("--- prepare db1 and db2");
        let tenant = "tenant1";
        {
            let res = self.create_database(mt, tenant, "db1").await?;
            assert_eq!(1, res.database_id);

            let res = self.create_database(mt, tenant, "db2").await?;
            assert_eq!(2, res.database_id);
        }

        tracing::info!("--- get_databases");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await?;
            let want: Vec<u64> = vec![1, 2];
            let got = dbs.iter().map(|x| x.database_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }

    pub async fn database_list_in_diff_tenant<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        tracing::info!("--- prepare db1 and db2");
        let tenant1 = "tenant1";
        let tenant2 = "tenant2";
        {
            let res = self.create_database(mt, tenant1, "db1").await?;
            assert_eq!(1, res.database_id);

            let res = self.create_database(mt, tenant1, "db2").await?;
            assert_eq!(2, res.database_id);
        }

        {
            let res = self.create_database(mt, tenant2, "db3").await?;
            assert_eq!(3, res.database_id);
        }

        tracing::info!("--- get_databases by tenant1");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant1.to_string(),
                })
                .await?;
            let want: Vec<u64> = vec![1, 2];
            let got = dbs.iter().map(|x| x.database_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        tracing::info!("--- get_databases by tenant2");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant2.to_string(),
                })
                .await?;
            let want: Vec<u64> = vec![3];
            let got = dbs.iter().map(|x| x.database_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }

    pub async fn table_create_get_drop<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]))
        };

        let options = || maplit::hashmap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        let unknown_database_code = ErrorCode::UnknownDatabase("").code();

        tracing::info!("--- create or get table on unknown db");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                table: tbl_name.to_string(),
                table_meta: table_meta(created_on),
            };
            // test create table
            {
                let res = mt.create_table(req).await;
                tracing::debug!("create table on unknown db res: {:?}", res);

                assert!(res.is_err());
                let err = res.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            };
            // test get table
            {
                let got = mt.get_table((tenant, db_name, tbl_name).into()).await;
                tracing::debug!("get table on unknown db got: {:?}", got);

                assert!(got.is_err());
                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            }
        }

        tracing::info!("--- drop table on unknown db");
        {
            // casually create a drop table plan
            // should be not vunerable?
            let plan = DropTableReq {
                if_exists: false,
                tenant: tenant.into(),
                db: db_name.into(),
                table: tbl_name.into(),
            };

            let got = mt.drop_table(plan).await;
            tracing::debug!("--- drop table on unknown database got: {:?}", got);

            assert!(got.is_err());
            let code = ErrorCode::from(got.unwrap_err()).code();
            assert_eq!(unknown_database_code, code);
        }

        tracing::info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            tracing::info!("create database res: {:?}", res);

            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create and get table");
        {
            let created_on = Utc::now();

            let mut req = CreateTableReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                table: tbl_name.to_string(),
                table_meta: table_meta(created_on),
            };

            {
                let res = mt.create_table(req.clone()).await?;
                assert_eq!(1, res.table_id, "table id is 1");

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;

                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_eq!(want, got.as_ref().clone(), "get created table");
            }

            tracing::info!("--- create table again with if_not_exists = true");
            {
                req.if_not_exists = true;
                let res = mt.create_table(req.clone()).await?;
                assert_eq!(1, res.table_id, "new table id");

                let got = mt.get_table((tenant, db_name, tbl_name).into()).await?;
                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_eq!(want, got.as_ref().clone(), "get created table");
            }

            tracing::info!("--- create table again with if_not_exists = false");
            {
                req.if_not_exists = false;

                let res = mt.create_table(req).await;
                tracing::info!("create table res: {:?}", res);

                let status = res.err().unwrap();
                let err_code = ErrorCode::from(status);

                assert_eq!(
                    format!("Code: 2302, displayText = table exists: {}.", tbl_name),
                    err_code.to_string()
                );

                // get_table returns the old table

                let got = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'.'{}'", tenant, db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                };
                assert_eq!(want, got.as_ref().clone(), "get old table");
            }
        }

        tracing::info!("--- upsert table options");
        {
            tracing::info!("--- upsert table options with key1=val1");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                mt.upsert_table_option(UpsertTableOptionReq::new(&table.ident, "key1", "val1"))
                    .await?;

                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            tracing::info!("--- upsert table options with key1=val1");
            {
                let table = mt.get_table((tenant, "db1", "tb2").into()).await.unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: table.ident.table_id,
                            version: table.ident.version - 1,
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
        }
        tracing::info!("--- drop table");
        {
            tracing::info!("--- drop table with if_exists = false");
            {
                let plan = DropTableReq {
                    if_exists: false,
                    tenant: tenant.to_string(),
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
                };
                mt.drop_table(plan.clone()).await?;

                tracing::info!("--- get table after drop");
                {
                    let res = mt.get_table((tenant, db_name, tbl_name).into()).await;
                    let status = res.err().unwrap();
                    let err_code = ErrorCode::from(status);

                    assert_eq!(
                        format!("Code: 1025, displayText = Unknown table: '{:}'.", tbl_name),
                        err_code.to_string(),
                        "get dropped table {}",
                        tbl_name
                    );
                }
            }

            tracing::info!("--- drop table with if_exists = false again, error");
            {
                let plan = DropTableReq {
                    if_exists: false,
                    tenant: tenant.to_string(),
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
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

            tracing::info!("--- drop table with if_exists = true again, ok");
            {
                let plan = DropTableReq {
                    if_exists: true,
                    tenant: tenant.to_string(),
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
                };
                mt.drop_table(plan.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn table_list<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let db_name = "db1";

        tracing::info!("--- list table on unknown db");
        {
            let res = mt.list_tables(ListTableReq::new(tenant, db_name)).await;
            tracing::debug!("list table on unknown db res: {:?}", res);
            assert!(res.is_err());

            let code = ErrorCode::from(res.unwrap_err()).code();
            assert_eq!(ErrorCode::UnknownDatabase("").code(), code);
        }

        tracing::info!("--- prepare db");
        {
            let res = self.create_database(mt, tenant, db_name).await?;
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create 2 tables: tb1 tb2");
        {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

            let mut plan = CreateTableReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                table: "tb1".to_string(),
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
            };

            {
                let res = mt.create_table(plan.clone()).await?;
                assert_eq!(1, res.table_id, "table id is 1");

                plan.table = "tb2".to_string();
                let res = mt.create_table(plan.clone()).await?;
                assert_eq!(2, res.table_id, "table id is 2");
            }

            tracing::info!("--- get_tables");
            {
                let res = mt.list_tables(ListTableReq::new(tenant, db_name)).await?;
                assert_eq!(1, res[0].ident.table_id);
                assert_eq!(2, res[1].ident.table_id);
            }
        }

        Ok(())
    }
}

impl MetaApiTestSuite {
    async fn create_database<MT: MetaApi>(
        &self,
        mt: &MT,
        tenant: &str,
        db_name: &str,
    ) -> anyhow::Result<CreateDatabaseReply> {
        tracing::info!("--- create database {}", db_name);

        let req = CreateDatabaseReq {
            if_not_exists: false,
            tenant: tenant.to_string(),
            db: db_name.to_string(),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        tracing::info!("create database res: {:?}", res);
        Ok(res)
    }
}

// Test write and read meta on different nodes
// This is meant for testing distributed MetaApi impl, to ensure a read-after-write consistency.
impl MetaApiTestSuite {
    /// Create db one node, get db on another
    pub async fn database_get_diff_nodes<MT: MetaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        tracing::info!("--- create db1 on node_a");
        let tenant = "tenant1";
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: "db1".to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = node_a.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- get db1 on node_b");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant, "db1"))
                .await;
            tracing::debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id, "db1 id is 1");
            assert_eq!("db1", res.db, "db1.db is db1");
        }

        tracing::info!("--- get nonexistent-db on node_b, expect correct error");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant, "nonexistent"))
                .await;
            tracing::debug!("get present database res: {:?}", res);
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
            assert_eq!("nonexistent", err.message());
            assert_eq!("Code: 1003, displayText = nonexistent.", format!("{}", err));
        }

        Ok(())
    }

    /// Create dbs on node_a, list dbs on node_b
    pub async fn list_database_diff_nodes<MT: MetaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        tracing::info!("--- create db1 and db3 on node_a");
        let tenant = "tenant1";
        {
            let dbs = vec!["db1", "db3"];
            for db_name in dbs {
                let req = CreateDatabaseReq {
                    if_not_exists: false,
                    tenant: tenant.to_string(),
                    db: db_name.to_string(),
                    meta: DatabaseMeta {
                        engine: "github".to_string(),
                        ..Default::default()
                    },
                };
                let res = node_a.create_database(req).await;
                tracing::info!("create database res: {:?}", res);
                assert!(res.is_ok());
            }
        }

        tracing::info!("--- list databases from node_b");
        {
            let res = node_b
                .list_databases(ListDatabaseReq {
                    tenant: tenant.to_string(),
                })
                .await;
            tracing::debug!("get database list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "database list len is 2");
            assert_eq!(1, res[0].database_id, "db1 id is 1");
            assert_eq!("db1", res[0].db, "db1.name is db1");
            assert_eq!(2, res[1].database_id, "db3 id is 2");
            assert_eq!("db3", res[1].db, "db3.name is db3");
        }

        Ok(())
    }

    /// Create table on node_a, list table on node_b
    pub async fn list_table_diff_nodes<MT: MetaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        tracing::info!("--- create db1 and tb1, tb2 on node_a");
        let tenant = "tenant1";
        let db_name = "db1";
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let tables = vec!["tb1", "tb2"];
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::hashmap! {"opt-1".into() => "val-1".into()};
            for tb in tables {
                let req = CreateTableReq {
                    if_not_exists: false,
                    tenant: tenant.to_string(),
                    db: db_name.to_string(),
                    table: tb.to_string(),
                    table_meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_string(),
                        options: options.clone(),
                        ..Default::default()
                    },
                };
                let res = node_a.create_table(req).await;
                tracing::info!("create table res: {:?}", res);
                assert!(res.is_ok());
            }
        }

        tracing::info!("--- list tables from node_b");
        {
            let res = node_b.list_tables(ListTableReq::new(tenant, db_name)).await;
            tracing::debug!("get table list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "table list len is 2");
            assert_eq!(1, res[0].ident.table_id, "tb1 id is 1");
            assert_eq!("tb1", res[0].name, "tb1.name is tb1");
            assert_eq!(2, res[1].ident.table_id, "tb2 id is 2");
            assert_eq!("tb2", res[1].name, "tb2.name is tb2");
        }

        Ok(())
    }

    /// Create table on node_a, get table on node_b
    pub async fn table_get_diff_nodes<MT: MetaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        tracing::info!("--- create table tb1 on node_a");
        let tenant = "tenant1";
        let db_name = "db1";
        {
            let req = CreateDatabaseReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            tracing::info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                u64::to_data_type(),
            )]));

            let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

            let req = CreateTableReq {
                if_not_exists: false,
                tenant: tenant.to_string(),
                db: db_name.to_string(),
                table: "tb1".to_string(),
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
            };

            let res = node_a.create_table(req).await;
            tracing::info!("create table res: {:?}", res);
            assert!(res.is_ok());
        }

        tracing::info!("--- get tb1 on node_b");
        {
            let res = node_b
                .get_table(GetTableReq::new(tenant, "db1", "tb1"))
                .await;
            tracing::debug!("get present table res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.table_id, "tb1 id is 1");
            assert_eq!("tb1", res.name, "tb1.name is tb1");
        }

        tracing::info!("--- get nonexistent-table on node_b, expect correct error");
        {
            let res = node_b
                .get_table(GetTableReq::new(tenant, "db1", "nonexistent"))
                .await;
            tracing::debug!("get present table res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownTable("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }
}
