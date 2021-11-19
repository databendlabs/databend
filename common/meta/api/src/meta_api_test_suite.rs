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

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
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
        tracing::info!("--- create db1");
        {
            let plan = CreateDatabasePlan {
                if_not_exists: false,
                db: "db1".to_string(),
                options: Default::default(),
            };

            let res = mt.create_database(plan.clone()).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create db1 again with if_not_exists=false");
        {
            let plan = CreateDatabasePlan {
                if_not_exists: false,
                db: "db1".to_string(),
                options: Default::default(),
            };

            let res = mt.create_database(plan.clone()).await;
            tracing::info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::DatabaseAlreadyExists("").code(), err.code());
        }

        tracing::info!("--- create db1 again with if_not_exists=true");
        {
            let plan = CreateDatabasePlan {
                if_not_exists: false,
                db: "db1".to_string(),
                options: Default::default(),
            };

            let res = mt.create_database(plan.clone()).await;
            tracing::info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::DatabaseAlreadyExists("").code(), err.code());
        }

        tracing::info!("--- get db1");
        {
            let res = mt.get_database("db1").await;
            tracing::debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id, "db1 id is 1");
            assert_eq!("db1".to_string(), res.db, "db1.db is db1");
        }

        tracing::info!("--- create db2");
        {
            let plan = CreateDatabasePlan {
                if_not_exists: false,
                db: "db2".to_string(),
                options: Default::default(),
            };

            let res = mt.create_database(plan.clone()).await;
            tracing::info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                4, res.database_id,
                "second database id is 4: seq increment but no used"
            );
        }

        tracing::info!("--- get db2");
        {
            let res = mt.get_database("db2").await?;
            assert_eq!("db2".to_string(), res.db, "db1.db is db1");
        }

        tracing::info!("--- get absent db");
        {
            let res = mt.get_database("absent").await;
            tracing::debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let res = res.unwrap_err();
            assert_eq!(3, res.code());
            assert_eq!("absent".to_string(), res.message());
        }

        tracing::info!("--- drop db2");
        {
            mt.drop_database(DropDatabasePlan {
                if_exists: false,
                db: "db2".to_string(),
            })
            .await?;
        }

        tracing::info!("--- get db2 should not found");
        {
            let res = mt.get_database("db2").await;
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UnknownDatabase("").code(), err.code());
        }

        tracing::info!("--- drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabasePlan {
                if_exists: true,
                db: "db2".to_string(),
            })
            .await?;
        }

        Ok(())
    }

    pub async fn database_list<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        tracing::info!("--- prepare db1 and db2");
        {
            let res = self.create_database(mt, "db1").await?;
            assert_eq!(1, res.database_id);

            let res = self.create_database(mt, "db2").await?;
            assert_eq!(2, res.database_id);
        }

        tracing::info!("--- get_databases");
        {
            let dbs = mt.get_databases().await?;
            let want: Vec<u64> = vec![1, 2];
            let got = dbs.iter().map(|x| x.database_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }
    pub async fn table_create_get_drop<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let db_name = "db1";
        let tbl_name = "tb2";

        tracing::info!("--- prepare db");
        {
            let plan = CreateDatabasePlan {
                if_not_exists: false,
                db: db_name.to_string(),
                options: Default::default(),
            };

            let res = mt.create_database(plan.clone()).await?;
            tracing::info!("create database res: {:?}", res);

            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create and get table");
        {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )]));

            let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

            let mut plan = CreateTablePlan {
                if_not_exists: false,
                db: db_name.to_string(),
                table: tbl_name.to_string(),
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                },
            };

            {
                let res = mt.create_table(plan.clone()).await?;
                assert_eq!(1, res.table_id, "table id is 1");

                let got = mt.get_table(db_name, tbl_name).await?;

                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_owned(),
                        options: options.clone(),
                    },
                };
                assert_eq!(want, got.as_ref().clone(), "get created table");
            }

            tracing::info!("--- create table again with if_not_exists = true");
            {
                plan.if_not_exists = true;
                let res = mt.create_table(plan.clone()).await?;
                assert_eq!(1, res.table_id, "new table id");

                let got = mt.get_table(db_name, tbl_name).await?;
                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_owned(),
                        options: options.clone(),
                    },
                };
                assert_eq!(want, got.as_ref().clone(), "get created table");
            }

            tracing::info!("--- create table again with if_not_exists = false");
            {
                plan.if_not_exists = false;

                let res = mt.create_table(plan.clone()).await;
                tracing::info!("create table res: {:?}", res);

                let status = res.err().unwrap();
                assert_eq!(
                    format!("Code: 4003, displayText = table exists: {}.", tbl_name),
                    status.to_string()
                );

                // get_table returns the old table

                let got = mt.get_table("db1", "tb2").await.unwrap();
                let want = TableInfo {
                    ident: TableIdent::new(1, 1),
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_owned(),
                        options: options.clone(),
                    },
                };
                assert_eq!(want, got.as_ref().clone(), "get old table");
            }

            tracing::info!("--- upsert table options");
            {
                tracing::info!("--- upsert table options with key1=val1");
                {
                    let table = mt.get_table("db1", "tb2").await.unwrap();

                    mt.upsert_table_option(
                        table.ident.table_id,
                        table.ident.version,
                        "key1".into(),
                        "val1".into(),
                    )
                    .await?;

                    let table = mt.get_table("db1", "tb2").await.unwrap();
                    assert_eq!(table.options().get("key1"), Some(&"val1".into()));
                }

                tracing::info!("--- upsert table options with key1=val1");
                {
                    let table = mt.get_table("db1", "tb2").await.unwrap();

                    let got = mt
                        .upsert_table_option(
                            table.ident.table_id,
                            table.ident.version - 1,
                            "key1".into(),
                            "val2".into(),
                        )
                        .await;

                    let got = got.unwrap_err();
                    assert_eq!(ErrorCode::TableVersionMissMatch("").code(), got.code());

                    // table is not affected.
                    let table = mt.get_table("db1", "tb2").await.unwrap();
                    assert_eq!(table.options().get("key1"), Some(&"val1".into()));
                }
            }

            tracing::info!("--- drop table with if_exists = false");
            {
                let plan = DropTablePlan {
                    if_exists: false,
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
                };
                mt.drop_table(plan.clone()).await?;

                tracing::info!("--- get table after drop");
                {
                    let res = mt.get_table(db_name, tbl_name).await;
                    let status = res.err().unwrap();
                    assert_eq!(
                        format!("Code: 25, displayText = Unknown table: '{:}'.", tbl_name),
                        status.to_string(),
                        "get dropped table {}",
                        tbl_name
                    );
                }
            }

            tracing::info!("--- drop table with if_exists = false again, error");
            {
                let plan = DropTablePlan {
                    if_exists: false,
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
                };
                let res = mt.drop_table(plan.clone()).await;
                let err = res.unwrap_err();
                assert_eq!(
                    ErrorCode::UnknownTable("").code(),
                    err.code(),
                    "drop table {} with if_exists=false again",
                    tbl_name
                );
            }

            tracing::info!("--- drop table with if_exists = true again, ok");
            {
                let plan = DropTablePlan {
                    if_exists: true,
                    db: db_name.to_string(),
                    table: tbl_name.to_string(),
                };
                mt.drop_table(plan.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn table_list<MT: MetaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let db_name = "db1";

        tracing::info!("--- prepare db");
        {
            let res = self.create_database(mt, db_name).await?;
            assert_eq!(1, res.database_id, "first database id is 1");
        }

        tracing::info!("--- create 2 tables: tb1 tb2");
        {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )]));

            let options = maplit::hashmap! {"opt‐1".into() => "val-1".into()};

            let mut plan = CreateTablePlan {
                if_not_exists: false,
                db: db_name.to_string(),
                table: "tb1".to_string(),
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
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
                let res = mt.get_tables(db_name).await?;
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
        db_name: &str,
    ) -> anyhow::Result<CreateDatabaseReply> {
        tracing::info!("--- create database {}", db_name);

        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            options: Default::default(),
        };

        let res = mt.create_database(plan.clone()).await?;
        tracing::info!("create database res: {:?}", res);
        Ok(res)
    }
}
