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

use common_datavalues::chrono::Utc;
use common_exception::ErrorCode;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::share::AddShareAccountReq;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::GrantShareObjectReq;
use common_meta_app::share::RemoveShareAccountReq;
use common_meta_app::share::RevokeShareObjectReq;
use common_meta_app::share::ShareAccountNameIdent;
use common_meta_app::share::ShareGrantObject;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use common_meta_app::share::ShareNameIdent;
use common_meta_app::share::ShowShareReq;
use common_tracing::tracing;
use enumflags2::BitFlags;

use crate::get_share_account_meta_or_err;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::ApiBuilder;
use crate::AsKVApi;
use crate::SchemaApi;
use crate::ShareApi;

/// Test suite of `ShareApi`.
///
/// It is not used by this crate, but is used by other crate that impl `ShareApi`,
/// to ensure an impl works as expected,
/// such as `common/meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct ShareApiTestSuite {}

impl ShareApiTestSuite {
    /// Test ShareApi on a single node
    pub async fn test_single_node_share<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: ApiBuilder<MT>,
        MT: ShareApi + AsKVApi + SchemaApi,
    {
        let suite = ShareApiTestSuite {};

        suite.share_create_show_drop(&b.build().await).await?;
        suite.share_add_remove_account(&b.build().await).await?;
        suite.share_grant_revoke_object(&b.build().await).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_create_show_drop<MT: ShareApi + AsKVApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_id: u64;

        tracing::info!("--- show share when there are no share");
        {
            let req = ShowShareReq {
                share_name: share_name.clone(),
            };

            let res = mt.show_share(req).await;
            tracing::info!("show share res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- create share1");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(share_name, share_name_ret)
        }

        tracing::info!("--- create share1 again with if_not_exists=false");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::ShareAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- create share1 again with if_not_exists=true");
        {
            let req = CreateShareReq {
                if_not_exists: true,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first share id is 1");
        }

        tracing::info!("--- show share in current database");
        {
            let req = ShowShareReq {
                share_name: share_name.clone(),
            };

            let res = mt.show_share(req).await;
            tracing::info!("show share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "share id should be 1");
        }

        tracing::info!("--- drop share1 with if_exists=true");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            let ret = get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await;
            let err = ret.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareId("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- drop share1 again with if_exists=false");
        {
            let req = DropShareReq {
                if_exists: false,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_add_remove_account<MT: ShareApi + AsKVApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let account = "account1";
        let account2 = "account2";
        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_id: u64;
        let share_on = Utc::now();
        let create_on = Utc::now();

        tracing::info!("--- prepare share1");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;
        }

        tracing::info!("--- add account account1");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account.to_string(),
            };

            // get share meta and check account has been added
            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(res.share_id, share_id);

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account.to_string()));

            // get and check share account meta
            let share_account_name = ShareAccountNameIdent {
                account: account.to_string(),
                share_id,
            };
            let (_share_account_meta_seq, share_account_meta) =
                get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await?;
            assert_eq!(share_account_meta.share_id, share_id);
            assert_eq!(share_account_meta.account, account.to_string());
            assert_eq!(share_account_meta.share_on, share_on);
        }

        // test show share api
        tracing::info!("--- show share check account information");
        {
            let req = ShowShareReq {
                share_name: share_name.clone(),
            };

            let res = mt.show_share(req).await;
            tracing::info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let res = res.unwrap();

            let meta = res.share_meta;
            assert!(meta.has_account(&account.to_string()));

            let account_meta = res.share_account_meta.get(0);
            assert!(account_meta.is_some());
            let share_account_meta = account_meta.unwrap();
            assert_eq!(share_account_meta.share_id, share_id);
            assert_eq!(share_account_meta.account, account.to_string());
            assert_eq!(share_account_meta.share_on, share_on);
        }

        tracing::info!("--- add account account1 again");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account.to_string(),
            };

            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::ShareAccountAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- add account account2");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account2.to_string(),
            };

            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(res.share_id, share_id);

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account2.to_string()));
        }

        tracing::info!("--- show share again");
        {
            let req = ShowShareReq {
                share_name: share_name.clone(),
            };

            let res = mt.show_share(req).await;
            tracing::info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let res = res.unwrap();

            let meta = res.share_meta;
            assert!(meta.has_account(&account.to_string()));
            assert!(meta.has_account(&account2.to_string()));

            assert_eq!(
                res.share_account_meta.len(),
                2,
                "there should be 2 share accounts"
            );
        }

        tracing::info!("--- remove account account2");
        {
            let req = RemoveShareAccountReq {
                share_id,
                account: account2.to_string(),
            };

            let res = mt.remove_share_account(req).await;
            tracing::info!("remove share account res: {:?}", res);
            assert!(res.is_ok());

            // check account2 has been removed from share_meta
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(!share_meta.has_account(&account2.to_string()));

            // check share account meta has been removed
            let share_account_name = ShareAccountNameIdent {
                account: account2.to_string(),
                share_id,
            };
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareAccount("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- drop share1 with if_exists=true");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            // check share account meta has been removed
            let share_account_name = ShareAccountNameIdent {
                account: account.to_string(),
                share_id,
            };
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareAccount("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_grant_revoke_object<MT: ShareApi + AsKVApi + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let db_name = "db1";
        let tbl_name = "table1";
        let db2_name = "db2";
        let tbl2_name = "table2";

        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_id: u64;
        let db_id: u64;
        let table_id: u64;

        tracing::info!("--- create share1,db1,table1");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(share_name, share_name_ret);

            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            tracing::info!("create database res: {:?}", res);
            db_id = res.db_id;

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
            };

            let res = mt.create_table(req.clone()).await?;
            tracing::info!("create table res: {:?}", res);
            table_id = res.table_id;

            let plan = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db2_name.to_string(),
                },
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            tracing::info!("create database res: {:?}", res);

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db2_name.to_string(),
                    table_name: tbl2_name.to_string(),
                },
                table_meta: TableMeta::default(),
            };

            let res = mt.create_table(req.clone()).await?;
            tracing::info!("create table res: {:?}", res);
        }

        tracing::info!("--- grant unknown db2,table2");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database("unknown_db".to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );

            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(
                    db_name.to_string(),
                    "unknown_table".to_string(),
                ),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownTable("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- grant unknown share2");
        {
            let req = GrantShareObjectReq {
                share_name: ShareNameIdent {
                    tenant: tenant.to_string(),
                    share_name: "share2".to_string(),
                },
                object: ShareGrantObjectName::Database("db2".to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- grant table2 on a unbound database share");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db2_name.to_string(), tbl2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::WrongShareObject("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- grant db object and table object");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await?;
            tracing::info!("grant object res: {:?}", res);

            let tbl_ob_name =
                ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string());
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: tbl_ob_name.clone(),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await?;
            tracing::info!("grant object res: {:?}", res);

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

        tracing::info!("--- grant db2, table2 on another bounded database share");
        {
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Database(db2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::WrongShareObject("").code(),
                ErrorCode::from(err).code()
            );

            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db2_name.to_string(), tbl2_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await;
            tracing::info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::WrongShareObject("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- revoke share of table");
        {
            let req = RevokeShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
                update_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.revoke_object(req).await?;
            tracing::info!("revoke object res: {:?}", res);

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

        tracing::info!("--- grant share of table again, and revoke the database");
        {
            // first grant share table again
            let req = GrantShareObjectReq {
                share_name: share_name.clone(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
                grant_on: create_on,
                privilege: ShareGrantObjectPrivilege::Usage,
            };

            let res = mt.grant_object(req).await?;
            tracing::info!("grant object res: {:?}", res);

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

            let res = mt.revoke_object(req).await?;
            tracing::info!("revoke object res: {:?}", res);

            // assert share_meta.database is none, and share_meta.entries is empty
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.database.is_none());
            assert!(share_meta.entries.is_empty());
        }

        Ok(())
    }
}
