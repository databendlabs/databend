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
use common_meta_app::share::*;
use common_meta_types::MetaError;
use enumflags2::BitFlags;
use tracing::info;

use crate::get_kv_data;
use crate::get_object_shared_by_share_ids;
use crate::get_share_account_meta_or_err;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::get_share_or_err;
use crate::is_all_db_data_removed;
use crate::ApiBuilder;
use crate::AsKVApi;
use crate::KVApi;
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
    kv_api: &(impl KVApi + ?Sized),
    entry: &ShareGrantEntry,
) -> Result<bool, MetaError> {
    if let Ok((_seq, _share_ids)) = get_object_shared_by_share_ids(kv_api, &entry.object).await {
        return Ok(false);
    }
    Ok(true)
}

// Return true if all the share data has been removed.
async fn is_all_share_data_removed(
    kv_api: &(impl KVApi + ?Sized),
    share_name: &ShareNameIdent,
    share_id: u64,
    share_meta: &ShareMeta,
) -> Result<bool, MetaError> {
    let res = get_share_or_err(kv_api, share_name, "").await;
    if res.is_ok() {
        return Ok(false);
    }

    let res = get_share_id_to_name_or_err(kv_api, share_id, "").await;
    if res.is_ok() {
        return Ok(false);
    }

    for account in share_meta.get_accounts() {
        let share_account_key = ShareAccountNameIdent {
            account: account.clone(),
            share_id,
        };
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

    for db_id in &share_meta.share_from_db_ids {
        if !is_all_db_data_removed(kv_api, *db_id).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

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
        suite.get_share_grant_objects(&b.build().await).await?;
        suite
            .get_grant_privileges_of_object(&b.build().await)
            .await?;

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

        info!("--- show share when there are no share");
        {
            let req = ShowSharesReq {
                tenant: tenant.to_string(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert!(resp.inbound_accounts.is_empty());
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
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(share_name, share_name_ret)
        }

        info!("--- show share again");
        {
            let req = ShowSharesReq {
                tenant: tenant.to_string(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert!(resp.inbound_accounts.is_empty());
            assert_eq!(resp.outbound_accounts.len(), 1);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_add_remove_account<MT: ShareApi + AsKVApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let tenant2 = "tenant2";
        let share1 = "share1";
        let share2 = "share2";
        let account = "account1";
        let account2 = "account2";
        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_name2 = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share2.to_string(),
        };
        let share_name3 = ShareNameIdent {
            tenant: tenant2.to_string(),
            share_name: share2.to_string(),
        };
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
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );

            let req = RemoveShareAccountsReq {
                share_name: share_name.clone(),
                if_exists: false,
                accounts: vec![account2.to_string()],
            };

            let res = mt.remove_share_tenants(req).await;
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
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

            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name3.clone(),
                comment: Some(comment3.to_string()),
                create_on,
            };

            let res = mt.create_share(req).await;
            info!("add share account res: {:?}", res);
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
            let res = mt.add_share_tenants(req).await;
            info!("add share account res: {:?}", res);
            assert!(res.is_ok());

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
                accounts: vec![tenant.to_string()],
            };

            // get share meta and check account has been added
            let res = mt.add_share_tenants(req).await;
            info!("add share account res: {:?}", res);
            assert!(res.is_ok());
        }

        // test show share api
        info!("--- show share check account information");
        {
            let req = ShowSharesReq {
                tenant: tenant.to_string(),
            };

            let res = mt.show_shares(req).await;
            info!("show share res: {:?}", res);
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert_eq!(resp.inbound_accounts.len(), 1);
            assert_eq!(resp.inbound_accounts[0].share_name, share_name3.clone());
            assert_eq!(resp.inbound_accounts[0].create_on, share_on.clone());
            assert_eq!(resp.inbound_accounts[0].comment, Some(comment3.to_string()));

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
                ErrorCode::ShareAccountsAlreadyExists("").code(),
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

            let res = mt.add_share_tenants(req).await;
            info!("add share account res: {:?}", res);
            assert!(res.is_ok());

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

            let res = mt.remove_share_tenants(req).await;
            info!("remove share account res: {:?}", res);
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
                ErrorCode::UnknownShareAccounts("").code(),
                ErrorCode::from(err).code()
            );
        }

        info!("--- drop share1 with if_exists=true");
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
                ErrorCode::UnknownShareAccounts("").code(),
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

        info!("--- create share1,db1,table1");
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
            info!("create database res: {:?}", res);
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
            info!("create table res: {:?}", res);
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
            info!("create database res: {:?}", res);

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

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownTable("").code(),
                ErrorCode::from(err).code()
            );
        }

        info!("--- grant unknown share2");
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

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
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
            assert_eq!(
                ErrorCode::WrongShareObject("").code(),
                ErrorCode::from(err).code()
            );
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

            let res = mt.grant_share_object(req).await;
            info!("grant object res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::WrongShareObject("").code(),
                ErrorCode::from(err).code()
            );
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

            // assert share_meta.database is none, and share_meta.entries is empty
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.database.is_none());
            assert!(share_meta.entries.is_empty());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_share_grant_objects<MT: ShareApi + AsKVApi + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let db_name = "db1";
        let tbl_name = "table1";

        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };

        info!("--- get unknown share");
        {
            let req = GetShareGrantObjectReq {
                share_name: share_name.clone(),
            };

            let res = mt.get_share_grant_objects(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
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
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_grant_privileges_of_object<MT: ShareApi + AsKVApi + SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = "tenant1";
        let share1 = "share1";
        let share2 = "share2";
        let db_name = "db1";
        let tbl_name = "table1";
        let share_id;

        let share_name1 = ShareNameIdent {
            tenant: tenant1.to_string(),
            share_name: share1.to_string(),
        };
        let share_name2 = ShareNameIdent {
            tenant: tenant1.to_string(),
            share_name: share2.to_string(),
        };

        info!("--- get unknown object");
        {
            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant1.to_string(),
                object: ShareGrantObjectName::Database("db".to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant1.to_string(),
                object: ShareGrantObjectName::Table("db".to_string(), "table".to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            info!("get_share_grant_objects res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownDatabase("").code(),
                ErrorCode::from(err).code()
            );
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
                if_not_exists: false,
                name_ident: DatabaseNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: db_name.to_string(),
                },
                meta: DatabaseMeta::default(),
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            let req = CreateTableReq {
                if_not_exists: false,
                name_ident: TableNameIdent {
                    tenant: tenant1.to_string(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: TableMeta::default(),
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
                tenant: tenant1.to_string(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 2);
            assert_eq!(&res.privileges[0].share_name, share1);
            assert_eq!(res.privileges[0].grant_on, grant_on);

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant1.to_string(),
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
            let tenant2 = "tenant2";
            let db2 = "db2";

            let db_name2 = DatabaseNameIdent {
                tenant: tenant2.to_string(),
                db_name: db2.to_string(),
            };

            // first grant account tenant2
            let req = AddShareAccountsReq {
                share_name: share_name1.clone(),
                share_on: Utc::now(),
                if_exists: false,
                accounts: vec![tenant2.to_string()],
            };
            let res = mt.add_share_tenants(req).await;
            assert!(res.is_ok());

            // tenant2 create a database from share1
            let req = CreateDatabaseReq {
                if_not_exists: false,
                name_ident: db_name2.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name1.clone()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());
            // save the db id
            let db_id = res.unwrap().db_id;

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

            let res = is_all_db_data_removed(mt.as_kv_api(), db_id).await?;
            assert!(res);

            // get_grant_privileges_of_object of db and table again
            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant1.to_string(),
                object: ShareGrantObjectName::Database(db_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 1);

            let req = GetObjectGrantPrivilegesReq {
                tenant: tenant1.to_string(),
                object: ShareGrantObjectName::Table(db_name.to_string(), tbl_name.to_string()),
            };

            let res = mt.get_grant_privileges_of_object(req).await;
            assert!(res.is_ok());
            let res = res.unwrap();
            assert_eq!(res.privileges.len(), 0);
        }

        Ok(())
    }
}
