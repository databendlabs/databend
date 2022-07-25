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

use std::fmt::Display;

use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::share::AddShareAccountReply;
use common_meta_app::share::AddShareAccountReq;
use common_meta_app::share::CreateShareReply;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReply;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::GetShareGrantObjectReply;
use common_meta_app::share::GetShareGrantObjectReq;
use common_meta_app::share::GrantShareObjectReply;
use common_meta_app::share::GrantShareObjectReq;
use common_meta_app::share::RemoveShareAccountReply;
use common_meta_app::share::RemoveShareAccountReq;
use common_meta_app::share::RevokeShareObjectReply;
use common_meta_app::share::RevokeShareObjectReq;
use common_meta_app::share::ShareAccountMeta;
use common_meta_app::share::ShareAccountNameIdent;
use common_meta_app::share::ShareGrantEntry;
use common_meta_app::share::ShareGrantObject;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectSeqAndId;
use common_meta_app::share::ShareId;
use common_meta_app::share::ShareIdToName;
use common_meta_app::share::ShareMeta;
use common_meta_app::share::ShareNameIdent;
use common_meta_types::app_error::AppError;
use common_meta_types::app_error::ShareAccountAlreadyExists;
use common_meta_types::app_error::ShareAlreadyExists;
use common_meta_types::app_error::TxnRetryMaxTimes;
use common_meta_types::app_error::UnknownShare;
use common_meta_types::app_error::UnknownShareAccount;
use common_meta_types::app_error::UnknownShareId;
use common_meta_types::app_error::WrongShareObject;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_meta_types::TxnCondition;
use common_meta_types::TxnOp;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use common_tracing::tracing;

use crate::db_has_to_exist;
use crate::fetch_id;
use crate::get_db_or_err;
use crate::get_struct_value;
use crate::get_u64_value;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::table_has_to_exist;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::KVApi;
use crate::ShareApi;
use crate::ShareIdGen;
use crate::TXN_MAX_RETRY_TIMES;

/// ShareApi is implemented upon KVApi.
/// Thus every type that impl KVApi impls ShareApi.
#[async_trait::async_trait]
impl<KV: KVApi> ShareApi for KV {
    #[tracing::instrument(level = "debug", ret, err, skip_all)]
    async fn create_share(&self, req: CreateShareReq) -> MetaResult<CreateShareReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            // Get share by name to ensure absence
            let (share_id_seq, share_id) = get_u64_value(self, name_key).await?;
            tracing::debug!(share_id_seq, share_id, ?name_key, "get_share");

            if share_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateShareReply { share_id })
                } else {
                    Err(MetaError::AppError(AppError::ShareAlreadyExists(
                        ShareAlreadyExists::new(
                            &name_key.share_name,
                            format!("create share: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            // Create share by inserting these record:
            // (tenant, share_name) -> share_id
            // (share_id) -> share_meta
            // (share) -> (tenant,share_name)

            let share_id = fetch_id(self, ShareIdGen {}).await?;
            let id_key = ShareId { share_id };
            let id_to_name_key = ShareIdToName { share_id };

            tracing::debug!(share_id, name_key = debug(&name_key), "new share id");

            // Create share by transaction.
            {
                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&id_to_name_key, Eq, 0),
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(share_id)?), /* (tenant, share_name) -> share_id */
                        txn_op_put(
                            &id_key,
                            serialize_struct(&ShareMeta::new(req.create_on, req.comment.clone()))?,
                        ), /* (share_id) -> share_meta */
                        txn_op_put(&id_to_name_key, serialize_struct(name_key)?), /* __fd_share_id_to_name/<share_id> -> (tenant,share_name) */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "create_database"
                );

                if succ {
                    return Ok(CreateShareReply { share_id });
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_share", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn drop_share(&self, req: DropShareReq) -> MetaResult<DropShareReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res = get_share_or_err(self, name_key, format!("drop_share: {}", &name_key)).await;

            let (share_id_seq, share_id, share_meta_seq, share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let MetaError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {});
                        }
                    }

                    return Err(e);
                }
            };

            let res =
                get_share_id_to_name_or_err(self, share_id, format!("drop_share: {}", &name_key))
                    .await;
            let (share_name_seq, _share_name) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let MetaError::AppError(AppError::UnknownShareId(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {});
                        }
                    }

                    return Err(e);
                }
            };

            // get all accounts seq from share_meta
            let mut accounts = vec![];
            for account in share_meta.get_accounts() {
                let share_account_key = ShareAccountNameIdent {
                    account: account.clone(),
                    share_id,
                };
                let ret = get_share_account_meta_or_err(
                    self,
                    &share_account_key,
                    format!("drop_share's account: {}/{}", share_id, account),
                )
                .await;

                match ret {
                    Err(_) => {}
                    Ok((seq, _meta)) => accounts.push((share_account_key, seq)),
                }
            }

            // Delete share by these operations:
            // del (tenant, share_name)
            // del share_id
            // del (share_id) -> (tenant, share_name)
            // del all outbound of share

            let share_id_key = ShareId { share_id };
            let id_name_key = ShareIdToName { share_id };

            tracing::debug!(share_id, name_key = debug(&name_key), "drop_share");

            {
                let mut condition = vec![
                    txn_cond_seq(name_key, Eq, share_id_seq),
                    txn_cond_seq(&share_id_key, Eq, share_meta_seq),
                    txn_cond_seq(&id_name_key, Eq, share_name_seq),
                ];
                let mut if_then = vec![
                    txn_op_del(name_key),      // del (tenant, share_name)
                    txn_op_del(&share_id_key), // del share_id
                    txn_op_del(&id_name_key),  // del (share_id) -> (tenant, share_name)
                ];
                for account in accounts {
                    condition.push(txn_cond_seq(&account.0, Eq, account.1));
                    if_then.push(txn_op_del(&account.0));
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&share_id_key),
                    succ = display(succ),
                    "drop_share"
                );

                if succ {
                    return Ok(DropShareReply {});
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_share", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn add_share_account(&self, req: AddShareAccountReq) -> MetaResult<AddShareAccountReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res =
                get_share_or_err(self, name_key, format!("add_share_account: {}", &name_key)).await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            if share_meta.has_account(&req.account) {
                return Err(MetaError::AppError(AppError::ShareAccountAlreadyExists(
                    ShareAccountAlreadyExists::new(
                        req.share_name.share_name,
                        req.account,
                        "share account already exists",
                    ),
                )));
            }

            // Add share account by these operations:
            // mod share_meta add account
            // add (account, share_id) -> share_account_meta
            // return share_id
            {
                let share_account_key = ShareAccountNameIdent {
                    account: req.account.clone(),
                    share_id,
                };
                let id_key = ShareId { share_id };
                share_meta.add_account(req.account.clone());

                let share_account_meta =
                    ShareAccountMeta::new(req.account.clone(), share_id, req.share_on);

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, share_id_seq),
                        txn_cond_seq(&id_key, Eq, share_meta_seq),
                        txn_cond_seq(&share_account_key, Eq, 0),
                    ],
                    if_then: vec![
                        txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                        txn_op_put(&share_account_key, serialize_struct(&share_account_meta)?), /* (account, share_id) -> share_account_meta */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "add_share_account"
                );

                if succ {
                    return Ok(AddShareAccountReply { share_id });
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("add_share_account", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn remove_share_account(
        &self,
        req: RemoveShareAccountReq,
    ) -> MetaResult<RemoveShareAccountReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let share_id = req.share_id;
        let mut retry = 0;

        let share_account_key = ShareAccountNameIdent {
            account: req.account.clone(),
            share_id: req.share_id,
        };
        let id_key = ShareId { share_id };

        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res = get_share_meta_by_id_or_err(
                self,
                share_id,
                format!("remove_share_account: {}", share_id),
            )
            .await;

            let (share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            if !share_meta.has_account(&req.account) {
                return Err(MetaError::AppError(AppError::UnknownShareAccount(
                    UnknownShareAccount::new(req.account, share_id, "unknown share account"),
                )));
            }

            let res = get_share_account_meta_or_err(
                self,
                &share_account_key,
                format!("remove_share_account: {}", share_id),
            )
            .await;

            let (share_meta_account_seq, _share_account_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            // Remove share account by these operations:
            // mod share_meta delete account
            // del (account, share_id)
            // return share_id
            {
                share_meta.del_account(&req.account);

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(&id_key, Eq, share_meta_seq),
                        txn_cond_seq(&share_account_key, Eq, share_meta_account_seq),
                    ],
                    if_then: vec![
                        txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                        txn_op_del(&share_account_key), // del (account, share_id)
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    id = debug(&id_key),
                    succ = display(succ),
                    "remove_share_account"
                );

                if succ {
                    return Ok(RemoveShareAccountReply {});
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("remove_share_account", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn grant_object(&self, req: GrantShareObjectReq) -> MetaResult<GrantShareObjectReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res = get_share_or_err(
                self,
                share_name_key,
                format!("grant_object: {}", &share_name_key),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let seq_and_id =
                get_share_object_seq_and_id(self, &req.object, &share_name_key.tenant).await?;

            check_share_object(&share_meta.database, &seq_and_id, &req.object)?;

            // Check the object privilege has been granted
            let has_granted_privileges =
                share_meta.has_granted_privileges(&req.object, &seq_and_id, req.privilege)?;

            if has_granted_privileges {
                return Ok(GrantShareObjectReply {});
            }

            // Grant the object privilege by inserting these record:
            // add privilege and upsert (share_id) -> share_meta
            // if grant database then update db_meta.shared_on and upsert (db_id) -> db_meta

            // Grant the object privilege by transaction.
            {
                let id_key = ShareId { share_id };
                // modify the share_meta add privilege
                let object = ShareGrantObject::new(&seq_and_id);
                share_meta.grant_object_privileges(object, req.privilege, req.grant_on);

                // condition
                let mut condition: Vec<TxnCondition> = vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                ];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![
                    txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                ];
                add_grant_object_txn_if_then(share_id, seq_and_id, &mut if_then)?;

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&share_name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "grant_object"
                );

                if succ {
                    return Ok(GrantShareObjectReply {});
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("grant_object", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn revoke_object(&self, req: RevokeShareObjectReq) -> MetaResult<RevokeShareObjectReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res = get_share_or_err(
                self,
                share_name_key,
                format!("revoke_object: {}", &share_name_key),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let seq_and_id =
                get_share_object_seq_and_id(self, &req.object, &share_name_key.tenant).await?;

            check_share_object(&share_meta.database, &seq_and_id, &req.object)?;

            // Check the object privilege has not been granted.
            let has_granted_privileges =
                share_meta.has_granted_privileges(&req.object, &seq_and_id, req.privilege)?;

            if !has_granted_privileges {
                return Ok(RevokeShareObjectReply {});
            }

            // Revoke the object privilege by upserting these record:
            // revoke privilege in share_meta and upsert (share_id) -> share_meta
            // if revoke database then update db_meta.shared_on and upsert (db_id) -> db_meta

            // Revoke the object privilege by transaction.
            {
                let id_key = ShareId { share_id };
                // modify the share_meta add privilege
                let object = ShareGrantObject::new(&seq_and_id);
                let _ =
                    share_meta.revoke_object_privileges(object, req.privilege, req.update_on)?;

                // condition
                let mut condition: Vec<TxnCondition> = vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                ];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![
                    txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                ];

                if let ShareGrantObjectSeqAndId::Database(_seq, db_id, mut db_meta) = seq_and_id {
                    db_meta.shared_by.remove(&share_id);
                    let key = DatabaseId { db_id };
                    if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&share_name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "revoke_object"
                );

                if succ {
                    return Ok(RevokeShareObjectReply {});
                }
            }
        }

        Err(MetaError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("revoke_object", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> MetaResult<GetShareGrantObjectReply> {
        tracing::debug!(req = debug(&req), "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;

        let res = get_share_or_err(
            self,
            share_name_key,
            format!("revoke_object: {}", &share_name_key),
        )
        .await;

        let (_share_id_seq, _share_id, _share_meta_seq, share_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        if share_meta.database.is_none() {
            return Ok(GetShareGrantObjectReply {
                share_name: req.share_name,
                objects: vec![],
            });
        }

        let entries = match req.object {
            Some(object_name) => {
                let seq_and_id =
                    get_share_object_seq_and_id(self, &object_name, &share_name_key.tenant).await?;
                let object = ShareGrantObject::new(&seq_and_id);
                let entry = share_meta.get_grant_entry(object);
                match entry {
                    Some(entry) => vec![entry],
                    None => vec![],
                }
            }
            None => {
                let mut entries = Vec::new();
                for entry in share_meta.entries {
                    entries.push(entry.1);
                }
                entries.push(share_meta.database.unwrap());
                entries
            }
        };

        Ok(GetShareGrantObjectReply {
            share_name: req.share_name,
            objects: entries,
        })
    }
}

fn check_share_object(
    database: &Option<ShareGrantEntry>,
    seq_and_id: &ShareGrantObjectSeqAndId,
    obj_name: &ShareGrantObjectName,
) -> Result<(), MetaError> {
    if let Some(entry) = database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            let object_db_id = match seq_and_id {
                ShareGrantObjectSeqAndId::Database(_, db_id, _) => *db_id,
                ShareGrantObjectSeqAndId::Table(db_id, _seq, _id) => *db_id,
            };
            if db_id != object_db_id {
                return Err(MetaError::AppError(AppError::WrongShareObject(
                    WrongShareObject::new(obj_name.to_string()),
                )));
            }
        } else {
            unreachable!("database MUST be Database object");
        }
    } else {
        // Table cannot be granted without database has been granted.
        if let ShareGrantObjectSeqAndId::Table(_, _, _) = seq_and_id {
            return Err(MetaError::AppError(AppError::WrongShareObject(
                WrongShareObject::new(obj_name.to_string()),
            )));
        }
    }

    Ok(())
}

/// Returns ShareGrantObjectSeqAndId by ShareGrantObjectName
async fn get_share_object_seq_and_id(
    kv_api: &(impl KVApi + ?Sized),
    obj_name: &ShareGrantObjectName,
    tenant: &str,
) -> Result<ShareGrantObjectSeqAndId, MetaError> {
    match obj_name {
        ShareGrantObjectName::Database(db_name) => {
            let name_key = DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.clone(),
            };
            let (_db_id_seq, db_id, db_meta_seq, db_meta) = get_db_or_err(
                kv_api,
                &name_key,
                format!("get_share_object_seq_and_id: {}", name_key),
            )
            .await?;

            Ok(ShareGrantObjectSeqAndId::Database(
                db_meta_seq,
                db_id,
                db_meta,
            ))
        }

        ShareGrantObjectName::Table(db_name, table_name) => {
            let db_name_key = DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.clone(),
            };
            let (db_seq, db_id) = get_u64_value(kv_api, &db_name_key).await?;
            db_has_to_exist(
                db_seq,
                &db_name_key,
                format!("get_share_object_seq_and_id: {}", db_name_key),
            )?;

            let name_key = DBIdTableName {
                db_id,
                table_name: table_name.clone(),
            };

            let (table_seq, table_id) = get_u64_value(kv_api, &name_key).await?;
            table_has_to_exist(
                table_seq,
                &TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.clone(),
                    table_name: table_name.clone(),
                },
                format!("get_share_object_seq_and_id: {}", name_key),
            )?;

            let tbid = TableId { table_id };
            let (table_meta_seq, _tb_meta): (_, Option<TableMeta>) =
                get_struct_value(kv_api, &tbid).await?;

            Ok(ShareGrantObjectSeqAndId::Table(
                db_id,
                table_meta_seq,
                table_id,
            ))
        }
    }
}

fn add_txn_condition(seq_and_id: &ShareGrantObjectSeqAndId, condition: &mut Vec<TxnCondition>) {
    match seq_and_id {
        ShareGrantObjectSeqAndId::Database(db_meta_seq, db_id, _meta) => {
            let key = DatabaseId { db_id: *db_id };
            condition.push(txn_cond_seq(&key, Eq, *db_meta_seq))
        }
        ShareGrantObjectSeqAndId::Table(_db_id, table_meta_seq, table_id) => {
            let key = TableId {
                table_id: *table_id,
            };
            condition.push(txn_cond_seq(&key, Eq, *table_meta_seq))
        }
    }
}

fn add_grant_object_txn_if_then(
    share_id: u64,
    seq_and_id: ShareGrantObjectSeqAndId,
    if_then: &mut Vec<TxnOp>,
) -> MetaResult<()> {
    match seq_and_id {
        ShareGrantObjectSeqAndId::Database(_db_meta_seq, db_id, mut db_meta) => {
            // modify db_meta add share_id into shared_by
            if !db_meta.shared_by.contains(&share_id) {
                db_meta.shared_by.insert(share_id);
                let key = DatabaseId { db_id };
                if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
            }
        }
        ShareGrantObjectSeqAndId::Table(_, _, _) => {}
    }

    Ok(())
}

/// Returns (share_meta_seq, share_meta)
pub(crate) async fn get_share_id_to_name_or_err(
    kv_api: &(impl KVApi + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareNameIdent), MetaError> {
    let id_key = ShareIdToName { share_id };

    let (share_name_seq, share_name) = get_struct_value(kv_api, &id_key).await?;
    if share_name_seq == 0 {
        tracing::debug!(share_name_seq, ?share_id, "share meta does not exist");

        return Err(MetaError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )));
    }

    Ok((share_name_seq, share_name.unwrap()))
}

/// Returns (share_meta_seq, share_meta)
pub(crate) async fn get_share_meta_by_id_or_err(
    kv_api: &(impl KVApi + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareMeta), MetaError> {
    let id_key = ShareId { share_id };

    let (share_meta_seq, share_meta) = get_struct_value(kv_api, &id_key).await?;
    share_meta_has_to_exist(share_meta_seq, share_id, msg)?;

    Ok((share_meta_seq, share_meta.unwrap()))
}

/// Returns (share_id_seq, share_id, share_meta_seq, share_meta)
async fn get_share_or_err(
    kv_api: &impl KVApi,
    name_key: &ShareNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ShareMeta), MetaError> {
    let (share_id_seq, share_id) = get_u64_value(kv_api, name_key).await?;
    share_has_to_exist(share_id_seq, name_key, &msg)?;

    let (share_meta_seq, share_meta) = get_share_meta_by_id_or_err(kv_api, share_id, msg).await?;

    Ok((share_id_seq, share_id, share_meta_seq, share_meta))
}

fn share_meta_has_to_exist(seq: u64, share_id: u64, msg: impl Display) -> Result<(), MetaError> {
    if seq == 0 {
        tracing::debug!(seq, ?share_id, "share meta does not exist");

        Err(MetaError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a share_id or share_meta exists by checking the seq.
///
/// Otherwise returns UnknownShare error
fn share_has_to_exist(
    seq: u64,
    share_name_ident: &ShareNameIdent,
    msg: impl Display,
) -> Result<(), MetaError> {
    if seq == 0 {
        tracing::debug!(seq, ?share_name_ident, "share does not exist");

        Err(MetaError::AppError(AppError::UnknownShare(
            UnknownShare::new(
                &share_name_ident.share_name,
                format!("{}: {}", msg, share_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (share_account_meta_seq, share_account_meta)
pub(crate) async fn get_share_account_meta_or_err(
    kv_api: &(impl KVApi + ?Sized),
    name_key: &ShareAccountNameIdent,
    msg: impl Display,
) -> Result<(u64, ShareAccountMeta), MetaError> {
    let (share_account_meta_seq, share_account_meta): (u64, Option<ShareAccountMeta>) =
        get_struct_value(kv_api, name_key).await?;
    share_account_meta_has_to_exist(share_account_meta_seq, name_key, msg)?;

    Ok((
        share_account_meta_seq,
        // Safe unwrap(): share_meta_seq > 0 implies share_meta is not None.
        share_account_meta.unwrap(),
    ))
}

/// Return OK if a share_id or share_account_meta exists by checking the seq.
///
/// Otherwise returns UnknownShareAccount error
fn share_account_meta_has_to_exist(
    seq: u64,
    name_key: &ShareAccountNameIdent,
    msg: impl Display,
) -> Result<(), MetaError> {
    if seq == 0 {
        tracing::debug!(seq, ?name_key, "share account does not exist");

        Err(MetaError::AppError(AppError::UnknownShareAccount(
            UnknownShareAccount::new(
                &name_key.account,
                name_key.share_id,
                format!("{}: {}", msg, name_key),
            ),
        )))
    } else {
        Ok(())
    }
}
