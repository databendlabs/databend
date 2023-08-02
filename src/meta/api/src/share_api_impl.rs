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

use chrono::DateTime;
use chrono::Utc;
use common_meta_app::app_error::AppError;
use common_meta_app::app_error::CannotShareDatabaseCreatedFromShare;
use common_meta_app::app_error::ShareAccountsAlreadyExists;
use common_meta_app::app_error::ShareAlreadyExists;
use common_meta_app::app_error::ShareEndpointAlreadyExists;
use common_meta_app::app_error::TxnRetryMaxTimes;
use common_meta_app::app_error::UnknownShare;
use common_meta_app::app_error::UnknownShareAccounts;
use common_meta_app::app_error::UnknownTable;
use common_meta_app::app_error::WrongShare;
use common_meta_app::app_error::WrongShareObject;
use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdToName;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdToName;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::share::*;
use common_meta_kvapi::kvapi;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::MetaError;
use common_meta_types::TxnCondition;
use common_meta_types::TxnOp;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use log::as_debug;
use log::debug;
use log::error;

use crate::assert_table_exist;
use crate::convert_share_meta_to_spec;
use crate::db_has_to_exist;
use crate::fetch_id;
use crate::get_db_or_err;
use crate::get_object_shared_by_share_ids;
use crate::get_pb_value;
use crate::get_share_account_meta_or_err;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::get_share_or_err;
use crate::get_share_table_info;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::list_keys;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::util::get_share_endpoint_id_to_name_or_err;
use crate::util::get_share_endpoint_or_err;
use crate::ShareApi;
use crate::TXN_MAX_RETRY_TIMES;

/// ShareApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls ShareApi.
#[async_trait::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> ShareApi for KV {
    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn show_shares(&self, req: ShowSharesReq) -> Result<ShowSharesReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        // Get all outbound share accounts.
        let outbound_accounts = get_outbound_share_infos_by_tenant(self, &req.tenant).await?;

        Ok(ShowSharesReply { outbound_accounts })
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn create_share(&self, req: CreateShareReq) -> Result<CreateShareReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            // Get share by name to ensure absence
            let (share_id_seq, share_id) = get_u64_value(self, name_key).await?;
            debug!(share_id_seq = share_id_seq, share_id = share_id, name_key = as_debug!(name_key); "get_share");

            if share_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateShareReply {
                        share_id,
                        spec_vec: None,
                    })
                } else {
                    Err(KVAppError::AppError(AppError::ShareAlreadyExists(
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

            let share_id = fetch_id(self, IdGenerator::share_id()).await?;
            let id_key = ShareId { share_id };
            let id_to_name_key = ShareIdToName { share_id };

            debug!(share_id = share_id, name_key = as_debug!(name_key); "new share id");

            // Create share by transaction.
            {
                let share_meta = ShareMeta::new(req.create_on, req.comment.clone());
                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&id_to_name_key, Eq, 0),
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(share_id)?), /* (tenant, share_name) -> share_id */
                        txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                        txn_op_put(&id_to_name_key, serialize_struct(name_key)?), /* __fd_share_id_to_name/<share_id> -> (tenant,share_name) */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_share"
                );

                if succ {
                    return Ok(CreateShareReply {
                        share_id,
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, name_key.tenant.clone()).await?,
                        ),
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_share", TXN_MAX_RETRY_TIMES),
        )))
    }

    // When drop a share, need to:
    // drop __fd_share/<tenant>/<share_name> -> <share_id>
    // drop __fd_share_id/<share_id> -> <share_meta>
    // drop __fd_share_id_to_name/<share_id> -> ShareNameIdent
    // iterator all the granted accounts(from ShareMeta.accounts),
    //     drop __fd_share_account/tenant/id -> ShareAccountMeta
    // iterator all the granted objects(from ShareMeta.{database|entries}),
    //     remove share id from ObjectSharedByShareIds
    // drop all the databases created from the share(from ShareMeta.share_from_db_ids),
    async fn drop_share(&self, req: DropShareReq) -> Result<DropShareReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res = get_share_or_err(self, name_key, format!("drop_share: {}", &name_key)).await;

            let (share_id_seq, share_id, share_meta_seq, share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {
                                share_id: None,
                                spec_vec: None,
                            });
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
                    if let KVAppError::AppError(AppError::UnknownShareId(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {
                                share_id: Some(share_id),
                                spec_vec: None,
                            });
                        }
                    }

                    return Err(e);
                }
            };

            // Delete share by these operations:
            // del (tenant, share_name)
            // del share_id
            // del (share_id) -> (tenant, share_name)
            // del all outbound of share
            // remove share id from share object metas
            // drop the database created from share

            let mut condition = vec![];
            let mut if_then = vec![];

            // drop all accounts seq from share_meta
            drop_accounts_granted_from_share(
                self,
                share_id,
                &share_meta,
                &mut condition,
                &mut if_then,
            )
            .await?;

            // remove share id from the share objects
            remove_share_id_from_share_objects(
                self,
                share_id,
                &share_meta,
                &mut condition,
                &mut if_then,
            )
            .await?;

            // drop all the databases created from the share
            // drop_all_database_from_share(self, share_id, &share_meta, &mut condition, &mut if_then)
            //    .await?;

            let share_id_key = ShareId { share_id };
            let id_name_key = ShareIdToName { share_id };

            debug!(share_id = share_id, name_key = as_debug!(name_key); "drop_share");

            {
                condition.push(txn_cond_seq(name_key, Eq, share_id_seq));
                condition.push(txn_cond_seq(&share_id_key, Eq, share_meta_seq));
                condition.push(txn_cond_seq(&id_name_key, Eq, share_name_seq));
                if_then.push(txn_op_del(name_key)); // del (tenant, share_name)
                if_then.push(txn_op_del(&share_id_key)); // del share_id
                if_then.push(txn_op_del(&id_name_key)); // del (share_id) -> (tenant, share_name)

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    id = as_debug!(&share_id_key),
                    succ = succ;
                    "drop_share"
                );

                if succ {
                    return Ok(DropShareReply {
                        share_id: Some(share_id),
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, name_key.tenant.clone()).await?,
                        ),
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_share", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn add_share_tenants(
        &self,
        req: AddShareAccountsReq,
    ) -> Result<AddShareAccountsReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res =
                get_share_or_err(self, name_key, format!("add_share_tenants: {}", &name_key)).await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(AddShareAccountsReply {
                                share_id: None,
                                spec_vec: None,
                            });
                        }
                    }
                    return Err(e);
                }
            };

            let mut add_share_account_keys = vec![];
            for account in req.accounts.iter() {
                if !share_meta.has_account(account) {
                    add_share_account_keys.push(ShareAccountNameIdent {
                        account: account.clone(),
                        share_id,
                    });
                }
            }
            if add_share_account_keys.is_empty() {
                return Err(KVAppError::AppError(AppError::ShareAccountsAlreadyExists(
                    ShareAccountsAlreadyExists::new(
                        req.share_name.share_name,
                        &req.accounts,
                        "share accounts already exists",
                    ),
                )));
            }

            // Add share account by these operations:
            // mod share_meta add account
            // add (account, share_id) -> share_account_meta
            // return share_id
            {
                let id_key = ShareId { share_id };
                let mut condition = vec![
                    txn_cond_seq(name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                ];
                let mut if_then = vec![];

                for share_account_key in add_share_account_keys.iter() {
                    condition.push(txn_cond_seq(share_account_key, Eq, 0));

                    let share_account_meta = ShareAccountMeta::new(
                        share_account_key.account.clone(),
                        share_id,
                        req.share_on,
                    );

                    if_then.push(txn_op_put(
                        share_account_key,
                        serialize_struct(&share_account_meta)?,
                    )); /* (account, share_id) -> share_account_meta */

                    share_meta.add_account(share_account_key.account.clone());
                }
                if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?)); /* (share_id) -> share_meta */

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    id = as_debug!(&id_key),
                    succ = succ;
                    "add_share_tenants"
                );

                if succ {
                    return Ok(AddShareAccountsReply {
                        share_id: Some(share_id),
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, name_key.tenant.clone()).await?,
                        ),
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("add_share_tenants", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn remove_share_tenants(
        &self,
        req: RemoveShareAccountsReq,
    ) -> Result<RemoveShareAccountsReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;
        let mut retry = 0;

        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res = get_share_or_err(
                self,
                name_key,
                format!("remove_share_tenants: {}", &name_key),
            )
            .await;

            let (_share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(RemoveShareAccountsReply {
                                share_id: None,
                                spec_vec: None,
                            });
                        }
                    }
                    return Err(e);
                }
            };

            let mut remove_share_account_keys_and_seqs = vec![];
            for account in req.accounts.iter() {
                if account == &name_key.tenant {
                    continue;
                }
                if share_meta.has_account(account) {
                    let share_account_key = ShareAccountNameIdent {
                        account: account.clone(),
                        share_id,
                    };

                    let res = get_share_account_meta_or_err(
                        self,
                        &share_account_key,
                        format!("remove_share_tenants: {}", share_id),
                    )
                    .await;

                    let (share_meta_account_seq, _share_account_meta) = match res {
                        Ok(x) => x,
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    remove_share_account_keys_and_seqs
                        .push((share_account_key, share_meta_account_seq));
                }
            }

            if remove_share_account_keys_and_seqs.is_empty() {
                return Err(KVAppError::AppError(AppError::UnknownShareAccounts(
                    UnknownShareAccounts::new(&req.accounts, share_id, "unknown share account"),
                )));
            }

            // Remove share account by these operations:
            // mod share_meta delete account
            // del (account, share_id)
            // return share_id
            {
                let id_key = ShareId { share_id };
                let mut condition = vec![txn_cond_seq(&id_key, Eq, share_meta_seq)];
                let mut if_then = vec![];

                for share_account_key_and_seq in remove_share_account_keys_and_seqs.iter() {
                    condition.push(txn_cond_seq(
                        &share_account_key_and_seq.0,
                        Eq,
                        share_account_key_and_seq.1,
                    ));

                    if_then.push(txn_op_del(&share_account_key_and_seq.0)); // del (account, share_id)

                    share_meta.del_account(&share_account_key_and_seq.0.account);
                }
                if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?)); /* (share_id) -> share_meta */

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    id = as_debug!(&id_key),
                    succ = succ;
                    "remove_share_tenants"
                );

                if succ {
                    return Ok(RemoveShareAccountsReply {
                        share_id: Some(share_id),
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, name_key.tenant.clone()).await?,
                        ),
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("remove_share_tenants", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn grant_share_object(
        &self,
        req: GrantShareObjectReq,
    ) -> Result<GrantShareObjectReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res = get_share_or_err(
                self,
                share_name_key,
                format!("grant_share_object: {}", &share_name_key),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let seq_and_id =
                get_share_object_seq_and_id(self, &req.object, &share_name_key.tenant, true)
                    .await?;

            check_share_object(&share_meta.database, &seq_and_id, &req.object)?;

            // Check the object privilege has been granted
            let has_granted_privileges =
                share_meta.has_granted_privileges(&req.object, &seq_and_id, req.privilege)?;

            if has_granted_privileges {
                return Ok(GrantShareObjectReply {
                    share_id,
                    spec_vec: None,
                    share_table_info: (share_name_key.share_name.clone(), None),
                });
            }

            // Grant the object privilege by inserting these record:
            // add privilege and upsert (share_id) -> share_meta
            // if grant database then update db_meta.shared_on and upsert (db_id) -> db_meta

            // Grant the object privilege by transaction.
            {
                let id_key = ShareId { share_id };
                // modify the share_meta add privilege
                let object = ShareGrantObject::new(&seq_and_id);

                // modify share_ids
                let res = get_object_shared_by_share_ids(self, &object).await?;
                let share_ids_seq = res.0;
                let mut share_ids: ObjectSharedByShareIds = res.1;
                share_ids.add(share_id);

                share_meta.grant_object_privileges(object.clone(), req.privilege, req.grant_on);

                // condition
                let mut condition: Vec<TxnCondition> = vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                    txn_cond_seq(&object, Eq, share_ids_seq),
                ];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![
                    txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                    txn_op_put(&object, serialize_struct(&share_ids)?),  // (object) -> share_ids
                ];
                // Some database has been created before `DatabaseIdToName`, so create it if need.
                create_db_name_to_id_key_if_need(
                    self,
                    &seq_and_id,
                    &share_name_key.tenant,
                    &req.object,
                    &mut condition,
                    &mut if_then,
                )
                .await?;
                add_grant_object_txn_if_then(
                    share_id,
                    seq_and_id.clone(),
                    &mut condition,
                    &mut if_then,
                )?;

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(share_name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "grant_share_object"
                );

                if succ {
                    return Ok(GrantShareObjectReply {
                        share_id,
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, share_name_key.tenant.clone()).await?,
                        ),
                        share_table_info: get_share_table_info(self, share_name_key, &share_meta)
                            .await?,
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("grant_share_object", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn revoke_share_object(
        &self,
        req: RevokeShareObjectReq,
    ) -> Result<RevokeShareObjectReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res = get_share_or_err(
                self,
                share_name_key,
                format!("revoke_share_object: {}", &share_name_key),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let seq_and_id =
                get_share_object_seq_and_id(self, &req.object, &share_name_key.tenant, false)
                    .await?;

            check_share_object(&share_meta.database, &seq_and_id, &req.object)?;

            // Check the object privilege has not been granted.
            let has_granted_privileges =
                share_meta.has_granted_privileges(&req.object, &seq_and_id, req.privilege)?;

            if !has_granted_privileges {
                return Ok(RevokeShareObjectReply {
                    share_id,
                    spec_vec: None,
                    share_table_info: (share_name_key.share_name.clone(), None),
                });
            }

            // Revoke the object privilege by upserting these record:
            // revoke privilege in share_meta and upsert (share_id) -> share_meta
            // update {db_meta|table_meta}.shared_by and upsert (db_id|table_id) -> (db_meta|table_id)

            // Revoke the object privilege by transaction.
            {
                let id_key = ShareId { share_id };
                // modify the share_meta add privilege
                let object = ShareGrantObject::new(&seq_and_id);

                // modify share_ids
                let res = get_object_shared_by_share_ids(self, &object).await?;
                let share_ids_seq = res.0;
                let mut share_ids: ObjectSharedByShareIds = res.1;
                share_ids.remove(share_id);

                // condition
                let mut condition: Vec<TxnCondition> = vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                    txn_cond_seq(&object, Eq, share_ids_seq),
                ];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![
                    txn_op_put(&object, serialize_struct(&share_ids)?), // (object) -> share_ids
                ];

                let _ = revoke_object_privileges(
                    self,
                    &mut share_meta,
                    object.clone(),
                    share_id,
                    req.privilege,
                    req.update_on,
                    &mut condition,
                    &mut if_then,
                )
                .await?;

                // update share meta
                if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?)); /* (share_id) -> share_meta */

                match seq_and_id {
                    ShareGrantObjectSeqAndId::Database(db_meta_seq, db_id, mut db_meta) => {
                        db_meta.shared_by.remove(&share_id);
                        let key = DatabaseId { db_id };
                        if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, db_meta_seq));
                    }
                    ShareGrantObjectSeqAndId::Table(
                        _db_id,
                        table_meta_seq,
                        table_id,
                        mut table_meta,
                    ) => {
                        table_meta.shared_by.remove(&share_id);
                        let key = TableId { table_id };
                        if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                    }
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(share_name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "revoke_share_object"
                );

                if succ {
                    return Ok(RevokeShareObjectReply {
                        share_id,
                        spec_vec: Some(
                            get_tenant_share_spec_vec(self, share_name_key.tenant.clone()).await?,
                        ),
                        share_table_info: get_share_table_info(self, share_name_key, &share_meta)
                            .await?,
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("revoke_share_object", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> Result<GetShareGrantObjectReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;

        let res = get_share_or_err(
            self,
            share_name_key,
            format!("revoke_share_object: {}", &share_name_key),
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

        let database_obj = share_meta.database.clone().unwrap();
        let database = get_object_name_from_id(self, &None, database_obj.object).await?;
        if database.is_none() {
            return Ok(GetShareGrantObjectReply {
                share_name: req.share_name,
                objects: vec![],
            });
        }
        let database_name = match database.as_ref().unwrap() {
            ShareGrantObjectName::Database(db_name) => Some(db_name),
            ShareGrantObjectName::Table(_, _) => {
                return Ok(GetShareGrantObjectReply {
                    share_name: req.share_name,
                    objects: vec![],
                });
            }
        };

        let mut entries = Vec::new();
        for entry in share_meta.entries {
            entries.push(entry.1);
        }
        entries.push(share_meta.database.unwrap());

        let mut objects = vec![];
        for entry in entries {
            let object = get_object_name_from_id(self, &database_name, entry.object).await?;
            if let Some(object) = object {
                objects.push(ShareGrantReplyObject {
                    object,
                    privileges: entry.privileges,
                    grant_on: entry.grant_on,
                })
            }
        }

        Ok(GetShareGrantObjectReply {
            share_name: req.share_name,
            objects,
        })
    }

    // Return all the grant tenants of the share
    async fn get_grant_tenants_of_share(
        &self,
        req: GetShareGrantTenantsReq,
    ) -> Result<GetShareGrantTenantsReply, KVAppError> {
        let accounts = get_outbound_share_tenants_by_name(self, &req.share_name).await?;

        Ok(GetShareGrantTenantsReply { accounts })
    }

    // Return all the grant privileges of the object
    async fn get_grant_privileges_of_object(
        &self,
        req: GetObjectGrantPrivilegesReq,
    ) -> Result<GetObjectGrantPrivilegesReply, KVAppError> {
        let entries = match req.object {
            ShareGrantObjectName::Database(db_name) => {
                let db_name_key = DatabaseNameIdent {
                    tenant: req.tenant,
                    db_name: db_name.clone(),
                };
                let (db_seq, db_id) = get_u64_value(self, &db_name_key).await?;
                db_has_to_exist(
                    db_seq,
                    &db_name_key,
                    format!("get_grant_privileges_of_object: {}", db_name_key),
                )?;
                let object = ShareGrantObject::Database(db_id);
                let (_seq, share_ids) = get_object_shared_by_share_ids(self, &object).await?;
                let mut entries = vec![];
                for share_id in share_ids.share_ids.iter() {
                    let (_seq, share_name) = get_share_id_to_name_or_err(
                        self,
                        *share_id,
                        format!("get_grant_privileges_of_object: {}", &share_id),
                    )
                    .await?;

                    let (_seq, share_meta) = get_share_meta_by_id_or_err(
                        self,
                        *share_id,
                        format!("get_grant_privileges_of_object: {}", &share_id),
                    )
                    .await?;

                    entries.push((
                        share_meta.get_grant_entry(object.clone()),
                        share_name.share_name,
                    ));
                }

                entries
            }
            ShareGrantObjectName::Table(db_name, table_name) => {
                let db_name_key = DatabaseNameIdent {
                    tenant: req.tenant.clone(),
                    db_name: db_name.clone(),
                };
                let (db_seq, db_id) = get_u64_value(self, &db_name_key).await?;
                db_has_to_exist(
                    db_seq,
                    &db_name_key,
                    format!("get_grant_privileges_of_object: {}", db_name_key),
                )?;

                let table_name_key = DBIdTableName {
                    db_id,
                    table_name: table_name.clone(),
                };
                let (table_seq, table_id) = get_u64_value(self, &table_name_key).await?;
                assert_table_exist(
                    table_seq,
                    &TableNameIdent {
                        tenant: req.tenant.clone(),
                        db_name: db_name.clone(),
                        table_name,
                    },
                    format!("get_grant_privileges_of_object: {}", table_name_key),
                )?;

                let object = ShareGrantObject::Table(table_id);
                let (_seq, share_ids) = get_object_shared_by_share_ids(self, &object).await?;
                let mut entries = vec![];
                for share_id in share_ids.share_ids.iter() {
                    let (_seq, share_name) = get_share_id_to_name_or_err(
                        self,
                        *share_id,
                        format!("get_grant_privileges_of_object: {}", &share_id),
                    )
                    .await?;

                    let (_seq, share_meta) = get_share_meta_by_id_or_err(
                        self,
                        *share_id,
                        format!("get_grant_privileges_of_object: {}", &share_id),
                    )
                    .await?;

                    entries.push((
                        share_meta.get_grant_entry(object.clone()),
                        share_name.share_name,
                    ));
                }

                entries
            }
        };
        let mut privileges = vec![];
        for (entry, share_name) in entries {
            if let Some(entry) = entry {
                privileges.push(ObjectGrantPrivilege {
                    share_name,
                    privileges: entry.privileges,
                    grant_on: entry.grant_on,
                });
            }
        }
        Ok(GetObjectGrantPrivilegesReply { privileges })
    }

    async fn create_share_endpoint(
        &self,
        req: CreateShareEndpointReq,
    ) -> Result<CreateShareEndpointReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            // Get share endpoint by name to ensure absence
            let (share_endpoint_id_seq, share_endpoint_id) = get_u64_value(self, name_key).await?;
            debug!(
                share_endpoint_id_seq = share_endpoint_id_seq,
                share_endpoint_id = share_endpoint_id,
                name_key = as_debug!(name_key);
                "create_share_endpoint"
            );

            if share_endpoint_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateShareEndpointReply { share_endpoint_id })
                } else {
                    Err(KVAppError::AppError(AppError::ShareEndpointAlreadyExists(
                        ShareEndpointAlreadyExists::new(
                            &name_key.endpoint,
                            format!("create share endpoint: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            // Create share endpoint by inserting these record:
            // (tenant, endpoint) -> share_endpoint_id
            // (share_endpoint_id) -> share_endpoint_meta
            // (share) -> (tenant,share_name)

            let share_endpoint_id = fetch_id(self, IdGenerator::share_endpoint_id()).await?;
            let id_key = ShareEndpointId { share_endpoint_id };
            let id_to_name_key = ShareEndpointIdToName { share_endpoint_id };

            debug!(
                share_endpoint_id = share_endpoint_id,
                name_key = as_debug!(name_key);
                "new share endpoint id"
            );

            // Create share endpoint by transaction.
            {
                let share_endpoint_meta = ShareEndpointMeta::new(&req);
                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&id_to_name_key, Eq, 0),
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(share_endpoint_id)?), /* (tenant, share_endpoint_name) -> share_endpoint_id */
                        txn_op_put(&id_key, serialize_struct(&share_endpoint_meta)?), /* (share_endpoint_id) -> share_endpoint_meta */
                        txn_op_put(&id_to_name_key, serialize_struct(name_key)?), /* __fd_share_endpoint_id_to_name/<share_endpoint_id> -> (tenant,share_endpoint_name) */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_share_endpoint"
                );

                if succ {
                    return Ok(CreateShareEndpointReply { share_endpoint_id });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_share_endpoint", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn upsert_share_endpoint(
        &self,
        req: UpsertShareEndpointReq,
    ) -> Result<UpsertShareEndpointReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            // Get share endpoint by name to ensure absence
            let (share_endpoint_id_seq, share_endpoint_id) = get_u64_value(self, name_key).await?;
            debug!(
                share_endpoint_id_seq = share_endpoint_id_seq,
                share_endpoint_id = share_endpoint_id,
                name_key = as_debug!(name_key);
                "upsert_share_endpoint"
            );

            // Create share endpoint by inserting these record:
            // (tenant, endpoint) -> share_endpoint_id
            // (share_endpoint_id) -> share_endpoint_meta
            // (share) -> (tenant,share_name)

            // get (share_endpoint_meta_seq, share_endpoint_meta)
            let (share_endpoint_meta_seq, share_endpoint_meta) = if share_endpoint_id_seq == 0 {
                let share_endpoint_meta = ShareEndpointMeta::empty().upsert(&req);
                (0, share_endpoint_meta)
            } else {
                let id_key = ShareEndpointId { share_endpoint_id };
                let (seq, share_endpoint_meta_opt): (u64, Option<ShareEndpointMeta>) =
                    get_pb_value(self, &id_key).await?;

                let share_endpoint_meta = share_endpoint_meta_opt.unwrap();
                // no need to upsert meta, return
                if !share_endpoint_meta.if_need_to_upsert(&req) {
                    return Ok(UpsertShareEndpointReply { share_endpoint_id });
                }
                (seq, share_endpoint_meta.upsert(&req))
            };

            debug!(
                share_endpoint_id = share_endpoint_id,
                name_key = as_debug!(name_key);
                "new share endpoint id"
            );

            // upsert share endpoint by transaction.
            {
                let id_key = ShareEndpointId { share_endpoint_id };
                let mut condition = vec![
                    txn_cond_seq(name_key, Eq, share_endpoint_id_seq),
                    txn_cond_seq(&id_key, Eq, share_endpoint_meta_seq),
                ];
                let mut if_then = vec![];

                let share_endpoint_id = if share_endpoint_id_seq == 0 {
                    // create a new endpoint if no endpoint exists before
                    let new_share_endpoint_id =
                        fetch_id(self, IdGenerator::share_endpoint_id()).await?;
                    let id_to_name_key = ShareEndpointIdToName {
                        share_endpoint_id: new_share_endpoint_id,
                    };
                    let share_endpoint_id_key = ShareEndpointId {
                        share_endpoint_id: new_share_endpoint_id,
                    };
                    if_then.push(txn_op_put(name_key, serialize_u64(new_share_endpoint_id)?));
                    if_then.push(txn_op_put(&id_to_name_key, serialize_struct(name_key)?));
                    if_then.push(txn_op_put(
                        &share_endpoint_id_key,
                        serialize_struct(&share_endpoint_meta)?,
                    ));
                    condition.push(txn_cond_seq(&id_to_name_key, Eq, 0));

                    new_share_endpoint_id
                } else {
                    // else only update share endpoint meta
                    let share_endpoint_id_key = ShareEndpointId { share_endpoint_id };
                    if_then.push(txn_op_put(
                        &share_endpoint_id_key,
                        serialize_struct(&share_endpoint_meta)?,
                    ));
                    share_endpoint_id
                };

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    succ = succ;
                    "upsert_share_endpoint"
                );

                if succ {
                    return Ok(UpsertShareEndpointReply { share_endpoint_id });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("upsert_share_endpoint", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn get_share_endpoint(
        &self,
        req: GetShareEndpointReq,
    ) -> Result<GetShareEndpointReply, KVAppError> {
        let mut share_endpoint_meta_vec = vec![];

        let tenant_share_endpoint_name_key = ShareEndpointIdent {
            tenant: req.tenant.clone(),
            endpoint: req.endpoint.clone().unwrap_or("".to_string()),
        };
        let share_endpoints = list_keys(self, &tenant_share_endpoint_name_key).await?;
        for share_endpoint in share_endpoints {
            let (_seq, share_endpoint_id) = get_u64_value(self, &share_endpoint).await?;
            let id_key = ShareEndpointId { share_endpoint_id };
            let (_seq, share_endpoint_meta): (u64, Option<ShareEndpointMeta>) =
                get_pb_value(self, &id_key).await?;
            if let Some(share_endpoint_meta) = share_endpoint_meta {
                if let Some(to_tenant) = &req.to_tenant {
                    if to_tenant == &share_endpoint_meta.tenant {
                        share_endpoint_meta_vec.push((share_endpoint, share_endpoint_meta));
                    }
                } else {
                    share_endpoint_meta_vec.push((share_endpoint, share_endpoint_meta));
                }
            }
        }

        Ok(GetShareEndpointReply {
            share_endpoint_meta_vec,
        })
    }

    async fn drop_share_endpoint(
        &self,
        req: DropShareEndpointReq,
    ) -> Result<DropShareEndpointReply, KVAppError> {
        debug!(req = as_debug!(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let res = get_share_endpoint_or_err(
                self,
                name_key,
                format!("drop_share_endpoint: {}", &name_key),
            )
            .await;

            let (
                share_endpoint_id_seq,
                share_endpoint_id,
                share_endpoint_meta_seq,
                _share_endpoint_meta,
            ) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShareEndpoint(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareEndpointReply {});
                        }
                    }

                    return Err(e);
                }
            };
            let (share_endpoint_name_seq, _share_endpoint) = get_share_endpoint_id_to_name_or_err(
                self,
                share_endpoint_id,
                format!("drop_share_endpoint: {}", &name_key),
            )
            .await?;

            // Delete share endpoint by these operations:
            // del (tenant, share_endpoint)
            // del share_endpoint_id
            // del (share_endpoint_id) -> (tenant, share_endpoint)

            let mut condition = vec![];
            let mut if_then = vec![];

            let share_id_key = ShareEndpointId { share_endpoint_id };
            let id_name_key = ShareEndpointIdToName { share_endpoint_id };

            debug!(
                share_endpoint_id = share_endpoint_id,
                name_key = as_debug!(name_key);
                "drop_share_endpoint"
            );

            {
                condition.push(txn_cond_seq(name_key, Eq, share_endpoint_id_seq));
                condition.push(txn_cond_seq(&share_id_key, Eq, share_endpoint_meta_seq));
                condition.push(txn_cond_seq(&id_name_key, Eq, share_endpoint_name_seq));
                if_then.push(txn_op_del(name_key)); // del (tenant, share_endpoint)
                if_then.push(txn_op_del(&share_id_key)); // del share_endpoint_id
                if_then.push(txn_op_del(&id_name_key)); // del (share_endpoint_id) -> (tenant, share_endpoint)

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key) ,
                    id = as_debug!(&share_id_key),
                    succ = succ;
                    "drop_share_endpoint"
                );

                if succ {
                    return Ok(DropShareEndpointReply {});
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_share_endpoint", TXN_MAX_RETRY_TIMES),
        )))
    }
}

async fn get_share_database_name(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_meta: &ShareMeta,
    share_name: &ShareNameIdent,
) -> Result<Option<String>, KVAppError> {
    if let Some(entry) = &share_meta.database {
        match entry.object {
            ShareGrantObject::Database(db_id) => {
                let id_to_name = DatabaseIdToName { db_id };
                let (name_ident_seq, name_ident): (_, Option<DatabaseNameIdent>) =
                    get_pb_value(kv_api, &id_to_name).await?;
                if name_ident_seq == 0 || name_ident.is_none() {
                    return Err(KVAppError::AppError(AppError::UnknownShare(
                        UnknownShare::new(&share_name.share_name, ""),
                    )));
                }
                Ok(Some(name_ident.unwrap().db_name))
            }
            ShareGrantObject::Table(_id) => Err(KVAppError::AppError(AppError::WrongShare(
                WrongShare::new(&share_name.share_name),
            ))),
        }
    } else {
        Ok(None)
    }
}

async fn get_outbound_share_tenants_by_name(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &ShareNameIdent,
) -> Result<Vec<GetShareGrantTenants>, KVAppError> {
    let res = get_share_or_err(kv_api, share_name, format!("get_share: {share_name}")).await?;
    let (_share_id_seq, share_id, _share_meta_seq, share_meta) = res;

    let mut accounts = vec![];
    for account in share_meta.get_accounts() {
        let share_account_key = ShareAccountNameIdent {
            account: account.clone(),
            share_id,
        };

        let (_seq, meta) = get_share_account_meta_or_err(
            kv_api,
            &share_account_key,
            format!("get_outbound_share_tenants_by_name's account: {share_id}/{account}"),
        )
        .await?;

        accounts.push(GetShareGrantTenants {
            account,
            grant_on: meta.share_on,
        });
    }

    Ok(accounts)
}

async fn get_outbound_share_info_by_name(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &ShareNameIdent,
) -> Result<ShareAccountReply, KVAppError> {
    let res = get_share_or_err(
        kv_api,
        share_name,
        format!("get_share: {}", share_name.clone()),
    )
    .await?;
    let (_share_id_seq, _share_id, _share_meta_seq, share_meta) = res;

    let mut accounts = vec![];
    for account in share_meta.get_accounts().iter() {
        accounts.push(account.clone());
    }

    let database_name = get_share_database_name(kv_api, &share_meta, share_name).await?;

    Ok(ShareAccountReply {
        share_name: share_name.clone(),
        database_name,
        create_on: share_meta.share_on,
        accounts: Some(accounts),
        comment: share_meta.comment.clone(),
    })
}

async fn get_outbound_share_infos_by_tenant(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &str,
) -> Result<Vec<ShareAccountReply>, KVAppError> {
    let mut outbound_share_accounts: Vec<ShareAccountReply> = vec![];

    let tenant_share_name_key = ShareNameIdent {
        tenant: tenant.to_string(),
        share_name: "".to_string(),
    };
    let share_name_keys = list_keys(kv_api, &tenant_share_name_key).await?;

    for share_name in share_name_keys {
        let reply = get_outbound_share_info_by_name(kv_api, &share_name).await;
        if let Ok(reply) = reply {
            outbound_share_accounts.push(reply)
        }
    }

    Ok(outbound_share_accounts)
}

async fn get_object_name_from_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    database_name: &Option<&String>,
    object: ShareGrantObject,
) -> Result<Option<ShareGrantObjectName>, KVAppError> {
    match object {
        ShareGrantObject::Database(db_id) => {
            let db_id_key = DatabaseIdToName { db_id };
            let (_db_name_seq, db_name): (_, Option<DatabaseNameIdent>) =
                get_pb_value(kv_api, &db_id_key).await?;
            Ok(db_name.map(|db_name| ShareGrantObjectName::Database(db_name.db_name)))
        }
        ShareGrantObject::Table(table_id) => {
            let table_id_key = TableIdToName { table_id };
            let (_db_id_table_name_seq, table_name): (_, Option<DBIdTableName>) =
                get_pb_value(kv_api, &table_id_key).await?;
            Ok(table_name.map(|table_name| {
                ShareGrantObjectName::Table(
                    database_name.as_ref().unwrap().to_string(),
                    table_name.table_name,
                )
            }))
        }
    }
}

async fn create_db_name_to_id_key_if_need(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    seq_and_id: &ShareGrantObjectSeqAndId,
    tenant: &str,
    obj_name: &ShareGrantObjectName,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    if let ShareGrantObjectName::Database(db) = obj_name {
        if let ShareGrantObjectSeqAndId::Database(_, db_id, _) = seq_and_id {
            let db_id_key = DatabaseIdToName { db_id: *db_id };
            let (db_name_seq, _): (_, Option<DatabaseNameIdent>) =
                get_pb_value(kv_api, &db_id_key).await?;
            if db_name_seq == 0 {
                condition.push(txn_cond_seq(&db_id_key, Eq, 0));

                let name_key = DatabaseNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db.clone(),
                };
                if_then.push(txn_op_put(&db_id_key, serialize_struct(&name_key)?));
            }
        }
    };
    Ok(())
}

fn check_share_object(
    database: &Option<ShareGrantEntry>,
    seq_and_id: &ShareGrantObjectSeqAndId,
    obj_name: &ShareGrantObjectName,
) -> Result<(), KVAppError> {
    if let Some(entry) = database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            let object_db_id = match seq_and_id {
                ShareGrantObjectSeqAndId::Database(_, db_id, _) => *db_id,
                ShareGrantObjectSeqAndId::Table(db_id, _seq, _id, _meta) => *db_id,
            };
            if db_id != object_db_id {
                return Err(KVAppError::AppError(AppError::WrongShareObject(
                    WrongShareObject::new(obj_name.to_string()),
                )));
            }
        } else {
            unreachable!("database MUST be Database object");
        }
    } else {
        // Table cannot be granted without database has been granted.
        if let ShareGrantObjectSeqAndId::Table(_, _, _, _) = seq_and_id {
            return Err(KVAppError::AppError(AppError::WrongShareObject(
                WrongShareObject::new(obj_name.to_string()),
            )));
        }
    }

    Ok(())
}

/// Returns ShareGrantObjectSeqAndId by ShareGrantObjectName
async fn get_share_object_seq_and_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    obj_name: &ShareGrantObjectName,
    tenant: &str,
    grant: bool,
) -> Result<ShareGrantObjectSeqAndId, KVAppError> {
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

            if grant && db_meta.from_share.is_some() {
                return Err(KVAppError::AppError(
                    AppError::CannotShareDatabaseCreatedFromShare(
                        CannotShareDatabaseCreatedFromShare::new(
                            db_name,
                            format!("get_share_object_seq_and_id: {}", name_key),
                        ),
                    ),
                ));
            }
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
            assert_table_exist(
                table_seq,
                &TableNameIdent {
                    tenant: tenant.to_string(),
                    db_name: db_name.clone(),
                    table_name: table_name.clone(),
                },
                format!("get_share_object_seq_and_id: {}", name_key),
            )?;

            let tbid = TableId { table_id };
            let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(kv_api, &tbid).await?;

            if table_meta_seq == 0 {
                return Err(KVAppError::AppError(AppError::UnknownTable(
                    UnknownTable::new(
                        &name_key.table_name,
                        format!("get_share_object_seq_and_id: {}", name_key),
                    ),
                )));
            }

            Ok(ShareGrantObjectSeqAndId::Table(
                db_id,
                table_meta_seq,
                table_id,
                table_meta.unwrap(),
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
        ShareGrantObjectSeqAndId::Table(_db_id, table_meta_seq, table_id, _) => {
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
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    match seq_and_id {
        ShareGrantObjectSeqAndId::Database(db_meta_seq, db_id, mut db_meta) => {
            // modify db_meta add share_id into shared_by
            if !db_meta.shared_by.contains(&share_id) {
                db_meta.shared_by.insert(share_id);
                let key = DatabaseId { db_id };
                if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
                condition.push(txn_cond_seq(&key, Eq, db_meta_seq));
            }
        }
        ShareGrantObjectSeqAndId::Table(_db_id, table_meta_seq, table_id, mut table_meta) => {
            // modify table_meta add share_id into shared_by
            if !table_meta.shared_by.contains(&share_id) {
                table_meta.shared_by.insert(share_id);
                let key = TableId { table_id };
                if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
            }
        }
    }

    Ok(())
}

async fn drop_accounts_granted_from_share(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    share_meta: &ShareMeta,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    // get all accounts seq from share_meta
    for account in share_meta.get_accounts() {
        let share_account_key = ShareAccountNameIdent {
            account: account.clone(),
            share_id,
        };
        let ret = get_share_account_meta_or_err(
            kv_api,
            &share_account_key,
            format!("drop_share's account: {}/{}", share_id, account),
        )
        .await;

        if let Ok((seq, _meta)) = ret {
            condition.push(txn_cond_seq(&share_account_key, Eq, seq));
            if_then.push(txn_op_del(&share_account_key));
        }
    }

    Ok(())
}

async fn remove_share_id_from_share_object(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    entry: &ShareGrantEntry,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    if let Ok((seq, mut share_ids)) = get_object_shared_by_share_ids(kv_api, &entry.object).await {
        share_ids.remove(share_id);

        condition.push(txn_cond_seq(&entry.object, Eq, seq));
        if_then.push(txn_op_put(&entry.object, serialize_struct(&share_ids)?));
    }
    Ok(())
}

async fn remove_share_id_from_share_objects(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    share_meta: &ShareMeta,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    if let Some(database) = &share_meta.database {
        remove_share_id_from_share_object(kv_api, share_id, database, condition, if_then).await?;
        if let ShareGrantObject::Database(db_id) = database.object {
            let key = DatabaseId { db_id };

            let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) =
                get_pb_value(kv_api, &key).await?;
            if let Some(mut db_meta) = db_meta {
                db_meta.shared_by.remove(&share_id);
                if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
                condition.push(txn_cond_seq(&key, Eq, db_meta_seq));
            }
        } else {
            unreachable!("share database MUST to be database object");
        }
    }

    for (_key, entry) in share_meta.entries.iter() {
        remove_share_id_from_share_object(kv_api, share_id, entry, condition, if_then).await?;
        if let ShareGrantObject::Table(table_id) = entry.object {
            let key = TableId { table_id };

            let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(kv_api, &key).await?;
            if let Some(mut table_meta) = table_meta {
                table_meta.shared_by.remove(&share_id);
                if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
            }
        } else {
            unreachable!("share entries MUST to be table object");
        }
    }

    Ok(())
}

async fn get_tenant_share_spec_vec(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: String,
) -> Result<Vec<ShareSpec>, KVAppError> {
    let mut share_metas = vec![];
    let share_name_list = ShareNameIdent {
        tenant,
        // Using a empty share to to list all
        share_name: "".to_string(),
    };

    let share_name_list_keys = list_keys(kv_api, &share_name_list).await?;
    for share_name in share_name_list_keys {
        let res = get_share_or_err(
            kv_api,
            &share_name,
            format!("get_tenant_share_spec_vec: {}", &share_name),
        )
        .await;

        let (_share_id_seq, share_id, _share_meta_seq, share_meta) = match res {
            Ok(x) => x,
            Err(e) => match e {
                KVAppError::AppError(AppError::UnknownShare(e)) => {
                    error!("{:?} when get_tenant_share_spec_vec", e);
                    continue;
                }
                KVAppError::AppError(AppError::UnknownShareId(_)) => {
                    error!("{:?} when get_tenant_share_spec_vec", e);
                    continue;
                }
                _ => {
                    return Err(e);
                }
            },
        };

        share_metas.push(
            convert_share_meta_to_spec(kv_api, &share_name.share_name, share_id, share_meta)
                .await?,
        );
    }

    Ok(share_metas)
}

#[allow(clippy::too_many_arguments)]
async fn revoke_object_privileges(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_meta: &mut ShareMeta,
    object: ShareGrantObject,
    share_id: u64,
    privileges: ShareGrantObjectPrivilege,
    update_on: DateTime<Utc>,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let key = object.to_string();

    match object {
        ShareGrantObject::Database(_db_id) => {
            if let Some(entry) = &mut share_meta.database {
                if object == entry.object {
                    if entry.revoke_privileges(privileges, update_on) {
                        // all database privileges have been revoked, clear database and entries.
                        share_meta.database = None;
                        share_meta.update_on = Some(update_on);

                        // clean all shared table `shared_by` field
                        for (_key, entry) in share_meta.entries.iter() {
                            if let ShareGrantObject::Table(table_id) = entry.object {
                                let key = TableId { table_id };

                                let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                                    get_pb_value(kv_api, &key).await?;
                                if let Some(mut table_meta) = table_meta {
                                    table_meta.shared_by.remove(&share_id);
                                    if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                                    condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                                }
                            } else {
                                unreachable!("share entries MUST to be table object");
                            }
                        }
                        share_meta.entries.clear();
                    }
                } else {
                    return Err(KVAppError::AppError(AppError::WrongShareObject(
                        WrongShareObject::new(&key),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::WrongShareObject(
                    WrongShareObject::new(object.to_string()),
                )));
            }
        }
        ShareGrantObject::Table(table_id) => match share_meta.entries.get_mut(&key) {
            Some(entry) => {
                if let ShareGrantObject::Table(self_table_id) = entry.object {
                    if self_table_id == table_id {
                        if entry.revoke_privileges(privileges, update_on) {
                            share_meta.entries.remove(&key);
                            // remove share id from table `shared_by` field
                            let key = TableId { table_id };

                            let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                                get_pb_value(kv_api, &key).await?;
                            if let Some(mut table_meta) = table_meta {
                                table_meta.shared_by.remove(&share_id);
                                if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                                condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                            }
                        }
                    } else {
                        return Err(KVAppError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(object.to_string()),
                        )));
                    }
                } else {
                    unreachable!("ShareMeta.entries MUST be Table Object");
                }
            }
            None => return Ok(()),
        },
    }
    Ok(())
}
