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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CannotShareDatabaseCreatedFromShare;
use databend_common_meta_app::app_error::ShareAccountsAlreadyExists;
use databend_common_meta_app::app_error::ShareAlreadyExists;
use databend_common_meta_app::app_error::ShareEndpointAlreadyExists;
use databend_common_meta_app::app_error::UnknownShareAccounts;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::WrongShareObject;
use databend_common_meta_app::app_error::WrongSharePrivileges;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::share::share_end_point_ident::ShareEndpointIdentRaw;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::share::*;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use enumflags2::BitFlags;
use fastrace::func_name;
use log::debug;

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
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::list_keys;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::util::get_share_endpoint_id_to_name_or_err;
use crate::util::get_share_endpoint_or_err;
use crate::util::get_table_info_by_share;
use crate::ShareApi;

/// ShareApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls ShareApi.
#[async_trait::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> ShareApi for KV {
    #[logcall::logcall]
    #[fastrace::trace]
    async fn show_shares(&self, req: ShowSharesReq) -> Result<ShowSharesReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        // Get all outbound share accounts.
        let outbound_accounts = get_outbound_share_infos_by_tenant(self, &req.tenant).await?;

        Ok(ShowSharesReply { outbound_accounts })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_share(&self, req: CreateShareReq) -> Result<CreateShareReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get share by name to ensure absence
            let (share_id_seq, share_id) = get_u64_value(self, name_key).await?;
            debug!(share_id_seq = share_id_seq, share_id = share_id, name_key :? =(name_key); "get_share");

            if share_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateShareReply {
                        share_id,
                        share_spec: None,
                    })
                } else {
                    Err(KVAppError::AppError(AppError::ShareAlreadyExists(
                        ShareAlreadyExists::new(
                            name_key.name(),
                            format!("create share: tenant: {}", name_key.tenant_name()),
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

            debug!(share_id = share_id, name_key :? =(name_key); "new share id");

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
                        txn_op_put(&id_to_name_key, serialize_struct(&name_key.to_raw())?), /* __fd_share_id_to_name/<share_id> -> (tenant,share_name) */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(name_key) ,
                    id :? =(&id_key),
                    succ = succ;
                    "create_share"
                );

                if succ {
                    return Ok(CreateShareReply {
                        share_id,
                        share_spec: Some(
                            convert_share_meta_to_spec(
                                self,
                                req.share_name.name(),
                                share_id,
                                share_meta,
                            )
                            .await?,
                        ),
                    });
                }
            }
        }
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
    #[logcall::logcall]
    async fn drop_share(&self, req: DropShareReq) -> Result<DropShareReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_share_or_err(
                self,
                name_key,
                format!("drop_share: {}", name_key.display()),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {
                                share_id: None,
                                share_spec: None,
                            });
                        }
                    }

                    return Err(e);
                }
            };

            let res = get_share_id_to_name_or_err(
                self,
                share_id,
                format!("drop_share: {}", name_key.display()),
            )
            .await;
            let (share_name_seq, _share_name) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShareId(_)) = e {
                        if req.if_exists {
                            return Ok(DropShareReply {
                                share_id: Some(share_id),
                                share_spec: None,
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

            debug!(share_id = share_id, name_key :? =(name_key); "drop_share");

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
                    name :? =(name_key) ,
                    id :? =(&share_id_key),
                    succ = succ;
                    "drop_share"
                );

                if succ {
                    return Ok(DropShareReply {
                        share_id: Some(share_id),
                        share_spec: Some(
                            convert_share_meta_to_spec(
                                self,
                                req.share_name.name(),
                                share_id,
                                share_meta,
                            )
                            .await?,
                        ),
                    });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn add_share_tenants(
        &self,
        req: AddShareAccountsReq,
    ) -> Result<AddShareAccountsReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_share_or_err(
                self,
                name_key,
                format!("add_share_tenants: {}", name_key.display()),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(AddShareAccountsReply {
                                share_id: None,
                                share_spec: None,
                            });
                        }
                    }
                    return Err(e);
                }
            };

            let mut add_share_account_keys = vec![];
            for account in req.accounts.iter() {
                if !share_meta.has_account(account) {
                    add_share_account_keys.push(ShareConsumerIdent::new(
                        Tenant::new_or_err(account, "add_share_tenants")?,
                        share_id,
                    ));
                }
            }
            if add_share_account_keys.is_empty() {
                return Err(KVAppError::AppError(AppError::ShareAccountsAlreadyExists(
                    ShareAccountsAlreadyExists::new(
                        req.share_name.name(),
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
                        share_account_key.tenant_name().to_string(),
                        share_id,
                        req.share_on,
                    );

                    if_then.push(txn_op_put(
                        share_account_key,
                        serialize_struct(&share_account_meta)?,
                    )); /* (account, share_id) -> share_account_meta */

                    share_meta.add_account(share_account_key.tenant_name().to_string());
                }
                if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?)); /* (share_id) -> share_meta */

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(name_key) ,
                    id :? =(&id_key),
                    succ = succ;
                    "add_share_tenants"
                );

                if succ {
                    return Ok(AddShareAccountsReply {
                        share_id: Some(share_id),
                        share_spec: Some(
                            convert_share_meta_to_spec(
                                self,
                                req.share_name.name(),
                                share_id,
                                share_meta,
                            )
                            .await?,
                        ),
                    });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn remove_share_tenants(
        &self,
        req: RemoveShareAccountsReq,
    ) -> Result<RemoveShareAccountsReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_share_or_err(
                self,
                name_key,
                format!("remove_share_tenants: {}", name_key.display()),
            )
            .await;

            let (_share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownShare(_)) = e {
                        if req.if_exists {
                            return Ok(RemoveShareAccountsReply {
                                share_id: None,
                                share_spec: None,
                            });
                        }
                    }
                    return Err(e);
                }
            };

            let mut remove_share_account_keys_and_seqs = vec![];
            for account in req.accounts.iter() {
                if account == name_key.tenant_name() {
                    continue;
                }
                if share_meta.has_account(account) {
                    let share_account_key = ShareConsumerIdent::new(
                        Tenant::new_or_err(account, "remove_share_tenants")?,
                        share_id,
                    );

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

                    share_meta.del_account(share_account_key_and_seq.0.tenant_name());
                }
                if_then.push(txn_op_put(&id_key, serialize_struct(&share_meta)?)); /* (share_id) -> share_meta */

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    id :? =(&id_key),
                    succ = succ;
                    "remove_share_tenants"
                );

                if succ {
                    return Ok(RemoveShareAccountsReply {
                        share_id: Some(share_id),
                        share_spec: Some(
                            convert_share_meta_to_spec(
                                self,
                                req.share_name.name(),
                                share_id,
                                share_meta,
                            )
                            .await?,
                        ),
                    });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn grant_share_object(
        &self,
        req: GrantShareObjectReq,
    ) -> Result<GrantShareObjectReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        check_share_privilege(&req.object, req.privilege)?;

        let share_name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_share_or_err(
                self,
                share_name_key,
                format!("grant_share_object: {}", share_name_key.display()),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let (seq_and_id, object) =
                get_share_object_seq_and_id(self, &req.object, share_name_key.tenant(), true)
                    .await?;

            check_share_object(&share_meta, &seq_and_id, &req.object, req.privilege)?;

            // Check the object privilege has been granted
            let has_granted_privileges =
                share_meta.has_granted_privileges(&object, req.privilege)?;

            if has_granted_privileges {
                return Ok(GrantShareObjectReply {
                    share_id,
                    share_spec: None,
                    grant_share_table: None,
                    reference_tables: None,
                });
            }

            // check if has access rights of all reference_tables
            let reference_table_info = if let Some(reference_tables) = &req.reference_tables {
                let view_id = if let ShareObject::View(_, _, table_id) = &object {
                    *table_id
                } else {
                    unreachable!()
                };
                check_access_reference_tables(
                    self,
                    view_id,
                    reference_tables,
                    &mut share_meta,
                    share_name_key.tenant_name(),
                    req.grant_on,
                )
                .await?
            } else {
                vec![]
            };

            // Grant the object privilege by inserting these record:
            // add privilege and upsert (share_id) -> share_meta
            // if grant database then update db_meta.shared_on and upsert (db_id) -> db_meta

            // Grant the object privilege by transaction.
            {
                let id_key = ShareId { share_id };

                // modify the share_meta add privilege
                // modify share_ids
                let res = get_object_shared_by_share_ids(self, &object).await?;
                let share_ids_seq = res.0;
                let mut share_ids: ObjectSharedByShareIds = res.1;
                share_ids.add(share_id);

                let view_reference_table: BTreeSet<u64> = reference_table_info
                    .iter()
                    .map(|(_, table_id, _db_id, _table_info)| *table_id)
                    .clone()
                    .collect();
                share_meta.grant_object_privileges(
                    &object,
                    req.privilege,
                    req.grant_on,
                    view_reference_table,
                )?;
                let grant_object = ShareGrantObject::new(&object);

                // condition
                let mut condition: Vec<TxnCondition> = vec![];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![];
                // Some database has been created before `DatabaseIdToName`, so create it if need.
                create_db_name_to_id_key_if_need(
                    self,
                    &seq_and_id,
                    share_name_key.tenant(),
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
                // add share_id into reference table `shared_by` field
                for (_table_name, db_id, table_id, _table_info) in &reference_table_info {
                    let table_id = *table_id;
                    let tbid = TableId { table_id };

                    let (tb_meta_seq, table_meta_opt): (_, Option<TableMeta>) =
                        get_pb_value(self, &tbid).await?;
                    if let Some(table_meta) = table_meta_opt {
                        let table_seq_id = ShareGrantObjectSeqAndId::Table(
                            *db_id,
                            tb_meta_seq,
                            table_id,
                            table_meta,
                        );

                        add_grant_object_txn_if_then(
                            share_id,
                            table_seq_id,
                            &mut condition,
                            &mut if_then,
                        )?;
                    }
                }

                condition.extend(vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                    txn_cond_seq(&grant_object, Eq, share_ids_seq),
                ]);
                if_then.extend(vec![
                    txn_op_put(&id_key, serialize_struct(&share_meta)?), /* (share_id) -> share_meta */
                    txn_op_put(&grant_object, serialize_struct(&share_ids)?), /* (object) -> share_ids */
                ]);

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(share_name_key),
                    id :? =(&id_key),
                    succ = succ;
                    "grant_share_object"
                );

                if succ {
                    let grant_share_table = match seq_and_id {
                        ShareGrantObjectSeqAndId::Database(..) => None,
                        ShareGrantObjectSeqAndId::Table(
                            db_id,
                            _table_meta_seq,
                            table_id,
                            _table_meta,
                        ) => {
                            let (_, share_table_info) = get_table_info_by_share(
                                self,
                                Some(table_id),
                                share_name_key,
                                &share_meta,
                            )
                            .await?;
                            Some((db_id, share_table_info[0].clone()))
                        }

                        ShareGrantObjectSeqAndId::View(
                            _db_name,
                            db_id,
                            _table_meta_seq,
                            table_id,
                            _table_meta,
                        ) => {
                            let (_, share_table_info) = get_table_info_by_share(
                                self,
                                Some(table_id),
                                share_name_key,
                                &share_meta,
                            )
                            .await?;
                            Some((db_id, share_table_info[0].clone()))
                        }
                    };
                    let share_spec = convert_share_meta_to_spec(
                        self,
                        share_name_key.name(),
                        share_id,
                        share_meta,
                    )
                    .await?;

                    return Ok(GrantShareObjectReply {
                        share_id,
                        share_spec: Some(share_spec),
                        grant_share_table,
                        reference_tables: if reference_table_info.is_empty() {
                            None
                        } else {
                            Some(
                                reference_table_info
                                    .into_iter()
                                    .map(|(_, _, db_id, table_info)| (db_id, table_info))
                                    .collect(),
                            )
                        },
                    });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn revoke_share_object(
        &self,
        req: RevokeShareObjectReq,
    ) -> Result<RevokeShareObjectReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        check_share_privilege(&req.object, req.privilege)?;
        let share_name_key = &req.share_name;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_share_or_err(
                self,
                share_name_key,
                format!("revoke_share_object: {}", share_name_key.display()),
            )
            .await;

            let (share_id_seq, share_id, share_meta_seq, mut share_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

            let (seq_and_id, object) =
                get_share_object_seq_and_id(self, &req.object, share_name_key.tenant(), false)
                    .await?;

            check_share_object(&share_meta, &seq_and_id, &req.object, req.privilege)?;

            // Check the object privilege has not been granted.
            let has_granted_privileges =
                share_meta.has_granted_privileges(&object, req.privilege)?;

            if !has_granted_privileges {
                return Ok(RevokeShareObjectReply {
                    share_id,
                    share_spec: None,
                    revoke_object: None,
                });
            }

            // Revoke the object privilege by upserting these record:
            // revoke privilege in share_meta and upsert (share_id) -> share_meta
            // update {db_meta|table_meta}.shared_by and upsert (db_id|table_id) -> (db_meta|table_id)

            // Revoke the object privilege by transaction.
            {
                let id_key = ShareId { share_id };
                // modify the share_meta add privilege
                // modify share_ids
                let res = get_object_shared_by_share_ids(self, &object).await?;
                let share_ids_seq = res.0;
                let mut share_ids: ObjectSharedByShareIds = res.1;
                share_ids.remove(share_id);

                let grant_object = ShareGrantObject::new(&object);
                // condition
                let mut condition: Vec<TxnCondition> = vec![
                    txn_cond_seq(share_name_key, Eq, share_id_seq),
                    txn_cond_seq(&id_key, Eq, share_meta_seq),
                    txn_cond_seq(&grant_object, Eq, share_ids_seq),
                ];
                add_txn_condition(&seq_and_id, &mut condition);
                // if_then
                let mut if_then = vec![
                    txn_op_put(&grant_object, serialize_struct(&share_ids)?), /* (object) -> share_ids */
                ];

                // construct the revoke_object before modify share meta
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
                    ShareGrantObjectSeqAndId::View(
                        _view_name,
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

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(share_name_key),
                    id :? =(&id_key),
                    succ = succ;
                    "revoke_share_object"
                );

                let share_spec =
                    convert_share_meta_to_spec(self, share_name_key.name(), share_id, share_meta)
                        .await?;
                if succ {
                    return Ok(RevokeShareObjectReply {
                        share_id,
                        share_spec: Some(share_spec),
                        revoke_object: Some(object.into()),
                    });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> Result<GetShareGrantObjectReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let share_name_key = &req.share_name;

        let res = get_share_or_err(
            self,
            share_name_key,
            format!("revoke_share_object: {}", share_name_key.display()),
        )
        .await;

        let (_share_id_seq, _share_id, _share_meta_seq, share_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        if let Some(use_database) = share_meta.use_database {
            let database_name = use_database.db_name.clone();

            let mut objects = vec![];
            objects.push(ShareGrantReplyObject {
                object: ShareGrantObjectName::Database(database_name.clone()),
                privileges: use_database.privileges,
                grant_on: use_database.grant_on,
            });
            for table in &share_meta.table {
                objects.push(ShareGrantReplyObject {
                    object: ShareGrantObjectName::Table(
                        database_name.clone(),
                        table.table_name.clone(),
                    ),
                    privileges: table.privileges,
                    grant_on: use_database.grant_on,
                });
            }

            Ok(GetShareGrantObjectReply {
                share_name: req.share_name,
                objects,
            })
        } else {
            return Ok(GetShareGrantObjectReply {
                share_name: req.share_name,
                objects: vec![],
            });
        }
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
        let mut privileges = vec![];

        match req.object {
            ShareGrantObjectName::Database(db_name) => {
                let db_name_key = DatabaseNameIdent::new(&req.tenant, db_name.clone());
                let (db_seq, db_id) = get_u64_value(self, &db_name_key).await?;
                db_has_to_exist(
                    db_seq,
                    &db_name_key,
                    format!("get_grant_privileges_of_object: {}", db_name_key.display()),
                )?;
                let object = ShareObject::Database(db_name.clone(), db_id);
                let (_seq, share_ids) = get_object_shared_by_share_ids(self, &object).await?;
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
                    if let Some(use_database) = share_meta.use_database {
                        if use_database.db_name == db_name {
                            privileges.push(ObjectGrantPrivilege {
                                share_name: share_name.share_name().to_string(),
                                privileges: use_database.privileges,
                                grant_on: use_database.grant_on,
                            });
                        }
                    }
                }
            }
            ShareGrantObjectName::Table(db_name, table_name) => {
                let db_name_key = DatabaseNameIdent::new(&req.tenant, &db_name);
                let (db_seq, db_id) = get_u64_value(self, &db_name_key).await?;
                db_has_to_exist(
                    db_seq,
                    &db_name_key,
                    format!("get_grant_privileges_of_object: {}", db_name_key.display()),
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
                        table_name: table_name.clone(),
                    },
                    format!("get_grant_privileges_of_object: {}", table_name_key),
                )?;

                let object = ShareObject::Table(table_name, db_id, table_id);
                let (_seq, share_ids) = get_object_shared_by_share_ids(self, &object).await?;
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
                    for table in &share_meta.table {
                        if table.db_id != db_id || table.table_id != table_id {
                            continue;
                        }
                        privileges.push(ObjectGrantPrivilege {
                            share_name: share_name.share_name().to_string(),
                            privileges: table.privileges,
                            grant_on: table.grant_on,
                        });
                    }
                }
            }

            ShareGrantObjectName::View(db_name, table_name) => {
                let db_name_key = DatabaseNameIdent::new(&req.tenant, &db_name);
                let (db_seq, db_id) = get_u64_value(self, &db_name_key).await?;
                db_has_to_exist(
                    db_seq,
                    &db_name_key,
                    format!("get_grant_privileges_of_object: {}", db_name_key.display()),
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
                        table_name: table_name.clone(),
                    },
                    format!("get_grant_privileges_of_object: {}", table_name_key),
                )?;

                let object = ShareObject::View(table_name, db_id, table_id);
                let (_seq, share_ids) = get_object_shared_by_share_ids(self, &object).await?;
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
                    for table in &share_meta.table {
                        if table.db_id != db_id || table.table_id != table_id {
                            continue;
                        }
                        privileges.push(ObjectGrantPrivilege {
                            share_name: share_name.share_name().to_string(),
                            privileges: table.privileges,
                            grant_on: table.grant_on,
                        });
                    }
                }
            }
        }

        Ok(GetObjectGrantPrivilegesReply { privileges })
    }

    #[logcall::logcall]
    async fn create_share_endpoint(
        &self,
        req: CreateShareEndpointReq,
    ) -> Result<CreateShareEndpointReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get share endpoint by name to ensure absence
            let (share_endpoint_id_seq, share_endpoint_id) = get_u64_value(self, name_key).await?;
            debug!(
                share_endpoint_id_seq = share_endpoint_id_seq,
                share_endpoint_id = share_endpoint_id,
                name_key :? =(name_key);
                "create_share_endpoint"
            );

            let mut condition = vec![];
            let mut if_then = vec![];

            if share_endpoint_id_seq > 0 {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(KVAppError::AppError(AppError::ShareEndpointAlreadyExists(
                            ShareEndpointAlreadyExists::new(name_key.name(), func_name!()),
                        )));
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateShareEndpointReply { share_endpoint_id });
                    }
                    CreateOption::CreateOrReplace => {
                        construct_drop_share_endpoint_txn_operations(
                            self,
                            name_key,
                            true,
                            false,
                            func_name!(),
                            &mut condition,
                            &mut if_then,
                        )
                        .await?;
                    }
                }
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
                name_key :? =(name_key);
                "new share endpoint id"
            );

            // Create share endpoint by transaction.
            {
                let share_endpoint_meta = ShareEndpointMeta::new(&req);
                condition.extend(vec![
                    txn_cond_seq(name_key, Eq, share_endpoint_id_seq),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                ]);
                if_then.extend(vec![
                        txn_op_put(name_key, serialize_u64(share_endpoint_id)?), /* (tenant, share_endpoint_name) -> share_endpoint_id */
                        txn_op_put(&id_key, serialize_struct(&share_endpoint_meta)?), /* (share_endpoint_id) -> share_endpoint_meta */
                        txn_op_put(&id_to_name_key, serialize_struct(&ShareEndpointIdentRaw::from(name_key.clone()))?), /* __fd_share_endpoint_id_to_name/<share_endpoint_id> -> (tenant,share_endpoint_name) */
                    ]);

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(name_key) ,
                    id :? =(&id_key),
                    succ = succ;
                    "create_share_endpoint"
                );

                if succ {
                    return Ok(CreateShareEndpointReply { share_endpoint_id });
                }
            }
        }
    }

    #[logcall::logcall]
    async fn upsert_share_endpoint(
        &self,
        req: UpsertShareEndpointReq,
    ) -> Result<UpsertShareEndpointReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get share endpoint by name to ensure absence
            let (share_endpoint_id_seq, share_endpoint_id) = get_u64_value(self, name_key).await?;
            debug!(
                share_endpoint_id_seq = share_endpoint_id_seq,
                share_endpoint_id = share_endpoint_id,
                name_key :? =(name_key);
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
                name_key :? =(name_key);
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
                    if_then.push(txn_op_put(
                        &id_to_name_key,
                        serialize_struct(&ShareEndpointIdentRaw::from(name_key.clone()))?,
                    ));
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
                    name :? =(name_key) ,
                    succ = succ;
                    "upsert_share_endpoint"
                );

                if succ {
                    return Ok(UpsertShareEndpointReply { share_endpoint_id });
                }
            }
        }
    }

    async fn get_share_endpoint(
        &self,
        req: GetShareEndpointReq,
    ) -> Result<GetShareEndpointReply, KVAppError> {
        let mut share_endpoint_meta_vec = vec![];

        let idents = {
            if let Some(endpoint) = &req.endpoint {
                vec![ShareEndpointIdent::new(&req.tenant, endpoint.clone())]
            } else {
                let ident = ShareEndpointIdent::new(&req.tenant, "dummy");

                let dir_name = DirName::new(ident);

                list_keys(self, &dir_name).await?
            }
        };

        for share_endpoint in idents {
            let (_seq, share_endpoint_id) = get_u64_value(self, &share_endpoint).await?;
            let id_key = ShareEndpointId { share_endpoint_id };
            let (_seq, share_endpoint_meta): (u64, Option<ShareEndpointMeta>) =
                get_pb_value(self, &id_key).await?;

            if let Some(share_endpoint_meta) = share_endpoint_meta {
                share_endpoint_meta_vec.push((share_endpoint, share_endpoint_meta));
            }
        }

        Ok(GetShareEndpointReply {
            share_endpoint_meta_vec,
        })
    }

    #[logcall::logcall]
    async fn drop_share_endpoint(
        &self,
        req: DropShareEndpointReq,
    ) -> Result<DropShareEndpointReply, KVAppError> {
        debug!(req :? =(&req); "ShareApi: {}", func_name!());

        let name_key = &req.endpoint;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut condition = vec![];
            let mut if_then = vec![];

            let share_endpoint_id = if let Some(share_endpoint_id) =
                construct_drop_share_endpoint_txn_operations(
                    self,
                    name_key,
                    req.if_exists,
                    true,
                    func_name!(),
                    &mut condition,
                    &mut if_then,
                )
                .await?
            {
                share_endpoint_id
            } else {
                return Ok(DropShareEndpointReply {});
            };

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            let share_id_key = ShareEndpointId { share_endpoint_id };
            debug!(
                name :? =(name_key) ,
                id :? =(&share_id_key),
                succ = succ;
                "drop_share_endpoint"
            );

            if succ {
                return Ok(DropShareEndpointReply {});
            }
        }
    }
}

async fn construct_drop_share_endpoint_txn_operations(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareEndpointIdent,
    drop_if_exists: bool,
    if_delete: bool,
    ctx: &str,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<Option<u64>, KVAppError> {
    let res = get_share_endpoint_or_err(
        kv_api,
        name_key,
        format!(
            "construct_drop_share_endpoint_txn_operations: {}",
            name_key.display()
        ),
    )
    .await;

    let (share_endpoint_id_seq, share_endpoint_id, share_endpoint_meta_seq, _share_endpoint_meta) =
        match res {
            Ok(x) => x,
            Err(e) => {
                if let KVAppError::AppError(AppError::UnknownShareEndpoint(_)) = e {
                    if drop_if_exists {
                        return Ok(None);
                    }
                }

                return Err(e);
            }
        };
    let (share_endpoint_name_seq, _share_endpoint) = get_share_endpoint_id_to_name_or_err(
        kv_api,
        share_endpoint_id,
        format!(
            "construct_drop_share_endpoint_txn_operations: {}",
            name_key.display()
        ),
    )
    .await?;

    // Delete share endpoint by these operations:
    // del (tenant, share_endpoint)
    // del share_endpoint_id
    // del (share_endpoint_id) -> (tenant, share_endpoint)
    let share_id_key = ShareEndpointId { share_endpoint_id };
    let id_name_key = ShareEndpointIdToName { share_endpoint_id };

    debug!(
        share_endpoint_id = share_endpoint_id,
        name_key :? =(name_key),
        ctx = ctx;
        "construct_drop_share_endpoint_txn_operations"
    );

    condition.push(txn_cond_seq(&share_id_key, Eq, share_endpoint_meta_seq));
    condition.push(txn_cond_seq(&id_name_key, Eq, share_endpoint_name_seq));
    if_then.push(txn_op_del(&share_id_key)); // del share_endpoint_id
    if_then.push(txn_op_del(&id_name_key)); // del (share_endpoint_id) -> (tenant, share_endpoint)
    if if_delete {
        condition.push(txn_cond_seq(name_key, Eq, share_endpoint_id_seq));
        if_then.push(txn_op_del(name_key)); // del (tenant, share_endpoint)
    }

    Ok(Some(share_endpoint_id))
}

async fn get_share_use_database_name(share_meta: &ShareMeta) -> Result<Option<String>, KVAppError> {
    if let Some(use_database) = &share_meta.use_database {
        Ok(Some(use_database.db_name.clone()))
    } else {
        Ok(None)
    }
}

async fn get_outbound_share_tenants_by_name(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_name: &ShareNameIdent,
) -> Result<Vec<GetShareGrantTenants>, KVAppError> {
    let res = get_share_or_err(
        kv_api,
        share_name,
        format!("get_share: {}", share_name.display()),
    )
    .await?;
    let (_share_id_seq, share_id, _share_meta_seq, share_meta) = res;

    let mut accounts = vec![];
    for account in share_meta.get_accounts() {
        let share_account_key = ShareConsumerIdent::new(
            Tenant::new_or_err(&account, "get_outbound_share_tenants_by_name")?,
            share_id,
        );

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
        format!("get_share: {}", share_name.display()),
    )
    .await?;
    let (_share_id_seq, _share_id, _share_meta_seq, share_meta) = res;

    let mut accounts = vec![];
    for account in share_meta.get_accounts().iter() {
        accounts.push(account.clone());
    }

    let database_name = get_share_use_database_name(&share_meta).await?;

    Ok(ShareAccountReply {
        share_name: share_name.clone(),
        database_name,
        create_on: share_meta.create_on,
        accounts: Some(accounts),
        comment: share_meta.comment.clone(),
    })
}

async fn get_outbound_share_infos_by_tenant(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
) -> Result<Vec<ShareAccountReply>, KVAppError> {
    let mut outbound_share_accounts: Vec<ShareAccountReply> = vec![];

    let tenant_share_name_key = ShareNameIdent::new(tenant, "dummy");

    let dir_name = DirName::new(tenant_share_name_key);

    let share_name_keys = list_keys(kv_api, &dir_name).await?;

    for share_name in share_name_keys {
        let reply = get_outbound_share_info_by_name(kv_api, &share_name).await;
        if let Ok(reply) = reply {
            outbound_share_accounts.push(reply)
        }
    }

    Ok(outbound_share_accounts)
}

async fn create_db_name_to_id_key_if_need(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    seq_and_id: &ShareGrantObjectSeqAndId,
    tenant: &Tenant,
    obj_name: &ShareGrantObjectName,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    if let ShareGrantObjectName::Database(db) = obj_name {
        if let ShareGrantObjectSeqAndId::Database(db_id, _, _) = seq_and_id {
            let db_id_key = DatabaseIdToName { db_id: *db_id };
            let (db_name_seq, _): (_, Option<DatabaseNameIdentRaw>) =
                get_pb_value(kv_api, &db_id_key).await?;
            if db_name_seq == 0 {
                condition.push(txn_cond_seq(&db_id_key, Eq, 0));

                let name_key = DatabaseNameIdentRaw::new(tenant.tenant_name(), db);
                if_then.push(txn_op_put(&db_id_key, serialize_struct(&name_key)?));
            }
        }
    };
    Ok(())
}

fn check_share_privilege(
    obj_name: &ShareGrantObjectName,
    privileges: ShareGrantObjectPrivilege,
) -> Result<(), KVAppError> {
    match obj_name {
        ShareGrantObjectName::Database(_db_name) => {
            // legal grant database privileges: usage/reference
            if privileges != ShareGrantObjectPrivilege::Usage
                && privileges != ShareGrantObjectPrivilege::ReferenceUsage
            {
                return Err(KVAppError::AppError(AppError::WrongSharePrivileges(
                    WrongSharePrivileges::new(obj_name.to_string()),
                )));
            }
        }
        ShareGrantObjectName::Table(_, _) | ShareGrantObjectName::View(_, _) => {
            // legal grant table privileges: select
            if privileges != ShareGrantObjectPrivilege::Select {
                return Err(KVAppError::AppError(AppError::WrongSharePrivileges(
                    WrongSharePrivileges::new(obj_name.to_string()),
                )));
            }
        }
    }
    Ok(())
}

fn check_share_object(
    share_meta: &ShareMeta,
    seq_and_id: &ShareGrantObjectSeqAndId,
    obj_name: &ShareGrantObjectName,
    privileges: ShareGrantObjectPrivilege,
) -> Result<(), KVAppError> {
    let object_db_id = match seq_and_id {
        ShareGrantObjectSeqAndId::Database(_, db_id, _) => *db_id,
        ShareGrantObjectSeqAndId::Table(db_id, _seq, _id, _meta) => *db_id,
        ShareGrantObjectSeqAndId::View(_, db_id, _seq, _id, _meta) => *db_id,
    };

    match obj_name {
        ShareGrantObjectName::Database(_db_name) => match privileges {
            ShareGrantObjectPrivilege::Usage => {
                if let Some(db) = &share_meta.use_database {
                    if db.db_id != object_db_id {
                        return Err(KVAppError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(obj_name.to_string()),
                        )));
                    }
                }
            }
            ShareGrantObjectPrivilege::ReferenceUsage => {}
            _ => {
                return Err(KVAppError::AppError(AppError::WrongSharePrivileges(
                    WrongSharePrivileges::new(obj_name.to_string()),
                )));
            }
        },
        ShareGrantObjectName::Table(_, _) => {
            match privileges {
                ShareGrantObjectPrivilege::Select => {
                    if let Some(db) = &share_meta.use_database {
                        if db.db_id != object_db_id {
                            return Err(KVAppError::AppError(AppError::WrongShareObject(
                                WrongShareObject::new(obj_name.to_string()),
                            )));
                        }
                    } else {
                        // Table cannot be granted without database has been granted.
                        return Err(KVAppError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(obj_name.to_string()),
                        )));
                    }
                }
                ShareGrantObjectPrivilege::ReferenceUsage => {
                    for db in &share_meta.reference_database {
                        if db.db_id == object_db_id {
                            return Ok(());
                        }
                    }

                    return Err(KVAppError::AppError(AppError::WrongShareObject(
                        WrongShareObject::new(obj_name.to_string()),
                    )));
                }
                _ => {
                    return Err(KVAppError::AppError(AppError::WrongSharePrivileges(
                        WrongSharePrivileges::new(obj_name.to_string()),
                    )));
                }
            }
        }

        ShareGrantObjectName::View(_, _) => {
            match privileges {
                ShareGrantObjectPrivilege::Select => {
                    if let Some(db) = &share_meta.use_database {
                        if db.db_id != object_db_id {
                            return Err(KVAppError::AppError(AppError::WrongShareObject(
                                WrongShareObject::new(obj_name.to_string()),
                            )));
                        }
                    } else {
                        // Table cannot be granted without database has been granted.
                        return Err(KVAppError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(obj_name.to_string()),
                        )));
                    }
                }
                _ => {
                    return Err(KVAppError::AppError(AppError::WrongSharePrivileges(
                        WrongSharePrivileges::new(obj_name.to_string()),
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Returns ShareGrantObjectSeqAndId by ShareGrantObjectName
async fn get_share_object_seq_and_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    obj_name: &ShareGrantObjectName,
    tenant: &Tenant,
    grant: bool,
) -> Result<(ShareGrantObjectSeqAndId, ShareObject), KVAppError> {
    match obj_name {
        ShareGrantObjectName::Database(db_name) => {
            let name_key = DatabaseNameIdent::new(tenant.clone(), db_name);
            let (_db_id_seq, db_id, db_meta_seq, db_meta) = get_db_or_err(
                kv_api,
                &name_key,
                format!("get_share_object_seq_and_id: {}", name_key.display()),
            )
            .await?;

            if grant && db_meta.from_share.is_some() {
                return Err(KVAppError::AppError(
                    AppError::CannotShareDatabaseCreatedFromShare(
                        CannotShareDatabaseCreatedFromShare::new(
                            db_name,
                            format!("get_share_object_seq_and_id: {}", name_key.display()),
                        ),
                    ),
                ));
            }
            Ok((
                ShareGrantObjectSeqAndId::Database(db_meta_seq, db_id, db_meta),
                ShareObject::Database(db_name.to_owned(), db_id),
            ))
        }

        ShareGrantObjectName::Table(db_name, table_name) => {
            let db_name_key = DatabaseNameIdent::new(tenant, db_name);
            let (db_seq, db_id) = get_u64_value(kv_api, &db_name_key).await?;
            db_has_to_exist(
                db_seq,
                &db_name_key,
                format!("get_share_object_seq_and_id: {}", db_name_key.display()),
            )?;

            let name_key = DBIdTableName {
                db_id,
                table_name: table_name.clone(),
            };

            let (table_seq, table_id) = get_u64_value(kv_api, &name_key).await?;
            assert_table_exist(
                table_seq,
                &TableNameIdent {
                    tenant: tenant.clone(),
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

            Ok((
                ShareGrantObjectSeqAndId::Table(
                    db_id,
                    table_meta_seq,
                    table_id,
                    table_meta.unwrap(),
                ),
                ShareObject::Table(table_name.to_owned(), db_id, table_id),
            ))
        }

        ShareGrantObjectName::View(db_name, table_name) => {
            let db_name_key = DatabaseNameIdent::new(tenant, db_name);
            let (db_seq, db_id) = get_u64_value(kv_api, &db_name_key).await?;
            db_has_to_exist(
                db_seq,
                &db_name_key,
                format!("get_share_object_seq_and_id: {}", db_name_key.display()),
            )?;

            let name_key = DBIdTableName {
                db_id,
                table_name: table_name.clone(),
            };

            let (table_seq, table_id) = get_u64_value(kv_api, &name_key).await?;
            assert_table_exist(
                table_seq,
                &TableNameIdent {
                    tenant: tenant.clone(),
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

            Ok((
                ShareGrantObjectSeqAndId::View(
                    table_name.to_owned(),
                    db_id,
                    table_meta_seq,
                    table_id,
                    table_meta.unwrap(),
                ),
                ShareObject::View(table_name.to_owned(), db_id, table_id),
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
        ShareGrantObjectSeqAndId::View(_table_name, _db_id, table_meta_seq, table_id, _) => {
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
        ShareGrantObjectSeqAndId::View(_, _db_id, table_meta_seq, table_id, mut table_meta) => {
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
        let share_account_key = ShareConsumerIdent::new(
            Tenant::new_or_err(&account, "drop_accounts_granted_from_share")?,
            share_id,
        );
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

async fn remove_share_id_from_share_database(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    database: &ShareDatabase,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let object = ShareObject::Database(database.db_name.to_owned(), database.db_id);
    if let Ok((seq, mut share_ids)) = get_object_shared_by_share_ids(kv_api, &object).await {
        share_ids.remove(share_id);

        let object = ShareGrantObject::Database(database.db_id);
        condition.push(txn_cond_seq(&object, Eq, seq));
        if_then.push(txn_op_put(&object, serialize_struct(&share_ids)?));
    }
    Ok(())
}

async fn remove_share_id_from_share_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    table: &ShareTable,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let object = ShareObject::Table(table.table_name.clone(), table.db_id, table.table_id);
    if let Ok((seq, mut share_ids)) = get_object_shared_by_share_ids(kv_api, &object).await {
        share_ids.remove(share_id);

        let object = if table.engine == "VIEW" {
            ShareGrantObject::View(table.table_id)
        } else {
            ShareGrantObject::Table(table.table_id)
        };
        condition.push(txn_cond_seq(&object, Eq, seq));
        if_then.push(txn_op_put(&object, serialize_struct(&share_ids)?));
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
    let mut databases = share_meta.reference_database.clone();
    if let Some(database) = &share_meta.use_database {
        databases.push(database.to_owned());
    }
    for database in &databases {
        remove_share_id_from_share_database(kv_api, share_id, database, condition, if_then).await?;

        let key = DatabaseId {
            db_id: database.db_id,
        };

        let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) = get_pb_value(kv_api, &key).await?;
        if let Some(mut db_meta) = db_meta {
            db_meta.shared_by.remove(&share_id);
            if_then.push(txn_op_put(&key, serialize_struct(&db_meta)?));
            condition.push(txn_cond_seq(&key, Eq, db_meta_seq));
        }
    }

    for table in &share_meta.table {
        remove_share_id_from_share_table(kv_api, share_id, table, condition, if_then).await?;
        let key = TableId {
            table_id: table.table_id,
        };

        let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
            get_pb_value(kv_api, &key).await?;
        if let Some(mut table_meta) = table_meta {
            table_meta.shared_by.remove(&share_id);
            if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
            condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn revoke_object_privileges(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_meta: &mut ShareMeta,
    object: ShareObject,
    share_id: u64,
    privileges: ShareGrantObjectPrivilege,
    update_on: DateTime<Utc>,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let key = object.to_string();

    match object {
        ShareObject::Database(_db_name, db_id) => {
            match privileges {
                ShareGrantObjectPrivilege::Usage => {
                    if let Some(use_database) = &mut share_meta.use_database {
                        if use_database.db_id == db_id {
                            use_database.revoke_object_privileges(privileges);
                            share_meta.use_database = None;
                            share_meta.update_on = update_on;

                            // clean all shared table `shared_by` field
                            for table in &share_meta.table {
                                if table.db_id != db_id {
                                    continue;
                                }
                                let table_id = table.table_id;
                                let key = TableId { table_id };

                                let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                                    get_pb_value(kv_api, &key).await?;
                                if let Some(mut table_meta) = table_meta {
                                    table_meta.shared_by.remove(&share_id);
                                    if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                                    condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                                }
                            }

                            share_meta.table.clear();
                        } else {
                            return Err(KVAppError::AppError(AppError::WrongShareObject(
                                WrongShareObject::new(&key),
                            )));
                        }
                    } else {
                        return Err(KVAppError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(&key),
                        )));
                    }
                }
                ShareGrantObjectPrivilege::ReferenceUsage => {
                    for db in &mut share_meta.reference_database {
                        if db.db_id != db_id {
                            continue;
                        }

                        db.revoke_object_privileges(privileges);
                        // clean all shared table `shared_by` field
                        for table in &share_meta.reference_table {
                            if table.db_id != db_id {
                                continue;
                            }
                            let table_id = table.table_id;
                            let key = TableId { table_id };

                            let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                                get_pb_value(kv_api, &key).await?;
                            if let Some(mut table_meta) = table_meta {
                                table_meta.shared_by.remove(&share_id);
                                if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                                condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                            }
                        }

                        let reference_table = share_meta
                            .reference_table
                            .iter()
                            .filter(|table| table.db_id != db_id)
                            .cloned()
                            .collect();
                        share_meta.reference_table = reference_table;
                        break;
                    }
                }
                _ => {}
            }
        }
        ShareObject::Table(_table_name, _db_id, table_id) => match privileges {
            ShareGrantObjectPrivilege::Select => {
                for table in &mut share_meta.table {
                    if table.table_id != table_id {
                        continue;
                    }
                    table.revoke_object_privileges(privileges);
                    // remove share id from table `shared_by` field
                    let table_id = table.table_id;
                    let key = TableId { table_id };

                    let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                        get_pb_value(kv_api, &key).await?;
                    if let Some(mut table_meta) = table_meta {
                        table_meta.shared_by.remove(&share_id);
                        if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                    }

                    let table = share_meta
                        .table
                        .iter()
                        .filter(|table| table.table_id != table_id)
                        .cloned()
                        .collect();
                    share_meta.table = table;
                    break;
                }
            }
            ShareGrantObjectPrivilege::ReferenceUsage => {
                for table in &mut share_meta.reference_table {
                    if table.table_id != table_id {
                        continue;
                    }
                    table.revoke_object_privileges(privileges);
                    // remove share id from table `shared_by` field
                    let table_id = table.table_id;
                    let key = TableId { table_id };

                    let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                        get_pb_value(kv_api, &key).await?;
                    if let Some(mut table_meta) = table_meta {
                        table_meta.shared_by.remove(&share_id);
                        if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                    }

                    let reference_table = share_meta
                        .reference_table
                        .iter()
                        .filter(|table| table.table_id != table_id)
                        .cloned()
                        .collect();
                    share_meta.reference_table = reference_table;
                    break;
                }
            }
            _ => {}
        },

        ShareObject::View(_table_name, _db_id, table_id) => match privileges {
            ShareGrantObjectPrivilege::Select => {
                for table in &mut share_meta.table {
                    if table.table_id != table_id {
                        continue;
                    }
                    table.revoke_object_privileges(privileges);
                    // remove share id from table `shared_by` field
                    let table_id = table.table_id;
                    let key = TableId { table_id };

                    let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                        get_pb_value(kv_api, &key).await?;
                    if let Some(mut table_meta) = table_meta {
                        table_meta.shared_by.remove(&share_id);
                        if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                    }

                    let table = share_meta
                        .table
                        .iter()
                        .filter(|table| table.table_id != table_id)
                        .cloned()
                        .collect();
                    share_meta.table = table;
                    break;
                }
            }
            _ => {}
        },
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn rename_share_object(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_meta: &mut ShareMeta,
    object: ShareObject,
    share_id: u64,
    update_on: DateTime<Utc>,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let key = object.to_string();

    match object {
        ShareObject::Database(_db_name, db_id) => {
            if let Some(use_database) = &mut share_meta.use_database {
                if use_database.db_id == db_id {
                    use_database.revoke_object_privileges(ShareGrantObjectPrivilege::Usage);
                    share_meta.use_database = None;
                    share_meta.update_on = update_on;

                    // clean all shared table `shared_by` field
                    for table in &share_meta.table {
                        let table_id = table.table_id;
                        let key = TableId { table_id };

                        let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                            get_pb_value(kv_api, &key).await?;
                        if let Some(mut table_meta) = table_meta {
                            table_meta.shared_by.remove(&share_id);
                            if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                            condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                        }
                    }

                    share_meta.table.clear();
                    return Ok(());
                } else {
                    return Err(KVAppError::AppError(AppError::WrongShareObject(
                        WrongShareObject::new(&key),
                    )));
                }
            }

            for db in &mut share_meta.reference_database {
                if db.db_id != db_id {
                    continue;
                }

                db.revoke_object_privileges(ShareGrantObjectPrivilege::ReferenceUsage);
                // clean all shared table `shared_by` field
                for table in &share_meta.reference_table {
                    if table.db_id != db_id {
                        continue;
                    }
                    let table_id = table.table_id;
                    let key = TableId { table_id };

                    let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                        get_pb_value(kv_api, &key).await?;
                    if let Some(mut table_meta) = table_meta {
                        table_meta.shared_by.remove(&share_id);
                        if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                        condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                    }
                }

                let reference_table = share_meta
                    .reference_table
                    .iter()
                    .filter(|table| table.db_id != db_id)
                    .cloned()
                    .collect();
                share_meta.reference_table = reference_table;
                break;
            }
        }
        ShareObject::Table(_table_name, _db_id, table_id)
        | ShareObject::View(_table_name, _db_id, table_id) => {
            for table in &mut share_meta.table {
                if table.table_id != table_id {
                    continue;
                }
                table.revoke_object_privileges(ShareGrantObjectPrivilege::Select);
                // remove share id from table `shared_by` field
                let table_id = table.table_id;
                let key = TableId { table_id };

                let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                    get_pb_value(kv_api, &key).await?;
                if let Some(mut table_meta) = table_meta {
                    table_meta.shared_by.remove(&share_id);
                    if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                    condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                }

                let table = share_meta
                    .table
                    .iter()
                    .filter(|table| table.table_id != table_id)
                    .cloned()
                    .collect();
                share_meta.table = table;
                return Ok(());
            }
            for table in &mut share_meta.reference_table {
                if table.table_id != table_id {
                    continue;
                }
                table.revoke_object_privileges(ShareGrantObjectPrivilege::ReferenceUsage);
                // remove share id from table `shared_by` field
                let table_id = table.table_id;
                let key = TableId { table_id };

                let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
                    get_pb_value(kv_api, &key).await?;
                if let Some(mut table_meta) = table_meta {
                    table_meta.shared_by.remove(&share_id);
                    if_then.push(txn_op_put(&key, serialize_struct(&table_meta)?));
                    condition.push(txn_cond_seq(&key, Eq, table_meta_seq));
                }

                let reference_table = share_meta
                    .reference_table
                    .iter()
                    .filter(|table| table.table_id != table_id)
                    .cloned()
                    .collect();
                share_meta.reference_table = reference_table;
                break;
            }
        }
    }
    Ok(())
}

// check if all the reference tables has access right
// return: Vec<(table name, table id, db id, TableInfo)>
pub async fn check_access_reference_tables(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    view_id: u64,
    reference_tables: &[(String, String)],
    share_meta: &mut ShareMeta,
    tenant: &str,
    grant_on: DateTime<Utc>,
) -> Result<Vec<(String, u64, u64, TableInfo)>, KVAppError> {
    let mut ref_tables = vec![];
    for (db, table) in reference_tables {
        let mut found = false;
        let mut ref_db_id = None;

        // first find table in reference tables
        for ref_db in &share_meta.reference_database {
            if &ref_db.db_name != db {
                continue;
            }

            ref_db_id = Some(ref_db.db_id);

            let db_id = ref_db.db_id;
            for ref_table in &mut share_meta.reference_table {
                if ref_table.db_id != db_id {
                    continue;
                }
                if &ref_table.table_name != table {
                    continue;
                }

                found = true;
                ref_table.reference_by.insert(view_id);
                ref_tables.push((table.to_string(), db, ref_table.clone()));
                break;
            }
        }

        if found {
            continue;
        }

        // if found reference database, add into reference table
        if let Some(db_id) = ref_db_id {
            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: table.clone(),
            };

            let (_tb_id_seq, table_id) = get_u64_value(kv_api, &dbid_tbname).await?;
            let mut reference_by = BTreeSet::new();
            reference_by.insert(view_id);

            let share_table = ShareReferenceTable {
                privileges: BitFlags::from(ShareGrantObjectPrivilege::ReferenceUsage),
                table_name: table.clone(),
                db_id,
                table_id: table_id,
                grant_on,
                engine: "FUSE".to_string(),
                reference_by,
            };
            ref_tables.push((table.to_string(), db, share_table.clone()));
            share_meta.reference_table.push(share_table);

            continue;
        }

        // else, find table in use tables
        if let Some(use_db) = &share_meta.use_database {
            if &use_db.db_name == db {
                for use_table in &share_meta.table {
                    if &use_table.table_name == table {
                        found = true;
                        break;
                    }
                }
            }
        }

        if !found {
            return Err(KVAppError::AppError(AppError::WrongShareObject(
                WrongShareObject::new(table),
            )));
        }
    }

    // after check success, get reference_table_info
    let mut reference_table_info = vec![];
    for (table_name, db, ref_table) in ref_tables {
        let table_id = ref_table.table_id;
        let tbid = TableId { table_id };
        let (table_meta_seq, table_meta): (_, Option<TableMeta>) =
            get_pb_value(kv_api, &tbid).await?;
        if let Some(table_meta) = table_meta {
            let table_info = TableInfo {
                ident: TableIdent {
                    table_id,
                    seq: table_meta_seq,
                },
                desc: format!("'{}'.'{}'", db, ref_table.table_name),
                meta: table_meta,
                name: ref_table.table_name.clone(),
                tenant: tenant.to_string(),
                db_type: DatabaseType::NormalDB,
                catalog_info: Default::default(),
            };
            reference_table_info.push((table_name, table_id, ref_table.db_id, table_info));
        } else {
            return Err(KVAppError::AppError(AppError::UnknownTable(
                UnknownTable::new(
                    ref_table.table_name.clone(),
                    format!("check_access_reference_tables: {}", ref_table.table_name),
                ),
            )));
        }
    }

    Ok(reference_table_info)
}
