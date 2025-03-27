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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_meta_api::reply::txn_reply_to_api_result;
use databend_common_meta_api::txn_backoff::txn_backoff;
use databend_common_meta_api::txn_cond_seq;
use databend_common_meta_api::txn_op_del;
use databend_common_meta_api::txn_op_put;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleIdent;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::ListKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use enumflags2::make_bitflags;
use fastrace::func_name;
use log::debug;
use log::error;

use crate::role::role_api::RoleApi;
use crate::serde::check_and_upgrade_to_pb;
use crate::serde::Quota;
use crate::serialize_struct;

static TXN_MAX_RETRY_TIMES: u32 = 60;

static BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";

pub struct RoleMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
    tenant: Tenant,
    upgrade_to_pb: bool,
}

impl RoleMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
        tenant: &Tenant,
        upgrade_to_pb: bool,
    ) -> Self {
        RoleMgr {
            kv_api,
            tenant: tenant.clone(),
            upgrade_to_pb,
        }
    }

    #[async_backtrace::framed]
    async fn upsert_role_info(
        &self,
        role_info: &RoleInfo,
        seq: MatchSeq,
    ) -> Result<u64, ErrorCode> {
        let key = self.role_ident(role_info.identity()).to_string_key();
        let value = serialize_struct(role_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let res = self
            .kv_api
            .upsert_kv(UpsertKV::new(&key, seq, Operation::Update(value), None))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownRole(format!(
                "Role '{}' does not exist.",
                role_info.name
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn upgrade_to_pb(
        &self,
        key: String,
        value: Vec<u8>,
        seq: MatchSeq,
    ) -> Result<UpsertKVReply, MetaError> {
        self.kv_api
            .upsert_kv(UpsertKV::new(&key, seq, Operation::Update(value), None))
            .await
    }

    /// Build meta-service for a role grantee, which is a tenant's database, table, stage, udf, etc.
    fn ownership_object_ident(&self, object: &OwnershipObject) -> TenantOwnershipObjectIdent {
        TenantOwnershipObjectIdent::new(self.tenant.clone(), object.clone())
    }

    /// Build meta-service for a listing keys belongs to the tenant.
    ///
    /// In form of `__fd_object_owners/<tenant>/`.
    fn ownership_object_prefix(&self) -> String {
        let dummy = OwnershipObject::UDF {
            name: "dummy".to_string(),
        };
        let grantee = TenantOwnershipObjectIdent::new(self.tenant.clone(), dummy);
        grantee.tenant_prefix()
    }

    fn role_ident(&self, role: &str) -> RoleIdent {
        RoleIdent::new(self.tenant.clone(), role.to_string())
    }

    fn role_prefix(&self) -> String {
        let r = RoleIdent::new(self.tenant.clone(), "dummy".to_string());
        r.tenant_prefix()
    }
}

#[async_trait::async_trait]
impl RoleApi for RoleMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_role(&self, role_info: RoleInfo) -> databend_common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = self.role_ident(role_info.identity()).to_string_key();
        let value = serialize_struct(&role_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let upsert_kv = self.kv_api.upsert_kv(UpsertKV::new(
            &key,
            match_seq,
            Operation::Update(value),
            None,
        ));

        let res_seq = upsert_kv.await?.added_seq_or_else(|_v| {
            ErrorCode::RoleAlreadyExists(format!("Role '{}' already exists.", role_info.name))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_role(&self, role: &String, seq: MatchSeq) -> Result<SeqV<RoleInfo>, ErrorCode> {
        let key = self.role_ident(role).to_string_key();
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownRole(format!("Role '{}' does not exist.", role)))?;

        match seq.match_seq(&seq_value) {
            Ok(_) => {
                let mut quota = quota(func_name!(), self.upgrade_to_pb);

                let u = check_and_upgrade_to_pb(&mut quota, &key, &seq_value, self.kv_api.as_ref())
                    .await?;

                // Keep the original seq.
                Ok(SeqV::new(seq_value.seq, u.data))
            }
            Err(_) => Err(ErrorCode::UnknownRole(format!(
                "Role '{}' does not exist.",
                role
            ))),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_meta_roles(&self) -> Result<Vec<SeqV<RoleInfo>>, ErrorCode> {
        let values = self.get_raw_meta_roles().await?;

        let mut r = vec![];

        let mut quota = quota(func_name!(), self.upgrade_to_pb);

        for (key, val) in values {
            let u = check_and_upgrade_to_pb(&mut quota, &key, &val, self.kv_api.as_ref()).await?;
            r.push(u);
        }

        Ok(r)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_raw_meta_roles(&self) -> Result<ListKVReply, ErrorCode> {
        let role_prefix = self.role_prefix();
        Ok(self.kv_api.prefix_list_kv(role_prefix.as_str()).await?)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_ownerships(&self) -> Result<Vec<SeqV<OwnershipInfo>>, ErrorCode> {
        let object_owner_prefix = self.ownership_object_prefix();
        let values = self
            .kv_api
            .prefix_list_kv(object_owner_prefix.as_str())
            .await?;

        let mut r = vec![];

        let mut quota = quota(func_name!(), self.upgrade_to_pb);

        for (key, val) in values {
            match check_and_upgrade_to_pb(&mut quota, &key, &val, self.kv_api.as_ref()).await {
                Ok(u) => r.push(u),
                // If we add a new item in OwnershipObject, and generate a new kv about this new item,
                // After rollback the old version, deserialize will return Err Ownership can not be none.
                // But get ownerships should try to ensure success because in this version.
                Err(err) => error!(
                    "deserialize key {} Got err {} while (get_ownerships)",
                    &key, err
                ),
            }
        }

        Ok(r)
    }

    /// General role update.
    ///
    /// It fetch the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn update_role_with<F>(
        &self,
        role: &String,
        seq: MatchSeq,
        f: F,
    ) -> Result<Option<u64>, ErrorCode>
    where
        F: FnOnce(&mut RoleInfo) + Send,
    {
        let SeqV {
            seq,
            data: mut role_info,
            ..
        } = self.get_role(role, seq).await?;

        f(&mut role_info);

        let seq = self
            .upsert_role_info(&role_info, MatchSeq::Exact(seq))
            .await?;
        Ok(Some(seq))
    }

    /// Only drop role will call transfer.
    ///
    /// If a role is dropped, but the owner object is exists,
    ///
    /// The owner role need to update to account_admin.
    ///
    /// get_ownerships use prefix_list_kv that will generate once meta call
    ///
    /// According to Txn reduce meta call. If role own n objects, will generate once meta call.
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn transfer_ownership_to_admin(
        &self,
        role: &str,
    ) -> databend_common_exception::Result<()> {
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap().map_err(AppError::from)?.await;
            let mut if_then = vec![];
            let mut condition = vec![];
            let seq_owns = self.get_ownerships().await.map_err(|e| {
                e.add_message_back("(while in transfer_ownership_to_admin get ownerships).")
            })?;
            let mut need_transfer = false;
            for own in seq_owns {
                if own.data.role == *role {
                    need_transfer = true;
                    let object = own.data.object;
                    let owner_key = self.ownership_object_ident(&object);
                    let owner_value = serialize_struct(
                        &OwnershipInfo {
                            object,
                            role: BUILTIN_ROLE_ACCOUNT_ADMIN.to_string(),
                        },
                        ErrorCode::IllegalUserInfoFormat,
                        || "",
                    )?;
                    // Ensure accurate matching of a key
                    condition.push(txn_cond_seq(&owner_key, Eq, own.seq));
                    if_then.push(txn_op_put(&owner_key, owner_value))
                }
            }

            if need_transfer {
                let txn_req = TxnRequest::new(condition.clone(), if_then.clone());
                let tx_reply = self.kv_api.transaction(txn_req.clone()).await?;
                let (succ, _) = txn_reply_to_api_result(tx_reply)?;
                debug!(
                    succ = succ;
                    "transfer_ownership_to_admin"
                );
                if succ {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn grant_ownership(
        &self,
        object: &OwnershipObject,
        new_role: &str,
    ) -> databend_common_exception::Result<()> {
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let old_role = self.get_ownership(object).await?.map(|o| o.role);
            let grant_object = convert_to_grant_obj(object);

            let owner_key = self.ownership_object_ident(object);
            let owner_value = serialize_struct(
                &OwnershipInfo {
                    object: object.clone(),
                    role: new_role.to_string(),
                },
                ErrorCode::IllegalUserInfoFormat,
                || "",
            )?;

            let mut condition = vec![];
            let mut if_then = vec![txn_op_put(&owner_key, owner_value.clone())];

            if let Some(ref old_role) = old_role {
                // BUILTIN role or Dropped role may get err, no need to revoke
                if let Ok(seqv) = self.get_role(old_role, MatchSeq::GE(1)).await {
                    let old_key = self.role_ident(old_role);
                    let old_seq = seqv.seq;
                    let mut old_role_info = seqv.data;
                    old_role_info.grants.revoke_privileges(
                        &grant_object,
                        make_bitflags!(UserPrivilegeType::{ Ownership }).into(),
                    );
                    old_role_info.update_role_time();
                    condition.push(txn_cond_seq(&old_key, Eq, old_seq));
                    if_then.push(txn_op_put(
                        &old_key,
                        serialize_struct(&old_role_info, ErrorCode::IllegalUserInfoFormat, || "")?,
                    ));
                }
            }

            let txn_req = TxnRequest::new(condition.clone(), if_then.clone());

            let tx_reply = self.kv_api.transaction(txn_req.clone()).await?;
            let (succ, _) = txn_reply_to_api_result(tx_reply)?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("grant_ownership", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_ownership(
        &self,
        object: &OwnershipObject,
    ) -> databend_common_exception::Result<Option<OwnershipInfo>> {
        let key = self.ownership_object_ident(object);
        let key = key.to_string_key();

        let res = self.kv_api.get_kv(&key).await?;
        let seq_value = match res {
            Some(value) => value,
            None => return Ok(None),
        };

        let mut quota = quota(func_name!(), self.upgrade_to_pb);

        // if can not get ownership, will directly return None.
        let seq_val =
            check_and_upgrade_to_pb(&mut quota, &key, &seq_value, self.kv_api.as_ref()).await?;

        Ok(Some(seq_val.data))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn revoke_ownership(
        &self,
        object: &OwnershipObject,
    ) -> databend_common_exception::Result<()> {
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let role = self.get_ownership(object).await?.map(|o| o.role);

            let owner_key = self.ownership_object_ident(object);

            let mut if_then = vec![txn_op_del(&owner_key)];
            let mut condition = vec![];

            if let Some(role) = role {
                if let Ok(seqv) = self.get_role(&role.to_owned(), MatchSeq::GE(1)).await {
                    let old_key = self.role_ident(&role);
                    let grant_object = convert_to_grant_obj(object);
                    let old_seq = seqv.seq;
                    let mut old_role_info = seqv.data;
                    // Old version store ownership in role, so verify_privilege first
                    // If not exists in old_role, no need to revoke privilege
                    if old_role_info
                        .grants
                        .verify_privilege(&grant_object, UserPrivilegeType::Ownership)
                    {
                        old_role_info.grants.revoke_privileges(
                            &grant_object,
                            make_bitflags!(UserPrivilegeType::{ Ownership }).into(),
                        );
                        old_role_info.update_role_time();
                        condition.push(txn_cond_seq(&old_key, Eq, old_seq));
                        if_then.push(txn_op_put(
                            &old_key,
                            serialize_struct(
                                &old_role_info,
                                ErrorCode::IllegalUserInfoFormat,
                                || "",
                            )?,
                        ));
                    }
                }
            }

            let txn_req = TxnRequest::new(condition.clone(), if_then.clone());

            let tx_reply = self.kv_api.transaction(txn_req.clone()).await?;
            let (succ, _) = txn_reply_to_api_result(tx_reply)?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("revoke_ownership", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn drop_role(&self, role: String, seq: MatchSeq) -> Result<(), ErrorCode> {
        let key = self.role_ident(&role).to_string_key();

        let res = self
            .kv_api
            .upsert_kv(UpsertKV::new(&key, seq, Operation::Delete, None))
            .await?;

        res.removed_or_else(|_p| {
            ErrorCode::UnknownRole(format!("Role '{}' does not exist.", role))
        })?;
        Ok(())
    }
}

fn convert_to_grant_obj(owner_obj: &OwnershipObject) -> GrantObject {
    match owner_obj {
        OwnershipObject::Database {
            catalog_name,
            db_id,
        } => GrantObject::DatabaseById(catalog_name.to_string(), *db_id),
        OwnershipObject::Table {
            catalog_name,
            db_id,
            table_id,
        } => GrantObject::TableById(catalog_name.to_string(), *db_id, *table_id),
        OwnershipObject::Stage { name } => GrantObject::Stage(name.to_string()),
        OwnershipObject::UDF { name } => GrantObject::UDF(name.to_string()),
        OwnershipObject::Warehouse { id } => GrantObject::Warehouse(id.to_string()),
    }
}

fn quota(target: impl ToString, upgrade_to_pb: bool) -> Quota {
    if upgrade_to_pb {
        Quota::new_limit(target, 10)
    } else {
        // Do not serialize to protobuf format
        Quota::new_limit(target, 0)
    }
}
