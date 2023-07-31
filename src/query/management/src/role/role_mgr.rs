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

use chrono::Utc;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_meta_api::serialize_struct;
use common_meta_api::txn_cond_seq;
use common_meta_api::txn_op_put;
use common_meta_api::SchemaApi;
use common_meta_api::TXN_MAX_RETRY_TIMES;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserPrivilegeSet;
use common_meta_app::principal::UserPrivilegeType;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::Ownership;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableNameIdent;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_store::MetaStore;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TxnRequest;

use crate::role::role_api::RoleApi;

static ROLE_API_KEY_PREFIX: &str = "__fd_roles";

// TODO(Zhihanz) unify built in role in management crate
pub const BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";

pub struct RoleMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    meta_api: Arc<MetaStore>,
    role_prefix: String,
    tenant: String,
}

impl RoleMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        meta_api: Arc<MetaStore>,
        tenant: &str,
    ) -> Result<Self, ErrorCode> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while role mgr create)",
            ));
        }
        let tenant = tenant.to_string();
        Ok(RoleMgr {
            kv_api,
            meta_api,
            tenant: tenant.clone(),
            role_prefix: format!("{}/{}", ROLE_API_KEY_PREFIX, tenant),
        })
    }

    #[async_backtrace::framed]
    async fn upsert_role_info(
        &self,
        role_info: &RoleInfo,
        seq: MatchSeq,
    ) -> Result<u64, ErrorCode> {
        let key = self.make_role_key(role_info.identity());
        let value = serde_json::to_vec(&role_info)?;

        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Update(value), None))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownRole(format!(
                "unknown role, or seq not match {}",
                role_info.name
            ))),
        }
    }

    fn make_role_key(&self, role: &str) -> String {
        format!("{}/{}", self.role_prefix, role)
    }
}

#[async_trait::async_trait]
impl RoleApi for RoleMgr {
    #[async_backtrace::framed]
    async fn add_role(&self, role_info: RoleInfo) -> common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = self.make_role_key(role_info.identity());
        let value = serde_json::to_vec(&role_info)?;

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVReq::new(
            &key,
            match_seq,
            Operation::Update(value),
            None,
        ));

        let res = upsert_kv.await?.added_or_else(|v| {
            ErrorCode::UserAlreadyExists(format!("Role already exists, seq [{}]", v.seq))
        })?;

        Ok(res.seq)
    }

    #[async_backtrace::framed]
    async fn get_role(&self, role: &String, seq: MatchSeq) -> Result<SeqV<RoleInfo>, ErrorCode> {
        let key = self.make_role_key(role);
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownRole(format!("unknown role {}", role)))?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownRole(format!("unknown role {}", role))),
        }
    }

    #[async_backtrace::framed]
    async fn get_roles(&self) -> Result<Vec<SeqV<RoleInfo>>, ErrorCode> {
        let role_prefix = self.role_prefix.clone();
        let kv_api = self.kv_api.clone();
        let values = kv_api.prefix_list_kv(role_prefix.as_str()).await?;

        let mut r = vec![];
        for (_key, val) in values {
            let u = serde_json::from_slice::<RoleInfo>(&val.data)
                .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    /// General role update.
    ///
    /// It fetch the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    #[async_backtrace::framed]
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

    async fn grant_ownership(
        &self,
        from: &String,
        to: &String,
        object: &GrantObject,
    ) -> common_exception::Result<()> {
        match object {
            GrantObject::Database(catalog, db) => {
                self.grant_database_ownership(from, to, catalog, db).await?;
            }
            GrantObject::Table(catalog, db, table) => {
                self.grant_table_ownership(from, to, catalog, db, table)
                    .await?;
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "grant object {:?} not implemented",
                    object
                )));
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn drop_role(&self, role: String, seq: MatchSeq) -> Result<(), ErrorCode> {
        let key = self.make_role_key(&role);
        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownRole(format!("unknown role {}", role)))
        }
    }
}

impl RoleMgr {
    async fn grant_database_ownership(
        &self,
        from: &String,
        to: &String,
        catalog: &str,
        db_name: &String,
    ) -> common_exception::Result<()> {
        let meta_api = self.meta_api.clone();
        let mut retry = 0;
        let tenant = self.tenant.clone();
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let db_info = meta_api
                .get_database(GetDatabaseReq {
                    inner: DatabaseNameIdent {
                        tenant: tenant.clone(),
                        db_name: db_name.clone(),
                    },
                })
                .await?;
            let mut db_meta = db_info.meta.clone();
            // if current owner is not none and not from, return error
            if db_meta.owner.is_some()
                && from.clone() != *BUILTIN_ROLE_ACCOUNT_ADMIN
                && db_meta.owner.as_ref().unwrap().owner_role_name != from.clone()
            {
                return Err(ErrorCode::IllegalGrant(format!(
                    "{} is not owner of {}",
                    from, db_name
                )));
            }

            let db_meta_seq = db_info.ident.seq;
            let db_id_key = DatabaseId {
                db_id: db_info.ident.db_id,
            };
            let from_role_key = self.make_role_key(from);
            let to_role_key = self.make_role_key(to);
            let from_role = self.get_role(from, MatchSeq::GE(0)).await?;
            let mut from_role_data = from_role.data;
            let to_role = self.get_role(to, MatchSeq::GE(0)).await?;
            let mut to_role_data = to_role.data;
            db_meta.owner = Some(Ownership {
                owner_role_name: to.clone(),
                updated_on: Utc::now(),
            });

            from_role_data.grants.revoke_privileges(
                &GrantObject::Database(catalog.to_string(), db_name.clone()),
                UserPrivilegeSet::from(UserPrivilegeType::Ownership),
            );
            to_role_data.grants.grant_privileges(
                &GrantObject::Database(catalog.to_string(), db_name.clone()),
                UserPrivilegeSet::from(UserPrivilegeType::Ownership),
            );
            let from_data = serde_json::to_vec(&from_role_data)?;
            let to_data = serde_json::to_vec(&to_role_data)?;
            let txn_req = TxnRequest {
                condition: vec![
                    txn_cond_seq(&db_id_key, Eq, db_meta_seq),
                    txn_cond_seq(&from_role_key, Eq, from_role.seq),
                    txn_cond_seq(&to_role_key, Eq, to_role.seq),
                ],
                if_then: vec![
                    txn_op_put(
                        &db_id_key,
                        serialize_struct(&db_meta).map_err_to_code(
                            ErrorCode::IllegalGrant,
                            || "failed to serialize database meta",
                        )?,
                    ),
                    txn_op_put(&from_role_key, from_data),
                    txn_op_put(&to_role_key, to_data),
                ],
                else_then: vec![],
            };
            let reply = self.kv_api.clone().transaction(txn_req).await?;
            let succ = reply.success;
            if succ {
                return Ok(());
            }
        }
        Err(ErrorCode::TxnRetryMaxTimes(format!(
            "failed to update database ownership {} from: {} to: {}",
            db_name, from, to
        )))
    }
    async fn grant_table_ownership(
        &self,
        from: &String,
        to: &String,
        catalog: &str,
        db_name: &String,
        table_name: &String,
    ) -> common_exception::Result<()> {
        let meta_api = self.meta_api.clone();
        let mut retry = 0;
        let tenant = self.tenant.clone();
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let table_info = meta_api
                .get_table(GetTableReq {
                    inner: TableNameIdent {
                        tenant: tenant.clone(),
                        db_name: db_name.clone(),
                        table_name: table_name.clone(),
                    },
                })
                .await?;
            let mut table_meta = table_info.meta.clone();
            // if current owner is not none and not from, return error
            if table_meta.owner.is_some()
                && table_meta.owner.as_ref().unwrap().owner_role_name != from.clone()
            {
                return Err(ErrorCode::IllegalGrant(format!(
                    "{} is not owner of {}.{}",
                    from, db_name, table_name
                )));
            }

            let tb_meta_seq = table_info.ident.seq;
            let tb_id_key = TableId {
                table_id: table_info.ident.table_id,
            };

            let from_role_key = self.make_role_key(from);
            let to_role_key = self.make_role_key(to);
            let from_role = self.get_role(from, MatchSeq::GE(0)).await?;
            let mut from_role_data = from_role.data;
            let to_role = self.get_role(to, MatchSeq::GE(0)).await?;
            let mut to_role_data = to_role.data;
            table_meta.owner = Some(Ownership {
                owner_role_name: to.clone(),
                updated_on: Utc::now(),
            });

            from_role_data.grants.revoke_privileges(
                &GrantObject::Table(catalog.to_string(), db_name.clone(), table_name.clone()),
                UserPrivilegeSet::from(UserPrivilegeType::Ownership),
            );
            to_role_data.grants.grant_privileges(
                &GrantObject::Table(catalog.to_string(), db_name.clone(), table_name.clone()),
                UserPrivilegeSet::from(UserPrivilegeType::Ownership),
            );
            let from_data = serde_json::to_vec(&from_role_data)?;
            let to_data = serde_json::to_vec(&to_role_data)?;
            let txn_req = TxnRequest {
                condition: vec![
                    txn_cond_seq(&tb_id_key, Eq, tb_meta_seq),
                    txn_cond_seq(&from_role_key, Eq, from_role.seq),
                    txn_cond_seq(&to_role_key, Eq, to_role.seq),
                ],
                if_then: vec![
                    txn_op_put(
                        &tb_id_key,
                        serialize_struct(&table_meta).map_err_to_code(
                            ErrorCode::IllegalGrant,
                            || "failed to serialize table meta",
                        )?,
                    ),
                    txn_op_put(&from_role_key, from_data),
                    txn_op_put(&to_role_key, to_data),
                ],
                else_then: vec![],
            };
            let reply = self.kv_api.clone().transaction(txn_req).await?;
            let succ = reply.success;
            if succ {
                return Ok(());
            }
        }
        Err(ErrorCode::TxnRetryMaxTimes(format!(
            "failed to update table ownership {}.{} from: {} to: {}",
            db_name, table_name, from, to
        )))
    }
}
