// Copyright 2026 Datafuse Labs
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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleIdent;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::InvalidReply;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MatchSeqExt;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;
use serde::de::DeserializeOwned;

use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::txn_del;
use crate::txn_put_pb;

/// Delete an object, its auxiliary keys and its ownership in one transaction.
/// Missing objects or a mismatched sequence leave all keys unchanged.
pub async fn drop_owned_object<K>(
    kv: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
    seq: MatchSeq,
    ownership: &TenantOwnershipObjectIdent,
    extra_keys: &[String],
) -> Result<Option<SeqV<K::ValueType>>, MetaTxnError>
where
    K: kvapi::Key,
    K::ValueType: FromToProto,
{
    let mut trials = txn_backoff(None, "drop_owned_object");
    loop {
        trials.next().unwrap()?.await;
        let Some(previous) = kv.get_pb(key).await? else {
            return Ok(None);
        };
        if seq.match_seq(&previous.seq).is_err() {
            return Ok(None);
        }
        let mut txn = TxnRequest::new(vec![txn_cond_eq_seq(key, previous.seq)], vec![txn_del(key)]);
        txn.if_then.extend(extra_keys.iter().map(TxnOp::delete));
        append_ownership_cleanup(kv, ownership, &mut txn).await?;
        if kv.transaction(txn).await?.success {
            return Ok(Some(previous));
        }
    }
}

/// Add ownership cleanup to the object's deletion transaction, including the
/// ownership grant stored in older role records. Guard every record read so a
/// concurrent transfer or role update causes the caller to retry the whole drop.
/// An absent ownership key is valid for objects created by older versions.
pub async fn append_ownership_cleanup(
    kv: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ownership: &TenantOwnershipObjectIdent,
    txn: &mut TxnRequest,
) -> Result<(), MetaError> {
    let current = kv.get_kv(&ownership.to_string_key()).await?;
    txn.condition.push(txn_cond_eq_seq(
        ownership,
        current.as_ref().map_or(0, |value| value.seq),
    ));
    txn.if_then.push(txn_del(ownership));
    let Some(current) = current else {
        return Ok(());
    };
    let info: OwnershipInfo = decode_legacy(&current.data)?;
    let role_key = RoleIdent::new(ownership.tenant(), &info.role);
    if let Some(current_role) = kv.get_kv(&role_key.to_string_key()).await? {
        let mut role: RoleInfo = decode_legacy(&current_role.data)?;
        let object = ownership_grant_object(&info.object);
        if role
            .grants
            .verify_privilege(&object, UserPrivilegeType::Ownership)
        {
            role.grants
                .revoke_privileges(&object, UserPrivilegeType::Ownership.into());
            role.update_role_time();
            txn.condition
                .push(txn_cond_eq_seq(&role_key, current_role.seq));
            txn.if_then.push(txn_put_pb(&role_key, &role));
        }
    }
    Ok(())
}

// Ownership and role records written by older versions may still be JSON.
fn decode_legacy<T: FromToProto + DeserializeOwned>(bytes: &[u8]) -> Result<T, MetaError> {
    match crate::deserialize_struct(bytes) {
        Ok(value) => Ok(value),
        Err(_) => serde_json::from_slice(bytes).map_err(|error| {
            InvalidReply::new("non json non pb ownership/role data", &error).into()
        }),
    }
}

pub fn ownership_grant_object(owner_obj: &OwnershipObject) -> GrantObject {
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
        OwnershipObject::Connection { name } => GrantObject::Connection(name.to_string()),
        OwnershipObject::Sequence { name } => GrantObject::Sequence(name.to_string()),
        OwnershipObject::Procedure { procedure_id } => GrantObject::Procedure(*procedure_id),
        OwnershipObject::MaskingPolicy { policy_id } => GrantObject::MaskingPolicy(*policy_id),
        OwnershipObject::RowAccessPolicy { policy_id } => GrantObject::RowAccessPolicy(*policy_id),
    }
}
