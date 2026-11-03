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

use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MatchSeqExt;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;

use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::txn_del;
use crate::txn_put_pb;

/// Create or replace an object and record its owner in one transaction.
///
/// - `Create` / `CreateIfNotExists`: write only if the object is absent. An
///   existing object is left untouched together with its owner, and `false`
///   is returned; the caller decides whether that is an error.
/// - `CreateOrReplace`: replace the object version that was read.
///
/// With `owner`, the ownership key is set to that role. Without it, a new
/// object gets no owner: an ownership left behind by a dropped object of the
/// same name is removed, while a replaced object keeps its owner.
/// A concurrent write to the object retries the whole transaction.
#[allow(clippy::too_many_arguments)]
pub async fn create_owned_object<K>(
    kv: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
    value: &K::ValueType,
    create_option: &CreateOption,
    tenant: &Tenant,
    object: &OwnershipObject,
    owner: Option<&str>,
    extra_ops: &[TxnOp],
) -> Result<bool, MetaTxnError>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    let mut trials = txn_backoff(None, "create_owned_object");
    loop {
        trials.next().unwrap()?.await;
        let current_seq = kv.get_kv(&key.to_string_key()).await?.map_or(0, |v| v.seq);
        if current_seq != 0 && !create_option.is_overriding() {
            return Ok(false);
        }
        let mut txn = TxnRequest::new(vec![txn_cond_eq_seq(key, current_seq)], vec![txn_put_pb(
            key, value,
        )]);
        txn.if_then.extend(extra_ops.iter().cloned());
        // A new object must not inherit an ownership left behind under its name.
        if owner.is_some() || current_seq == 0 {
            txn.if_then.push(ownership_change(tenant, object, owner));
        }
        if kv.transaction(txn).await?.success {
            return Ok(true);
        }
    }
}

/// Delete an object, its auxiliary keys and its ownership in one transaction.
/// Missing objects or a mismatched sequence leave all keys unchanged.
pub async fn drop_owned_object<K>(
    kv: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
    seq: MatchSeq,
    tenant: &Tenant,
    object: &OwnershipObject,
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
        txn.if_then.push(ownership_change(tenant, object, None));
        if kv.transaction(txn).await?.success {
            return Ok(Some(previous));
        }
    }
}

/// The operation that sets the object's owner to `new_owner`, or deletes its
/// ownership when it is `None`. It is guarded by the object's own condition in
/// the transaction it is added to.
pub fn ownership_change(
    tenant: &Tenant,
    object: &OwnershipObject,
    new_owner: Option<&str>,
) -> TxnOp {
    let ownership = TenantOwnershipObjectIdent::new(tenant, object.clone());
    match new_owner {
        Some(role) => txn_put_pb(&ownership, &OwnershipInfo {
            object: object.clone(),
            role: role.to_string(),
        }),
        None => txn_del(&ownership),
    }
}
