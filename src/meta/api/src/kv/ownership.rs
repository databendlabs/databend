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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;

use crate::send_txn;
use crate::txn_cond_eq_seq;
use crate::txn_del;
use crate::txn_put_pb;

/// Create or replace an object and record its owner in one transaction.
///
/// An absent object is written together with `extra_ops` and owned by `owner`.
/// Without an owner, an ownership left behind under the name by a dropped
/// object is removed, so the new object never inherits it.
///
/// An existing object is replaced only with `replace`: the owner is then set to
/// `owner`, or kept without one. Otherwise nothing is written.
///
/// Returns whether the object was written.
pub async fn create_owned_object<K>(
    kv: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
    value: &K::ValueType,
    replace: bool,
    object: &OwnershipObject,
    owner: Option<&str>,
    extra_ops: &[TxnOp],
) -> Result<bool, MetaError>
where
    K: kvapi::Key + KeyWithTenant,
    K::ValueType: FromToProto + 'static,
{
    let ownership = TenantOwnershipObjectIdent::new(key.tenant(), object.clone());
    let set_owner = owner.map(|role| {
        txn_put_pb(&ownership, &OwnershipInfo {
            object: object.clone(),
            role: role.to_string(),
        })
    });
    let mut write = vec![txn_put_pb(key, value)];
    write.extend(extra_ops.iter().cloned());

    let mut txn = TxnRequest::new(vec![txn_cond_eq_seq(key, 0)], write.clone());
    txn.if_then
        .push(set_owner.clone().unwrap_or_else(|| txn_del(&ownership)));
    if replace {
        txn.else_then = write;
        txn.else_then.extend(set_owner);
    }
    let (absent, _) = send_txn(kv, txn).await?;
    Ok(absent || replace)
}
