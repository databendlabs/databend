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

//! Core transaction utilities extracted from util.rs

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnRequest;
use databend_common_proto_conv::FromToProto;
use display_more::DisplaySliceExt;
use log::debug;

use crate::reply::unpack_txn_reply;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_op_builder_util::txn_op_del;
use crate::txn_op_builder_util::txn_op_put_pb;

/// Send a transaction to the KV API and return success status and responses.
///
/// This is the core transaction sending function used throughout the meta API.
pub async fn send_txn(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
    debug!("send txn: {}", txn_req);
    let tx_reply = kv_api.transaction(txn_req).await?;
    let (succ, responses) = unpack_txn_reply(tx_reply);
    debug!("txn success: {}: {}", succ, responses.display_n(20));
    Ok((succ, responses))
}

/// Add a delete operation by key and exact seq to [`TxnRequest`].
pub fn txn_delete_exact(txn: &mut TxnRequest, key: &impl kvapi::Key, seq: u64) {
    txn.condition.push(txn_cond_eq_seq(key, seq));
    txn.if_then.push(txn_op_del(key));
}

/// Add a replace operation by key and exact seq to [`TxnRequest`].
pub fn txn_replace_exact<K>(
    txn: &mut TxnRequest,
    key: &K,
    seq: u64,
    value: &K::ValueType,
) -> Result<(), InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    txn.condition.push(txn_cond_eq_seq(key, seq));
    txn.if_then.push(txn_op_put_pb(key, value, None)?);

    Ok(())
}
