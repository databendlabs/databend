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

use std::time::Duration;

use databend_meta_kvapi::kvapi;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaError;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnOpResponse;
use databend_meta_types::TxnRequest;
use databend_meta_types::anyerror::AnyError;
use databend_meta_types::txn_op_response::Response;
use log::info;
use uuid::Uuid;

use crate::txn_core_util::send_txn;

pub const DEFAULT_MGET_SIZE: usize = 256;

pub struct IdempotentKVTxnSender {
    txn_id: String,
    ttl: Duration,
}

pub enum IdempotentKVTxnResponse {
    Success(Vec<TxnOpResponse>),
    AlreadyCommitted,
    Failed(Vec<TxnOpResponse>),
}

impl IdempotentKVTxnSender {
    pub fn new() -> Self {
        let txn_id = Self::gen_txn_id();
        let ttl = Duration::from_secs(300);
        Self { txn_id, ttl }
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        let txn_id = Self::gen_txn_id();
        Self { txn_id, ttl }
    }

    fn gen_txn_id() -> String {
        let uuid = Uuid::now_v7().simple().to_string();
        format!("_txn_id/{}", uuid)
    }

    pub fn get_txn_id(&self) -> &str {
        &self.txn_id
    }

    pub async fn send_txn(
        &self,
        kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
        mut txn: TxnRequest,
    ) -> Result<IdempotentKVTxnResponse, MetaError> {
        // Wrap transaction with unique id
        txn.condition
            .push(TxnCondition::eq_seq(self.txn_id.clone(), 0));

        txn.if_then.push(TxnOp::put_with_ttl(
            self.txn_id.clone(),
            vec![],
            Some(self.ttl),
        ));

        txn.else_then.push(TxnOp::get(self.txn_id.clone()));

        match send_txn(kv_api, txn).await {
            Ok((true, mut then_op_responses)) => Ok({
                // Pop the put_with_ttl txn_id KV pair operation response
                then_op_responses.pop();
                IdempotentKVTxnResponse::Success(then_op_responses)
            }),
            Ok((false, mut else_op_responses)) => {
                // Since we manipulate the transaction, the last operation response SHOULD be the
                // get transaction ID operation response. Let's check it.
                let Some(last_op_resp) = else_op_responses.pop() else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed transaction response",
                        &AnyError::error(format!(
                            "Get transaction ID key response not found {}",
                            self.txn_id
                        )),
                    )));
                };

                let Some(response) = last_op_resp.response else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed get transaction ID response",
                        &AnyError::error(format!(
                            "Get transaction ID key {} operation responses nothing",
                            self.txn_id
                        )),
                    )));
                };

                let Response::Get(get_txn_id) = response else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed get transaction ID response",
                        &AnyError::error(format!(
                            "Expects TxnGetResponse of get transaction ID {}, but got {}",
                            self.txn_id, response
                        )),
                    )));
                };

                if get_txn_id.key != self.txn_id {
                    return Err(MetaError::from(InvalidReply::new(
                        "Transaction ID key mismatch",
                        &AnyError::error(format!(
                            "Transaction ID key mismatch, expects {}, got {}",
                            self.txn_id, get_txn_id.key
                        )),
                    )));
                }

                if get_txn_id.value.is_some() {
                    // The txn_id already exists, indicating some transaction with the same transaction
                    // ID was previously committed successfully.
                    //
                    // Notes about this "idempotency":
                    // 1. This method ALLOWS sending different transaction each time
                    //    while using the same txn_id for duplicate prevention.
                    // 2. It is the CALLER'S RESPONSIBILITY to ensure that all transactions
                    //    sharing the same txn_id do make sense, even if their actual content differs.
                    // 3. This is a best-effort idempotency check, only guaranteed roughly
                    //    within the TTL duration of the transaction ID KV pair.
                    // 4. DO NOT rely on this mechanism for critical safety properties.
                    //    The main purpose of this "idempotency check" is to help identify if this transaction
                    //    has been committed successfully during implicit transaction retry.
                    info!("Transaction with ID {} already exist", self.txn_id);
                    // Operation responses of else-branch are omitted in this case:
                    // Since `AlreadyCommitted` is somehow a "success" response, return the operation
                    // responses of else-branch might be misleading. Currently, no scenarios expect to use it.
                    Ok(IdempotentKVTxnResponse::AlreadyCommitted)
                } else {
                    info!(
                        "Transaction failed, and txn id {} does not present",
                        self.txn_id
                    );
                    // Return operation responses of else branch (last response has been removed)
                    Ok(IdempotentKVTxnResponse::Failed(else_op_responses))
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Default for IdempotentKVTxnSender {
    fn default() -> Self {
        Self::new()
    }
}
