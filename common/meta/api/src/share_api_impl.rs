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

use common_datavalues::chrono::Utc;
use common_meta_app::share::CreateShareReply;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReply;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::ShareId;
use common_meta_app::share::ShareMeta;
use common_meta_app::share::ShareNameIdent;
use common_meta_types::app_error::AppError;
use common_meta_types::app_error::ShareAlreadyExists;
use common_meta_types::app_error::TxnRetryMaxTimes;
use common_meta_types::app_error::UnknownShare;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use common_tracing::tracing;

use crate::fetch_id;
use crate::get_struct_value;
use crate::get_u64_value;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
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

            let share_id = fetch_id(self, ShareIdGen {}).await?;
            let id_key = ShareId { share_id };

            tracing::debug!(share_id, name_key = debug(&name_key), "new share id");

            // Create share by transaction.
            {
                let now = Utc::now();
                let txn_req = TxnRequest {
                    condition: vec![txn_cond_seq(name_key, Eq, 0)],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(share_id)?), /* (tenant, share_name) -> share_id */
                        txn_op_put(
                            &id_key,
                            serialize_struct(&ShareMeta::new(now, req.comment.clone()))?,
                        ), // (share_id) -> share_meta
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

            let (share_id_seq, share_id, share_meta_seq, _share_meta) = match res {
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

            // Delete share by these operations:
            // del (tenant, share_name)
            // del share_id
            // TODO: del all outbound of share

            let share_id_key = ShareId { share_id };

            tracing::debug!(share_id, name_key = debug(&name_key), "drop_share");

            {
                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, share_id_seq),
                        txn_cond_seq(&share_id_key, Eq, share_meta_seq),
                    ],
                    if_then: vec![
                        txn_op_del(name_key),      // del (tenant, share_name)
                        txn_op_del(&share_id_key), // del share_id
                    ],
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
}

/// Returns (share_id_seq, share_id, share_meta_seq, share_meta)
async fn get_share_or_err(
    kv_api: &impl KVApi,
    name_key: &ShareNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ShareMeta), MetaError> {
    let (share_id_seq, share_id) = get_u64_value(kv_api, name_key).await?;
    share_has_to_exist(share_id_seq, name_key, &msg)?;

    let id_key = ShareId { share_id };

    let (share_meta_seq, share_meta) = get_struct_value(kv_api, &id_key).await?;
    share_has_to_exist(share_meta_seq, name_key, msg)?;

    Ok((
        share_id_seq,
        share_id,
        share_meta_seq,
        // Safe unwrap(): share_meta_seq > 0 implies share_meta is not None.
        share_meta.unwrap(),
    ))
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
