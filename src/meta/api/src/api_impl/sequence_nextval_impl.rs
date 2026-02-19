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

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::OutOfSequenceRange;
use databend_common_meta_app::app_error::SequenceError;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::sequence_storage::SequenceStorageIdent;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::kv_app_error::KVAppError;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// The implementation of `next_val` for sequence number.
///
/// The implementation is different for different storage versions:
///
/// - v0: `current` field of `SequenceMeta`.
/// - v1: standalone key that support `FetchAddU64` to get next value.
pub(crate) struct NextVal<'a, KV>
where KV: kvapi::KVApi<Error = MetaError> + ?Sized
{
    pub(crate) kv_api: &'a KV,
    pub(crate) ident: SequenceIdent,
    pub(crate) sequence_meta: SeqV<SequenceMeta>,
}

impl<'a, KV> NextVal<'a, KV>
where KV: kvapi::KVApi<Error = MetaError> + ?Sized
{
    /// Sequence number v0 stores the value in `current` field of `SequenceMeta`.
    /// The update is a clientside CAS.
    pub(crate) async fn next_val_v0(
        mut self,
        count: u64,
    ) -> Result<Result<(u64, u64), &'static str>, KVAppError> {
        debug!("{}", func_name!());

        let start = self.sequence_meta.current;
        if u64::MAX - start < count {
            return Err(KVAppError::AppError(AppError::SequenceError(
                SequenceError::OutOfSequenceRange(OutOfSequenceRange::new(
                    self.ident.name(),
                    format!(
                        "{:?}: current: {}, count: {}",
                        self.ident.name(),
                        start,
                        count
                    ),
                )),
            )));
        }

        // update meta
        self.sequence_meta.current += count * self.sequence_meta.step as u64;

        let condition = vec![txn_cond_eq_seq(&self.ident, self.sequence_meta.seq)];
        let if_then = vec![
            txn_put_pb_with_ttl(&self.ident, &self.sequence_meta.data, None)?, // name -> meta
        ];

        let txn_req = TxnRequest::new(condition, if_then);

        let (succ, _responses) = send_txn(self.kv_api, txn_req).await?;

        debug!(
            "{} txn result: succ: {succ}, ident: {}, update seq to {}",
            func_name!(),
            self.ident,
            self.sequence_meta.current
        );

        if succ {
            Ok(Ok((start, self.sequence_meta.current)))
        } else {
            Ok(Err("transaction conflict"))
        }
    }

    /// Sequence number v1 stores the value in standalone key that support `FetchAddU64`.
    pub(crate) async fn next_val_v1(
        self,
        count: u64,
    ) -> Result<Result<(u64, u64), &'static str>, KVAppError> {
        debug!("{}", func_name!());

        // Key for the sequence number value.
        let storage_ident = SequenceStorageIdent::new_from(self.ident.clone());
        let storage_key = storage_ident.to_string_key();

        let sequence_meta = &self.sequence_meta.data;

        let delta = count * (self.sequence_meta.step as u64);

        let txn = TxnRequest::new(
            vec![txn_cond_eq_seq(&self.ident, self.sequence_meta.seq)],
            vec![TxnOp::fetch_add_u64(&storage_key, delta as i64)],
        );

        let (succ, responses) = send_txn(self.kv_api, txn).await?;

        debug!(
            "{} txn result: succ: {succ}, ident: {}, update seq by {delta}",
            func_name!(),
            self.ident,
        );

        if succ {
            let resp = responses[0].try_as_fetch_increase_u64().unwrap();

            let got_delta = resp.delta();

            if got_delta < delta {
                return Err(KVAppError::AppError(AppError::SequenceError(
                    SequenceError::OutOfSequenceRange(OutOfSequenceRange::new(
                        self.ident.name(),
                        format!(
                            "{sequence_meta}: count: {count}; expected delta: {delta}, but got: {resp}",
                        ),
                    )),
                )));
            }

            Ok(Ok((resp.before, resp.after)))
        } else {
            Ok(Err("transaction conflict"))
        }
    }
}
