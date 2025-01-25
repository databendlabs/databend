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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::OutofSequenceRange;
use databend_common_meta_app::app_error::SequenceError;
use databend_common_meta_app::app_error::WrongSequenceCount;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::databend_common_meta_types::With;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::send_txn;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::util::txn_op_put_pb;
use crate::SequenceApi;

#[async_trait::async_trait]
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError> + ?Sized> SequenceApi for KV {
    async fn create_sequence(
        &self,
        req: CreateSequenceReq,
    ) -> Result<CreateSequenceReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let meta: SequenceMeta = req.clone().into();

        let seq = MatchSeq::from(req.create_option);
        let key = req.ident.clone();
        let reply = self
            .upsert_pb(&UpsertPB::update(key, meta).with(seq))
            .await?;

        debug!(
            ident :?= (req.ident),
            prev :? = (reply.prev),
            is_changed = reply.is_changed();
            "create_sequence"
        );

        if !reply.is_changed() {
            match req.create_option {
                CreateOption::Create => Err(KVAppError::AppError(AppError::SequenceError(
                    SequenceError::SequenceAlreadyExists(req.ident.exist_error(func_name!())),
                ))),
                CreateOption::CreateIfNotExists => Ok(CreateSequenceReply {}),
                CreateOption::CreateOrReplace => {
                    unreachable!("CreateOrReplace should always success")
                }
            }
        } else {
            Ok(CreateSequenceReply {})
        }
    }

    async fn get_sequence(
        &self,
        name_ident: &SequenceIdent,
    ) -> Result<Option<SeqV<SequenceMeta>>, MetaError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());
        let seq_meta = self.get_pb(name_ident).await?;
        Ok(seq_meta)
    }

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let sequence_name = req.ident.name();
        if req.count == 0 {
            return Err(KVAppError::AppError(AppError::SequenceError(
                SequenceError::WrongSequenceCount(WrongSequenceCount::new(sequence_name)),
            )));
        }

        let ident = req.ident.clone();
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;
            let seq_meta = self.get_pb(&ident).await?;
            let Some(seq_meta) = seq_meta else {
                return Err(AppError::SequenceError(SequenceError::UnknownSequence(
                    ident.unknown_error(func_name!()),
                ))
                .into());
            };
            let sequence_seq = seq_meta.seq;
            let mut sequence_meta = seq_meta.data;

            let start = sequence_meta.current;
            let count = req.count;
            if u64::MAX - sequence_meta.current < count {
                return Err(KVAppError::AppError(AppError::SequenceError(
                    SequenceError::OutofSequenceRange(OutofSequenceRange::new(
                        sequence_name,
                        format!(
                            "{:?}: current: {}, count: {}",
                            sequence_name, sequence_meta.current, count
                        ),
                    )),
                )));
            }

            // update meta
            sequence_meta.current += count;
            sequence_meta.update_on = Utc::now();

            let condition = vec![txn_cond_eq_seq(&ident, sequence_seq)];
            let if_then = vec![
                txn_op_put_pb(&ident, &sequence_meta, None)?, // name -> meta
            ];

            let txn_req = TxnRequest::new(condition, if_then);

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                current :? =(&sequence_meta.current),
                ident :?= (req.ident),
                succ = succ;
                "get_sequence_next_values"
            );
            if succ {
                return Ok(GetSequenceNextValueReply {
                    start,
                    step: sequence_meta.step,
                    end: sequence_meta.current - 1,
                });
            }
        }
    }

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let key = req.ident.clone();
        let reply = self.upsert_pb(&UpsertPB::delete(key)).await?;

        debug!(
            ident :?= (req.ident),
            prev :? = (reply.prev),
            is_changed = reply.is_changed();
            "drop_sequence"
        );

        // return prev if drop success
        let prev = reply.prev.map(|prev| prev.seq);

        Ok(DropSequenceReply { prev })
    }
}
