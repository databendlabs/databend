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

use std::ops::Deref;

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::SequenceError;
use databend_common_meta_app::app_error::UnsupportedSequenceStorageVersion;
use databend_common_meta_app::app_error::WrongSequenceCount;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::sequence_storage::SequenceStorageIdent;
use databend_common_meta_app::schema::sequence_storage::SequenceStorageValue;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use super::sequence_nextval_impl::NextVal;
use crate::SequenceApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_op_builder_util::txn_put_pb;

#[async_trait::async_trait]
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError> + ?Sized> SequenceApi for KV {
    async fn create_sequence(
        &self,
        req: CreateSequenceReq,
    ) -> Result<CreateSequenceReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let meta: SequenceMeta = req.clone().into();

        let storage_ident = SequenceStorageIdent::new_from(req.ident.clone());
        let storage_value = Id::new_typed(SequenceStorageValue(req.start));

        let conditions = if req.create_option == CreateOption::CreateOrReplace {
            vec![]
        } else {
            vec![txn_cond_eq_seq(&req.ident, 0)]
        };

        let txn = TxnRequest::new(conditions, vec![
            txn_put_pb(&req.ident, &meta)?,
            txn_put_pb(&storage_ident, &storage_value)?,
        ]);

        let (succ, _response) = send_txn(self, txn).await?;

        if succ {
            return Ok(CreateSequenceReply {});
        }

        match req.create_option {
            CreateOption::Create => Err(KVAppError::AppError(AppError::SequenceError(
                SequenceError::SequenceAlreadyExists(req.ident.exist_error(func_name!())),
            ))),
            CreateOption::CreateIfNotExists => Ok(CreateSequenceReply {}),
            CreateOption::CreateOrReplace => {
                unreachable!("CreateOrReplace should always success")
            }
        }
    }

    async fn get_sequence(
        &self,
        name_ident: &SequenceIdent,
    ) -> Result<Option<SeqV<SequenceMeta>>, MetaError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());
        let seq_meta = self.get_pb(name_ident).await?;

        let Some(mut seq_meta) = seq_meta else {
            return Ok(None);
        };

        if seq_meta.data.storage_version == 0 {
            return Ok(Some(seq_meta));
        }

        // V1 sequence stores the value in a separate key.

        let storage_ident = SequenceStorageIdent::new_from(name_ident.clone());
        let storage_value = self.get_pb(&storage_ident).await?;

        // If the storage value is removed, the sequence meta must also be removed.
        let Some(storage_value) = storage_value else {
            return Ok(None);
        };

        let next_available = *storage_value.data.deref();

        seq_meta.data.current = next_available;
        Ok(Some(seq_meta))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_sequences(
        &self,
        tenant: &Tenant,
    ) -> Result<Vec<(String, SequenceMeta)>, MetaError> {
        let dir_name = DirName::new(SequenceIdent::new(tenant, "dummy"));

        let strm = self.list_pb(ListOptions::unlimited(&dir_name)).await?;
        let ident_metas = strm
            .map_ok(|itm| (itm.key, itm.seqv.data))
            .try_collect::<Vec<_>>()
            .await?;

        let storage_idents = ident_metas
            .iter()
            .map(|(ident, _)| SequenceStorageIdent::new_from(ident.clone()));

        let strm = self.get_pb_values(storage_idents).await?;
        let storage_values = strm.try_collect::<Vec<_>>().await?;

        let key_metas = ident_metas
            .into_iter()
            .zip(storage_values)
            .map(|((k, mut meta), sto)| {
                // For version==1  sequence, load the current value from the external storage.
                if meta.storage_version == 1 {
                    if let Some(seq_storage_value) = sto {
                        meta.current = seq_storage_value.data.into_inner().0;
                    }
                }

                (k.name().to_string(), meta)
            })
            .collect::<Vec<_>>();

        Ok(key_metas)
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
            let sequence_meta = seq_meta.data.clone();

            let next_val = NextVal {
                kv_api: self,
                ident: req.ident.clone(),
                sequence_meta: seq_meta.clone(),
            };

            let response = match sequence_meta.storage_version {
                0 => next_val.next_val_v0(req.count).await?,
                1 => next_val.next_val_v1(req.count).await?,
                _ => {
                    return Err(KVAppError::AppError(AppError::SequenceError(
                        SequenceError::UnsupportedSequenceStorageVersion(
                            UnsupportedSequenceStorageVersion::new(sequence_meta.storage_version),
                        ),
                    )));
                }
            };

            match response {
                Ok((start, end)) => {
                    return Ok(GetSequenceNextValueReply {
                        start,
                        step: sequence_meta.step,
                        end,
                    });
                }
                Err(_e) => {
                    // conflict, continue
                }
            }
        }
    }

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let storage_ident = SequenceStorageIdent::new_from(req.ident.clone());
        let txn = TxnRequest::new(
            vec![txn_cond_seq(&req.ident, ConditionResult::Gt, 0)],
            vec![txn_del(&req.ident), txn_del(&storage_ident)],
        );
        let (success, _response) = send_txn(self, txn).await?;

        Ok(DropSequenceReply { success })
    }
}
