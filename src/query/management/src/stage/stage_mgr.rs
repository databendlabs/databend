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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_api::reply::unpack_txn_reply;
use databend_common_meta_api::txn_cond_eq_seq;
use databend_common_meta_api::txn_cond_seq;
use databend_common_meta_api::txn_del;
use databend_common_meta_api::txn_put_pb;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_app::principal::StageFile;
use databend_common_meta_app::principal::StageFileIdent;
use databend_common_meta_app::principal::StageIdent;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use databend_meta_types::With;
use futures::TryStreamExt;

use crate::errors::meta_service_error;
use crate::serde::deserialize_struct;
use crate::stage::StageApi;

const TXN_MAX_RETRY_TIMES: u32 = 10;

pub struct StageMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl StageMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        StageMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    fn stage_ident(&self, stage: &str) -> StageIdent {
        StageIdent::new(self.tenant.clone(), stage)
    }

    fn stage_file_ident(&self, stage: impl ToString, name: impl ToString) -> StageFileIdent {
        let si = StageIdent::new(self.tenant.clone(), stage);
        StageFileIdent::new(si, name)
    }

    fn stage_file_prefix(&self, stage: &str) -> String {
        let si = StageIdent::new(self.tenant.clone(), stage);
        let fi = StageFileIdent::new(si, "");
        fi.to_string_key()
    }
}

#[async_trait::async_trait]
impl StageApi for StageMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_stage(&self, info: StageInfo, create_option: &CreateOption) -> Result<()> {
        let ident = self.stage_ident(&info.stage_name);
        let seq = MatchSeq::from(*create_option);
        let upsert = UpsertPB::update(ident, info.clone()).with(seq);

        let res = self
            .kv_api
            .upsert_pb(&upsert)
            .await
            .map_err(meta_service_error)?;

        if let CreateOption::Create = create_option {
            if res.prev.is_some() {
                return Err(ErrorCode::StageAlreadyExists(format!(
                    "Stage '{}' already exists.",
                    info.stage_name
                )));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_stage(&self, name: &str) -> Result<(u64, StageInfo)> {
        let ident = self.stage_ident(name);
        let res = self
            .kv_api
            .get_pb(&ident)
            .await
            .map_err(meta_service_error)?;
        let seq_value = res
            .ok_or_else(|| ErrorCode::UnknownStage(format!("Stage '{}' does not exist.", name)))?;

        Ok((seq_value.seq, seq_value.data))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_stages(&self) -> Result<Vec<StageInfo>> {
        let dir_name = DirName::new(self.stage_ident("dummy"));

        let values = self
            .kv_api
            .list_pb_values(ListOptions::unlimited(&dir_name))
            .await
            .map_err(meta_service_error)?;
        let stages = values.try_collect().await.map_err(meta_service_error)?;

        Ok(stages)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn drop_stage(&self, name: &str) -> Result<()> {
        let stage_ident = self.stage_ident(name);
        let file_key_prefix = self.stage_file_prefix(name);

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let stage_seq = match self
                .kv_api
                .get_kv(&stage_ident.to_string_key())
                .await
                .map_err(meta_service_error)?
            {
                Some(seq_v) => seq_v.seq,
                None => return Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name))),
            };

            // list all stage file keys, and delete them
            let file_keys = self
                .kv_api
                .list_kv_collect(ListOptions::unlimited(&file_key_prefix))
                .await
                .map_err(meta_service_error)?;
            let mut dels: Vec<TxnOp> = file_keys
                .iter()
                .map(|(key, _)| TxnOp::delete(key))
                .collect();
            dels.push(txn_del(&stage_ident));

            let txn_req = TxnRequest::new(
                vec![
                    // stage is not change, prevent add file to stage
                    txn_cond_eq_seq(&stage_ident, stage_seq),
                ],
                dels,
            );
            let tx_reply = self
                .kv_api
                .transaction(txn_req)
                .await
                .map_err(meta_service_error)?;
            let (succ, _) = unpack_txn_reply(tx_reply);

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_stage", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn update_stage(&self, stage: StageInfo, seq: u64) -> Result<()> {
        let ident = self.stage_ident(&stage.stage_name);
        let txn_req = TxnRequest::new(vec![txn_cond_eq_seq(&ident, seq)], vec![
            txn_put_pb(&ident, &stage)
                .map_err(|e| ErrorCode::IllegalUserStageFormat(e.to_string()))?,
        ]);
        let tx_reply = self
            .kv_api
            .transaction(txn_req)
            .await
            .map_err(meta_service_error)?;
        let (succ, _) = unpack_txn_reply(tx_reply);

        if succ {
            Ok(())
        } else {
            Err(ErrorCode::UnknownStage(format!(
                "Stage '{}' was modified concurrently, please retry.",
                stage.stage_name
            )))
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_file(&self, name: &str, file: StageFile) -> Result<u64> {
        let stage_ident = self.stage_ident(name);
        let file_ident = self.stage_file_ident(name, &file.path);

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            if let Some(_v) = self
                .kv_api
                .get_kv(&file_ident.to_string_key())
                .await
                .map_err(meta_service_error)?
            {
                return Err(ErrorCode::StageAlreadyExists(format!(
                    "Stage '{}' already exists.",
                    name,
                )));
            }
            let (stage_seq, mut old_stage): (_, StageInfo) = if let Some(seq_v) = self
                .kv_api
                .get_kv(&stage_ident.to_string_key())
                .await
                .map_err(meta_service_error)?
            {
                (
                    seq_v.seq,
                    deserialize_struct(&seq_v.data, ErrorCode::IllegalUserStageFormat, || "")?,
                )
            } else {
                return Err(ErrorCode::UnknownStage(format!(
                    "Stage '{}' does not exist.",
                    name
                )));
            };
            old_stage.number_of_files += 1;

            let txn_req = TxnRequest::new(
                vec![
                    // file does not exist
                    txn_cond_seq(&file_ident, Eq, 0),
                    // stage is not changed
                    txn_cond_seq(&stage_ident, Eq, stage_seq),
                ],
                vec![
                    txn_put_pb(&file_ident, &file)
                        .map_err(|e| ErrorCode::IllegalStageFileFormat(e.to_string()))?,
                    txn_put_pb(&stage_ident, &old_stage)
                        .map_err(|e| ErrorCode::IllegalUserStageFormat(e.to_string()))?,
                ],
            );

            let tx_reply = self
                .kv_api
                .transaction(txn_req)
                .await
                .map_err(meta_service_error)?;
            let (succ, _) = unpack_txn_reply(tx_reply);

            if succ {
                return Ok(0);
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("add_file", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn list_files(&self, name: &str) -> Result<Vec<StageFile>> {
        let dir_name = DirName::new(self.stage_file_ident(name, "dummy"));

        let values = self
            .kv_api
            .list_pb_values(ListOptions::unlimited(&dir_name))
            .await
            .map_err(meta_service_error)?;
        let files = values.try_collect().await.map_err(meta_service_error)?;

        Ok(files)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn remove_files(&self, name: &str, paths: Vec<String>) -> Result<()> {
        let stage_ident = self.stage_ident(name);

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let (stage_seq, mut old_stage): (_, StageInfo) = if let Some(seq_v) = self
                .kv_api
                .get_kv(&stage_ident.to_string_key())
                .await
                .map_err(meta_service_error)?
            {
                (
                    seq_v.seq,
                    deserialize_struct(&seq_v.data, ErrorCode::IllegalUserStageFormat, || "")?,
                )
            } else {
                return Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name)));
            };

            let mut if_then = Vec::with_capacity(paths.len());
            for path in &paths {
                let key = self.stage_file_ident(name, path);
                if_then.push(txn_del(&key));
            }
            old_stage.number_of_files -= paths.len() as u64;
            if_then.push(
                txn_put_pb(&stage_ident, &old_stage)
                    .map_err(|e| ErrorCode::IllegalUserStageFormat(e.to_string()))?,
            );

            let txn_req = TxnRequest::new(
                vec![
                    // stage is not change
                    txn_cond_seq(&stage_ident, Eq, stage_seq),
                ],
                if_then,
            );
            let tx_reply = self
                .kv_api
                .transaction(txn_req)
                .await
                .map_err(meta_service_error)?;
            let (succ, _) = unpack_txn_reply(tx_reply);

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("remove_files", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }
}
