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

use std::sync::Arc;

use common_base::base::escape_for_key;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::txn_cond_seq;
use common_meta_api::txn_op_del;
use common_meta_api::txn_op_put;
use common_meta_api::KVApi;
use common_meta_types::app_error::TxnRetryMaxTimes;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::StageFile;
use common_meta_types::TxnOp;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReq;
use common_meta_types::UserStageInfo;

use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;
use crate::stage::StageApi;

static USER_STAGE_API_KEY_PREFIX: &str = "__fd_stages";
static STAGE_FILE_API_KEY_PREFIX: &str = "__fd_stage_files";
const TXN_MAX_RETRY_TIMES: u32 = 10;

pub struct StageMgr {
    kv_api: Arc<dyn KVApi>,
    stage_prefix: String,
    stage_file_prefix: String,
}

impl StageMgr {
    pub fn create(kv_api: Arc<dyn KVApi>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while role mgr create)",
            ));
        }

        Ok(StageMgr {
            kv_api,
            stage_prefix: format!("{}/{}", USER_STAGE_API_KEY_PREFIX, escape_for_key(tenant)?),
            stage_file_prefix: format!("{}/{}", STAGE_FILE_API_KEY_PREFIX, escape_for_key(tenant)?),
        })
    }
}

#[async_trait::async_trait]
impl StageApi for StageMgr {
    async fn add_stage(&self, info: UserStageInfo) -> Result<u64> {
        let seq = MatchSeq::Exact(0);
        let val = Operation::Update(serialize_struct(
            &info,
            ErrorCode::IllegalUserStageFormat,
            || "",
        )?);
        let key = format!(
            "{}/{}",
            self.stage_prefix,
            escape_for_key(&info.stage_name)?
        );
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, val, None));

        let res = upsert_info.await?.into_add_result()?;

        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Err(ErrorCode::StageAlreadyExists(format!(
                "Stage already exists, seq [{}]",
                v.seq
            ))),
        }
    }

    async fn get_stage(&self, name: &str, seq: Option<u64>) -> Result<SeqV<UserStageInfo>> {
        let key = format!("{}/{}", self.stage_prefix, escape_for_key(name)?);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownStage(format!("Unknown stage {}", name)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::new(
                seq_value.seq,
                deserialize_struct(&seq_value.data, ErrorCode::IllegalUserStageFormat, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name))),
        }
    }

    async fn get_stages(&self) -> Result<Vec<UserStageInfo>> {
        let values = self.kv_api.prefix_list_kv(&self.stage_prefix).await?;

        let mut stage_infos = Vec::with_capacity(values.len());
        for (_, value) in values {
            let stage_info =
                deserialize_struct(&value.data, ErrorCode::IllegalUserStageFormat, || "")?;
            stage_infos.push(stage_info);
        }
        Ok(stage_infos)
    }

    async fn drop_stage(&self, name: &str) -> Result<()> {
        let stage_key = format!("{}/{}", self.stage_prefix, escape_for_key(name)?);
        let file_key_prefix = format!("{}/{}/", self.stage_file_prefix, escape_for_key(name)?);

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let stage_seq = match self.kv_api.get_kv(&stage_key).await? {
                Some(seq_v) => seq_v.seq,
                None => return Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name))),
            };

            // list all stage file keys, and delete them
            let file_keys = self.kv_api.prefix_list_kv(&file_key_prefix).await?;
            let mut dels: Vec<TxnOp> = file_keys.iter().map(|(key, _)| txn_op_del(key)).collect();
            dels.push(txn_op_del(&stage_key));

            let txn_req = TxnRequest {
                condition: vec![
                    // stage is not change, prevent add file to stage
                    txn_cond_seq(&stage_key, Eq, stage_seq),
                ],
                if_then: dels,
                else_then: vec![],
            };
            let tx_reply = self.kv_api.transaction(txn_req).await?;
            let res: std::result::Result<_, MetaError> = tx_reply.into();
            let (succ, _) = res?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_stage", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    async fn add_file(&self, name: &str, file: StageFile) -> Result<u64> {
        let stage_key = format!("{}/{}", self.stage_prefix, escape_for_key(name)?);
        let file_key = format!(
            "{}/{}/{}",
            self.stage_file_prefix,
            escape_for_key(name)?,
            escape_for_key(&file.path)?
        );

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            if let Some(seq_v) = self.kv_api.get_kv(&file_key).await? {
                return Err(ErrorCode::StageFileAlreadyExists(format!(
                    "Stage file already exists, seq [{}]",
                    seq_v.seq
                )));
            }
            let (stage_seq, mut old_stage): (_, UserStageInfo) =
                if let Some(seq_v) = self.kv_api.get_kv(&stage_key).await? {
                    (
                        seq_v.seq,
                        deserialize_struct(&seq_v.data, ErrorCode::IllegalUserStageFormat, || "")?,
                    )
                } else {
                    return Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name)));
                };
            old_stage.number_of_files += 1;

            let txn_req = TxnRequest {
                condition: vec![
                    // file is not exists
                    txn_cond_seq(&file_key, Eq, 0),
                    // stage is not change
                    txn_cond_seq(&stage_key, Eq, stage_seq),
                ],
                if_then: vec![
                    txn_op_put(
                        &file_key,
                        serialize_struct(&file, ErrorCode::IllegalStageFileFormat, || "")?,
                    ),
                    txn_op_put(
                        &stage_key,
                        serialize_struct(&old_stage, ErrorCode::IllegalUserStageFormat, || "")?,
                    ),
                ],
                else_then: vec![],
            };
            let tx_reply = self.kv_api.transaction(txn_req).await?;
            let res: std::result::Result<_, MetaError> = tx_reply.into();
            let (succ, _) = res?;

            if succ {
                return Ok(0);
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("add_file", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    async fn list_files(&self, name: &str) -> Result<Vec<StageFile>> {
        let list_prefix = format!("{}/{}/", self.stage_file_prefix, escape_for_key(name)?);
        let values = self.kv_api.prefix_list_kv(&list_prefix).await?;
        let mut files = Vec::with_capacity(values.len());
        for (_, value) in values {
            let file = deserialize_struct(&value.data, ErrorCode::IllegalStageFileFormat, || "")?;
            files.push(file)
        }
        Ok(files)
    }

    async fn remove_files(&self, name: &str, paths: Vec<String>) -> Result<()> {
        let stage_key = format!("{}/{}", self.stage_prefix, escape_for_key(name)?);

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let (stage_seq, mut old_stage): (_, UserStageInfo) =
                if let Some(seq_v) = self.kv_api.get_kv(&stage_key).await? {
                    (
                        seq_v.seq,
                        deserialize_struct(&seq_v.data, ErrorCode::IllegalUserStageFormat, || "")?,
                    )
                } else {
                    return Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name)));
                };

            let mut if_then = Vec::with_capacity(paths.len());
            for path in &paths {
                let key = format!(
                    "{}/{}/{}",
                    self.stage_file_prefix,
                    escape_for_key(name)?,
                    escape_for_key(path)?
                );
                if_then.push(txn_op_del(&key));
            }
            old_stage.number_of_files -= paths.len() as u64;
            if_then.push(txn_op_put(
                &stage_key,
                serialize_struct(&old_stage, ErrorCode::IllegalUserStageFormat, || "")?,
            ));

            let txn_req = TxnRequest {
                condition: vec![
                    // stage is not change
                    txn_cond_seq(&stage_key, Eq, stage_seq),
                ],
                if_then,
                else_then: vec![],
            };
            let tx_reply = self.kv_api.transaction(txn_req).await?;
            let res: std::result::Result<_, MetaError> = tx_reply.into();
            let (succ, _) = res?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("remove_files", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }
}
