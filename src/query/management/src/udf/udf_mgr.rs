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

use databend_common_base::base::escape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_functions::is_builtin_function;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;

use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;
use crate::udf::UdfApi;

static UDF_API_KEY_PREFIX: &str = "__fd_udfs";

pub struct UdfMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    udf_prefix: String,
}

impl UdfMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while udf mgr create)",
            ));
        }

        Ok(UdfMgr {
            kv_api,
            udf_prefix: format!("{}/{}", UDF_API_KEY_PREFIX, escape_for_key(tenant)?),
        })
    }
}

#[async_trait::async_trait]
impl UdfApi for UdfMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_udf(&self, info: UserDefinedFunction) -> Result<u64> {
        if is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UdfAlreadyExists(format!(
                "It's a builtin function: {}",
                info.name.as_str()
            )));
        }

        let seq = MatchSeq::Exact(0);
        let val = Operation::Update(serialize_struct(&info, ErrorCode::IllegalUDFFormat, || "")?);
        let key = format!("{}/{}", self.udf_prefix, escape_for_key(&info.name)?);
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, val, None));

        let res_seq = upsert_info.await?.added_seq_or_else(|v| {
            ErrorCode::UdfAlreadyExists(format!("UDF already exists, seq [{}]", v.seq))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_udf(&self, info: UserDefinedFunction, seq: MatchSeq) -> Result<u64> {
        if is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UdfAlreadyExists(format!(
                "Builtin function can not be updated: {}",
                info.name.as_str()
            )));
        }

        // Check if UDF is defined
        let _ = self.get_udf(info.name.as_str(), seq).await?;

        let val = Operation::Update(serialize_struct(&info, ErrorCode::IllegalUDFFormat, || "")?);
        let key = format!("{}/{}", self.udf_prefix, escape_for_key(&info.name)?);
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, val, None));

        let res = upsert_info.await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownUDF(format!(
                "Unknown Function, or seq not match {}",
                info.name.clone()
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_udf(&self, udf_name: &str, seq: MatchSeq) -> Result<SeqV<UserDefinedFunction>> {
        let key = format!("{}/{}", self.udf_prefix, escape_for_key(udf_name)?);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownUDF(format!("Unknown Function {}", udf_name)))?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::with_meta(
                seq_value.seq,
                seq_value.meta.clone(),
                deserialize_struct(&seq_value.data, ErrorCode::IllegalUDFFormat, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownUDF(format!(
                "Unknown Function {}",
                udf_name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>> {
        let values = self.kv_api.prefix_list_kv(&self.udf_prefix).await?;

        let mut udfs = Vec::with_capacity(values.len());
        for (name, value) in values {
            let udf = deserialize_struct(&value.data, ErrorCode::IllegalUDFFormat, || {
                format!("udf {name} is corrupt")
            })?;
            udfs.push(udf);
        }
        Ok(udfs)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_udf(&self, udf_name: &str, seq: MatchSeq) -> Result<()> {
        let key = format!("{}/{}", self.udf_prefix, escape_for_key(udf_name)?);
        let kv_api = self.kv_api.clone();
        let upsert_kv = async move {
            kv_api
                .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
                .await
        };
        let res = upsert_kv.await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUDF(format!(
                "Unknown Function {}",
                udf_name
            )))
        }
    }
}
