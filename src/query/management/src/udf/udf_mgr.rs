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
use databend_common_functions::is_builtin_function;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::principal::UdfName;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use futures::stream::TryStreamExt;

use crate::serde::serialize_struct;
use crate::udf::UdfApi;

pub struct UdfMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: String,
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
            tenant: tenant.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl UdfApi for UdfMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_udf(&self, info: UserDefinedFunction, create_option: &CreateOption) -> Result<()> {
        if is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UdfAlreadyExists(format!(
                "It's a builtin function: {}",
                info.name.as_str()
            )));
        }

        let seq = MatchSeq::from(*create_option);

        let key = UdfName::new(&self.tenant, &info.name);
        let value = serialize_struct(&info, ErrorCode::IllegalUDFFormat, || "")?;
        let req = UpsertKV::insert(key.to_string_key(), &value).with(seq);
        let res = self.kv_api.upsert_kv(req).await?;

        if let CreateOption::CreateIfNotExists(false) = create_option {
            if res.prev.is_some() {
                return Err(ErrorCode::UdfAlreadyExists(format!(
                    "UDF '{}' already exists.",
                    info.name
                )));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_udf(&self, info: UserDefinedFunction, seq: MatchSeq) -> Result<u64> {
        if is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UdfAlreadyExists(format!(
                "Cannot add UDF '{}': name conflicts with a built-in function.",
                info.name
            )));
        }

        // TODO: remove get_udf(), check if the UDF exists after upsert_kv()
        // Check if UDF is defined
        let seqv = self.get_udf(info.name.as_str()).await?;

        match seq.match_seq(&seqv) {
            Ok(_) => {}
            Err(_) => {
                return Err(ErrorCode::UnknownUDF(format!(
                    "UDF '{}' does not exist.",
                    &info.name
                )));
            }
        }

        let key = UdfName::new(&self.tenant, &info.name);
        // TODO: these logic are reppeated several times, consider to extract them.
        // TODO: add a new trait PBKVApi for the common logic that saves pb values in kvapi.
        let value = serialize_struct(&info, ErrorCode::IllegalUDFFormat, || "")?;
        let req = UpsertKV::update(key.to_string_key(), &value).with(seq);
        let res = self.kv_api.upsert_kv(req).await?;

        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownUDF(format!(
                "UDF '{}' does not exist.",
                info.name.clone()
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_udf(&self, udf_name: &str) -> Result<SeqV<UserDefinedFunction>> {
        // TODO: do not return ErrorCode, return UDFError

        let key = UdfName::new(&self.tenant, udf_name);
        let res = self.kv_api.get_pb(&key).await?;

        let seqv = res
            .ok_or_else(|| ErrorCode::UnknownUDF(format!("UDF '{}' does not exist.", udf_name)))?;

        Ok(seqv)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>> {
        let key = DirName::new(UdfName::new(&self.tenant, ""));
        let strm = self.kv_api.list_pb(&key).await?;
        let strm = strm.map_ok(|item| item.seqv.data);
        let udfs = strm.try_collect().await?;
        Ok(udfs)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_udf(&self, udf_name: &str, seq: MatchSeq) -> Result<()> {
        let key = UdfName::new(&self.tenant, udf_name);
        let req = UpsertKV::delete(key.to_string_key()).with(seq);
        let res = self.kv_api.upsert_kv(req).await?;

        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUDF(format!(
                "UDF '{}' does not exist.",
                udf_name
            )))
        }
    }
}
