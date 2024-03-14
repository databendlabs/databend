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

use databend_common_functions::is_builtin_function;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::UdfName;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::OnExist;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::NonEmptyStr;
use databend_common_meta_types::NonEmptyString;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::stream::TryStreamExt;

use crate::udf::UdfApiError;
use crate::udf::UdfError;

pub struct UdfMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: NonEmptyString,
}

impl UdfMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: NonEmptyStr) -> Self {
        UdfMgr {
            kv_api,
            tenant: tenant.into(),
        }
    }

    /// Add a UDF to /tenant/udf-name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn add_udf(
        &self,
        info: UserDefinedFunction,
        create_option: &OnExist,
    ) -> Result<Result<(), UdfError>, UdfApiError> {
        if let Err(e) = self.ensure_non_builtin(info.name.as_str()) {
            return Ok(Err(e));
        }

        let seq = MatchSeq::from(*create_option);

        let key = UdfName::new(self.tenant.as_str(), &info.name);
        let req = UpsertPB::insert(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if let OnExist::Error = create_option {
            if res.prev.is_some() {
                let err = UdfError::Exists {
                    tenant: self.tenant.to_string(),
                    name: info.name.to_string(),
                    reason: "".to_string(),
                };
                return Ok(Err(err));
            }
        }

        Ok(Ok(()))
    }

    /// Update a UDF to /tenant/udf-name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn update_udf(
        &self,
        info: UserDefinedFunction,
        seq: MatchSeq,
    ) -> Result<Result<u64, UdfError>, UdfApiError> {
        if let Err(e) = self.ensure_non_builtin(info.name.as_str()) {
            return Ok(Err(e));
        }

        let key = UdfName::new(self.tenant.as_str(), &info.name);
        let req = UpsertPB::update(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        let res = if res.is_changed() {
            Ok(res.result.unwrap().seq)
        } else {
            Err(UdfError::NotFound {
                tenant: self.tenant.to_string(),
                name: info.name.to_string(),
                context: "while update udf".to_string(),
            })
        };
        Ok(res)
    }

    /// Get UDF by name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn get_udf(
        &self,
        udf_name: &str,
    ) -> Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfName::new(self.tenant.as_str(), udf_name);
        let res = self.kv_api.get_pb(&key).await?;
        Ok(res)
    }

    /// Get all the UDFs for a tenant.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn list_udf(&self) -> Result<Vec<UserDefinedFunction>, UdfApiError> {
        let key = DirName::new(UdfName::new(self.tenant.as_str(), ""));
        let strm = self.kv_api.list_pb_values(&key).await?;
        let udfs = strm
            .try_collect()
            .await
            .map_err(|e| UdfApiError::MetaError {
                meta_err: e,
                context: "while list UDF".to_string(),
            })?;
        Ok(udfs)
    }

    /// Drop the tenant's UDF by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn drop_udf(
        &self,
        udf_name: &str,
        seq: MatchSeq,
    ) -> Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfName::new(self.tenant.as_str(), udf_name);
        let req = UpsertPB::delete(key).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if res.is_changed() {
            Ok(res.prev)
        } else {
            Ok(None)
        }
    }

    fn ensure_non_builtin(&self, name: &str) -> Result<(), UdfError> {
        if is_builtin_function(name) {
            return Err(UdfError::Exists {
                tenant: self.tenant.to_string(),
                name: name.to_string(),
                reason: " It is a builtin function".to_string(),
            });
        }
        Ok(())
    }
}
