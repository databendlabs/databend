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
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::UdfName;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::stream::TryStreamExt;

use crate::errors::TenantError;

pub struct UdfMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: String,
}

impl UdfMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &str,
    ) -> std::result::Result<Self, TenantError> {
        if tenant.is_empty() {
            return Err(TenantError::CanNotBeEmpty {
                context: "create UdfMgr".to_string(),
            });
        }

        Ok(UdfMgr {
            kv_api,
            tenant: tenant.to_string(),
        })
    }

    /// Add a UDF to /tenant/udf-name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn add_udf(
        &self,
        info: UserDefinedFunction,
        create_option: &CreateOption,
    ) -> Result<()> {
        Self::ensure_non_builtin(info.name.as_str())?;

        let seq = MatchSeq::from(*create_option);

        let key = UdfName::new(&self.tenant, &info.name);
        let req = UpsertPB::insert(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

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

    /// Update a UDF to /tenant/udf-name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn update_udf(&self, info: UserDefinedFunction, seq: MatchSeq) -> Result<u64> {
        Self::ensure_non_builtin(info.name.as_str())?;

        let key = UdfName::new(&self.tenant, &info.name);
        let req = UpsertPB::update(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;
        if res.is_changed() {
            Ok(res.result.unwrap().seq)
        } else {
            Err(ErrorCode::UnknownUDF(format!(
                "UDF '{}' does not exist.",
                info.name
            )))
        }
    }

    /// Get UDF by name.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn get_udf(
        &self,
        udf_name: &str,
    ) -> std::result::Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfName::new(&self.tenant, udf_name);
        let res = self.kv_api.get_pb(&key).await?;
        Ok(res)
    }

    /// Get all the UDFs for a tenant.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>> {
        let key = DirName::new(UdfName::new(&self.tenant, ""));
        let strm = self.kv_api.list_pb_values(&key).await?;
        let udfs = strm.try_collect().await?;
        Ok(udfs)
    }

    /// Drop the tenant's UDF by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn drop_udf(
        &self,
        udf_name: &str,
        seq: MatchSeq,
    ) -> std::result::Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfName::new(&self.tenant, udf_name);
        let req = UpsertPB::delete(key).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if res.is_changed() {
            Ok(res.prev)
        } else {
            Ok(None)
        }
    }

    fn ensure_non_builtin(name: &str) -> Result<(), ErrorCode> {
        if is_builtin_function(name) {
            return Err(ErrorCode::UdfAlreadyExists(format!(
                "It's a builtin function: {}",
                name
            )));
        }
        Ok(())
    }
}
