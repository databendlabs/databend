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
use databend_common_functions::is_builtin_function;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::UdfIdent;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::TryStreamExt;

use crate::udf::UdfApiError;
use crate::udf::UdfError;

pub struct UdfMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl UdfMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        UdfMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    /// Add a UDF to /tenant/udf-name.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn add_udf(
        &self,
        info: UserDefinedFunction,
        create_option: &CreateOption,
    ) -> Result<Result<(), UdfError>, UdfApiError> {
        if let Err(e) = self.ensure_non_builtin(info.name.as_str()) {
            return Ok(Err(e));
        }

        let seq = MatchSeq::from(*create_option);

        let key = UdfIdent::new(&self.tenant, &info.name);
        let req = UpsertPB::insert(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if let CreateOption::Create = create_option {
            if res.prev.is_some() {
                let err = UdfError::Exists {
                    tenant: self.tenant.tenant_name().to_string(),
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
    #[fastrace::trace]
    pub async fn update_udf(
        &self,
        info: UserDefinedFunction,
        seq: MatchSeq,
    ) -> Result<Result<u64, UdfError>, UdfApiError> {
        if let Err(e) = self.ensure_non_builtin(info.name.as_str()) {
            return Ok(Err(e));
        }

        let key = UdfIdent::new(&self.tenant, &info.name);
        let req = UpsertPB::update(key, info.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        let res = if res.is_changed() {
            Ok(res.result.unwrap().seq)
        } else {
            Err(UdfError::NotFound {
                tenant: self.tenant.tenant_name().to_string(),
                name: info.name.to_string(),
                context: "while update udf".to_string(),
            })
        };
        Ok(res)
    }

    /// Get UDF by name.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_udf(
        &self,
        udf_name: &str,
    ) -> Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfIdent::new(&self.tenant, udf_name);
        let res = self.kv_api.get_pb(&key).await?;
        Ok(res)
    }

    /// Get all the UDFs for a tenant.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn list_udf(&self) -> Result<Vec<UserDefinedFunction>, ErrorCode> {
        let key = DirName::new(UdfIdent::new(&self.tenant, ""));
        let strm = self.kv_api.list_pb_values(&key).await?;

        match strm.try_collect().await {
            Ok(udfs) => Ok(udfs),
            Err(_) => self.list_udf_fallback().await,
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn list_udf_fallback(&self) -> Result<Vec<UserDefinedFunction>, ErrorCode> {
        let key = UdfIdent::new(&self.tenant, "");
        let values = self.kv_api.prefix_list_kv(&key.to_string_key()).await?;

        let mut udfs = Vec::with_capacity(values.len());
        // At begin udf is serialize to json. https://github.com/datafuselabs/databend/pull/12729/files#diff-9c992028e59caebc313d761b8488b17f142618fb89db64c51c1655689d68c41b
        // But we can not deserialize the UserDefinedFunction from json now,
        // because add a new field created_on and the field `definition` refactor to a ENUM type.
        for (name, value) in values {
            let udf = crate::deserialize_struct(&value.data, ErrorCode::IllegalUDFFormat, || {
                format!(
                    "Encountered invalid json data for LambdaUDF '{}', \
                please drop this invalid udf and re-create it. \n\
                Example: `DROP FUNCTION <invalid_udf>;` then `CREATE FUNCTION <invalid_udf> AS <udf_definition>;`\n\
                ",
                    name
                )
            })?;
            udfs.push(udf);
        }
        Ok(udfs)
    }

    /// Drop the tenant's UDF by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn drop_udf(
        &self,
        udf_name: &str,
        seq: MatchSeq,
    ) -> Result<Option<SeqV<UserDefinedFunction>>, MetaError> {
        let key = UdfIdent::new(&self.tenant, udf_name);
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
                tenant: self.tenant.tenant_name().to_string(),
                name: name.to_string(),
                reason: " It is a builtin function".to_string(),
            });
        }
        Ok(())
    }
}
