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
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use common_functions::udfs::UDFFactory;
use common_functions::udfs::UDFParser;
use common_meta_api::KVApi;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVAction;
use common_meta_types::UserDefinedFunction;

use crate::udf::UdfMgrApi;

static UDF_API_KEY_PREFIX: &str = "__fd_udfs";

pub struct UdfMgr {
    kv_api: Arc<dyn KVApi>,
    tenant: String,
    udf_prefix: String,
}

impl UdfMgr {
    #[allow(dead_code)]
    pub fn new(kv_api: Arc<dyn KVApi>, tenant: &str) -> Self {
        UdfMgr {
            kv_api,
            tenant: tenant.to_owned(),
            udf_prefix: format!("{}/{}", UDF_API_KEY_PREFIX, tenant),
        }
    }

    fn is_builtin_function(name: &str) -> bool {
        FunctionFactory::instance().check(name) || AggregateFunctionFactory::instance().check(name)
    }
}

#[async_trait::async_trait]
impl UdfMgrApi for UdfMgr {
    async fn add_udf(&self, info: UserDefinedFunction) -> Result<u64> {
        if UdfMgr::is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UDFAlreadyExists(format!(
                "It's a builtin function: {}",
                info.name.as_str()
            )));
        }

        let mut udf_parser = UDFParser::default();
        udf_parser.parse_definition(
            &self.tenant,
            &info.name,
            &info.parameters,
            &info.definition,
        )?;

        let seq = MatchSeq::Exact(0);
        let val = Operation::Update(serde_json::to_vec(&info)?);
        let key = format!("{}/{}", self.udf_prefix, info.name);
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(&key, seq, val, None));

        let res = upsert_info.await?.into_add_result()?;

        match res.res {
            OkOrExist::Ok(v) => {
                UDFFactory::register(
                    self.tenant.as_str(),
                    info.name.as_str(),
                    &info.parameters,
                    info.definition.as_str(),
                )?;

                Ok(v.seq)
            }
            OkOrExist::Exists(v) => Err(ErrorCode::UDFAlreadyExists(format!(
                "UDF already exists, seq [{}]",
                v.seq
            ))),
        }
    }

    async fn update_udf(&self, info: UserDefinedFunction, seq: Option<u64>) -> Result<u64> {
        if UdfMgr::is_builtin_function(info.name.as_str()) {
            return Err(ErrorCode::UDFAlreadyExists(format!(
                "Builtin function can not be udpated: {}",
                info.name.as_str()
            )));
        }

        // Check if UDF is defined
        let _ = self.get_udf(info.name.as_str(), seq).await?;

        let val = Operation::Update(serde_json::to_vec(&info)?);
        let key = format!("{}/{}", self.udf_prefix, info.name);
        let upsert_info =
            self.kv_api
                .upsert_kv(UpsertKVAction::new(&key, MatchSeq::from(seq), val, None));

        let res = upsert_info.await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => {
                UDFFactory::register(
                    self.tenant.as_str(),
                    info.name.as_str(),
                    &info.parameters,
                    info.definition.as_str(),
                )?;

                Ok(s)
            }
            None => Err(ErrorCode::UnknownUDF(format!(
                "unknown UDF, or seq not match {}",
                info.name.clone()
            ))),
        }
    }

    async fn get_udf(&self, udf_name: &str, seq: Option<u64>) -> Result<SeqV<UserDefinedFunction>> {
        let key = format!("{}/{}", self.udf_prefix, udf_name);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownUDF(format!("Unknown UDF {}", udf_name)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownUser(format!("Unknown UDF {}", udf_name))),
        }
    }

    async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>> {
        let values = self.kv_api.prefix_list_kv(&self.udf_prefix).await?;

        let mut udfs = Vec::with_capacity(values.len());
        for (_, value) in values {
            let udf = serde_json::from_slice::<UserDefinedFunction>(&value.data)?;
            udfs.push(udf);
        }
        Ok(udfs)
    }

    async fn drop_udf(&self, udf_name: &str, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.udf_prefix, udf_name);
        let kv_api = self.kv_api.clone();
        let upsert_kv = async move {
            kv_api
                .upsert_kv(UpsertKVAction::new(
                    &key,
                    seq.into(),
                    Operation::Delete,
                    None,
                ))
                .await
        };
        let res = upsert_kv.await?;
        if res.prev.is_some() && res.result.is_none() {
            UDFFactory::unregister(self.tenant.as_str(), udf_name)?;

            Ok(())
        } else {
            Err(ErrorCode::UnknownUDF(format!("Unknown UDF {}", udf_name)))
        }
    }
}
