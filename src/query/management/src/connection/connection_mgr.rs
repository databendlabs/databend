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
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;

use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;
use crate::ConnectionApi;

static USER_CONNECTION_API_KEY_PREFIX: &str = "__fd_connection";

pub struct ConnectionMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    connection_prefix: String,
}

impl ConnectionMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while role mgr create)",
            ));
        }

        Ok(Self {
            kv_api,
            connection_prefix: format!(
                "{}/{}",
                USER_CONNECTION_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
        })
    }
}

#[async_trait::async_trait]
impl ConnectionApi for ConnectionMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_connection(&self, info: UserDefinedConnection) -> Result<u64> {
        let seq = MatchSeq::Exact(0);
        let val = Operation::Update(serialize_struct(
            &info,
            ErrorCode::IllegalConnection,
            || "",
        )?);
        let key = format!("{}/{}", self.connection_prefix, escape_for_key(&info.name)?);
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, val, None));

        let res_seq = upsert_info.await?.added_seq_or_else(|_v| {
            ErrorCode::ConnectionAlreadyExists(format!(
                "Connection '{}' already exists.",
                info.name
            ))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_connection(
        &self,
        name: &str,
        seq: MatchSeq,
    ) -> Result<SeqV<UserDefinedConnection>> {
        let key = format!("{}/{}", self.connection_prefix, escape_for_key(name)?);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownConnection(format!("Connection '{}' not found.", name))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::new(
                seq_value.seq,
                deserialize_struct(&seq_value.data, ErrorCode::IllegalConnection, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownConnection(format!(
                "Connection '{}' not found.",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_connections(&self) -> Result<Vec<UserDefinedConnection>> {
        let values = self.kv_api.prefix_list_kv(&self.connection_prefix).await?;

        let mut connection_infos = Vec::with_capacity(values.len());
        for (_, value) in values {
            let connection_info =
                deserialize_struct(&value.data, ErrorCode::IllegalConnection, || "")?;
            connection_infos.push(connection_info);
        }
        Ok(connection_infos)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_connection(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let key = format!("{}/{}", self.connection_prefix, escape_for_key(name)?);
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
            Err(ErrorCode::UnknownConnection(format!(
                "Connection '{}' not found.",
                name
            )))
        }
    }
}
