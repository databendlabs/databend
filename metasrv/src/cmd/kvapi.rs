// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::RpcClientConf;
use common_meta_api::KVApi;
use common_meta_store::MetaStoreProvider;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVReq;

use crate::configs::Config;

#[async_trait::async_trait]
trait KvOp {
    async fn do_op(&self) -> Result<String>;
}

struct KvGet {
    client: Arc<dyn KVApi>,
    key: String,
}

#[async_trait::async_trait]
impl KvOp for KvGet {
    async fn do_op(&self) -> Result<String> {
        let res = self.client.get_kv(self.key.as_str()).await?;
        Ok(serde_json::to_string_pretty(&res)?)
    }
}

struct KvUpsert {
    client: Arc<dyn KVApi>,
    key: String,
    value: String,
}

#[async_trait::async_trait]
impl KvOp for KvUpsert {
    async fn do_op(&self) -> Result<String> {
        let res = self
            .client
            .upsert_kv(UpsertKVReq::new(
                self.key.as_str(),
                MatchSeq::Any,
                Operation::Update(self.value.clone().into_bytes()),
                None,
            ))
            .await?;
        Ok(serde_json::to_string_pretty(&res)?)
    }
}

struct KvMGet {
    client: Arc<dyn KVApi>,
    keys: Vec<String>,
}

#[async_trait::async_trait]
impl KvOp for KvMGet {
    async fn do_op(&self) -> Result<String> {
        let res = self.client.mget_kv(self.keys.as_slice()).await?;
        Ok(serde_json::to_string_pretty(&res)?)
    }
}

struct KvList {
    client: Arc<dyn KVApi>,
    prefix: String,
}

#[async_trait::async_trait]
impl KvOp for KvList {
    async fn do_op(&self) -> Result<String> {
        let res = self.client.prefix_list_kv(self.prefix.as_str()).await?;
        Ok(serde_json::to_string_pretty(&res)?)
    }
}

pub struct KvApiCommand {
    kv_op: Box<dyn KvOp>,
}

impl KvApiCommand {
    pub async fn create(config: &Config, op: &str) -> Result<Self> {
        let rpc_conf = RpcClientConf {
            address: config.grpc_api_address.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            ..Default::default()
        };
        let client = MetaStoreProvider::new(rpc_conf)
            .try_get_meta_store()
            .await?
            .arc();

        let kv_op: Box<dyn KvOp> = match op {
            "upsert" => {
                if config.key.len() != 1 {
                    return Err(ErrorCode::BadArguments("The number of keys must be 1"));
                }
                Box::new(KvUpsert {
                    client,
                    key: config.key[0].clone(),
                    value: config.value.clone(),
                })
            }
            "get" => {
                if config.key.len() != 1 {
                    return Err(ErrorCode::BadArguments("The number of keys must be 1"));
                }
                Box::new(KvGet {
                    client,
                    key: config.key[0].clone(),
                })
            }
            "mget" => Box::new(KvMGet {
                client,
                keys: config.key.clone(),
            }),
            "list" => Box::new(KvList {
                client,
                prefix: config.prefix.clone(),
            }),
            _ => {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Unknown kv api command: {}",
                    op
                )));
            }
        };

        Ok(KvApiCommand { kv_op })
    }

    pub async fn do_op(&self) -> Result<String> {
        self.kv_op.do_op().await
    }
}
