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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::With;
use databend_meta::configs::Config;

pub enum KvApiCommand {
    Get(String),
    Upsert(UpsertKVReq),
    MGet(Vec<String>),
    List(String),
}

impl KvApiCommand {
    pub fn from_config(config: &Config, op: &str) -> Result<Self, String> {
        let api = match op {
            "upsert" => {
                if config.key.len() != 1 {
                    return Err("The number of keys must be 1".to_string());
                }

                let req = UpsertKVReq::update(config.key[0].as_str(), config.value.as_bytes());

                let req = if let Some(expire_after) = config.expire_after {
                    req.with(MetaSpec::new_ttl(std::time::Duration::from_secs(
                        expire_after,
                    )))
                } else {
                    req
                };

                Self::Upsert(req)
            }
            "delete" => {
                if config.key.len() != 1 {
                    return Err("The number of keys must be 1".to_string());
                }
                Self::Upsert(UpsertKVReq::delete(&config.key[0]))
            }
            "get" => {
                if config.key.len() != 1 {
                    return Err("The number of keys must be 1".to_string());
                }
                Self::Get(config.key[0].clone())
            }
            "mget" => Self::MGet(config.key.clone()),
            "list" => Self::List(config.prefix.clone()),
            _ => {
                return Err(format!("Unknown kv api command: {}", op));
            }
        };
        Ok(api)
    }

    pub async fn execute(
        &self,
        client: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    ) -> anyhow::Result<String> {
        let res_str = match self {
            KvApiCommand::Get(key) => {
                let res = client.get_kv(key.as_str()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApiCommand::Upsert(req) => {
                let res = client.upsert_kv(req.clone()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApiCommand::MGet(keys) => {
                let res = client.mget_kv(keys.as_slice()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApiCommand::List(prefix) => {
                let res = client.prefix_list_kv(prefix.as_str()).await?;
                serde_json::to_string_pretty(&res)?
            }
        };
        Ok(res_str)
    }
}
