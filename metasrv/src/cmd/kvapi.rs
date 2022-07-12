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

use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVReq;

use crate::configs::Config;

pub enum KvApi {
    Get(String),
    Upsert(UpsertKVReq),
    MGet(Vec<String>),
    List(String),
}

impl KvApi {
    pub fn from_config(config: &Config, op: &str) -> std::result::Result<Self, String> {
        let api = match op {
            "upsert" => {
                if config.key.len() != 1 {
                    return Err("The number of keys must be 1".to_string());
                }
                Self::Upsert(UpsertKVReq::new(
                    config.key[0].as_str(),
                    MatchSeq::Any,
                    Operation::Update(config.value.clone().into_bytes()),
                    None,
                ))
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

    pub async fn execute(&self, client: Arc<dyn KVApi>) -> Result<String> {
        let res_str = match self {
            KvApi::Get(key) => {
                let res = client.get_kv(key.as_str()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApi::Upsert(req) => {
                let res = client.upsert_kv(req.clone()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApi::MGet(keys) => {
                let res = client.mget_kv(keys.as_slice()).await?;
                serde_json::to_string_pretty(&res)?
            }
            KvApi::List(prefix) => {
                let res = client.prefix_list_kv(prefix.as_str()).await?;
                serde_json::to_string_pretty(&res)?
            }
        };
        Ok(res_str)
    }
}
