// Copyright 2020 Datafuse Labs.
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

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_metatypes::{MatchSeq, KVMeta};
use common_metatypes::SeqValue;
use common_store_api::{KVApi, UpsertKVActionResult};

use crate::namespace::NamespaceApi;
use crate::namespace::NodeInfo;
use std::time::{Duration, UNIX_EPOCH};
use std::ops::Add;

#[allow(dead_code)]
pub static NAMESPACE_API_KEY_PREFIX: &str = "__fd_namespaces";

#[allow(dead_code)]
pub struct NamespaceMgr<KV> {
    kv_api: KV,
    lift_time: Duration,
    namespace_prefix: String,
}

impl<T: KVApi> NamespaceMgr<T> {
    pub fn new(kv_api: T, tenant: &str, namespace: &str, lift_time: Duration) -> Result<Self> {
        Ok(NamespaceMgr {
            kv_api,
            lift_time,
            namespace_prefix: format!(
                "{}/{}/{}/databend_query",
                NAMESPACE_API_KEY_PREFIX,
                Self::escape_for_key(tenant)?,
                Self::escape_for_key(namespace)?
            ),
        })
    }

    fn escape_for_key(key: &str) -> Result<String> {
        let mut new_key = Vec::with_capacity(key.len());

        fn hex(num: u8) -> u8 {
            match num {
                0..=9 => b'0' + num,
                10..=15 => b'a' + (num - 10),
                unreachable => unreachable!("Unreachable branch num = {}", unreachable),
            }
        }

        for char in key.as_bytes() {
            match char {
                b'_' | b'a'..=b'z' | b'A'..=b'Z' => new_key.push(*char),
                _other => {
                    new_key.push(b'%');
                    new_key.push(hex(*char / 16));
                    new_key.push(hex(*char % 16));
                }
            }
        }

        Ok(String::from_utf8(new_key)?)
    }

    fn unescape_for_key(key: &str) -> Result<String> {
        let mut new_key = Vec::with_capacity(key.len());

        fn unhex(num: u8) -> u8 {
            match num {
                b'0'..=b'9' => num - b'0',
                b'a'..=b'f' => num - b'a',
                unreachable => unreachable!("Unreachable branch num = {}", unreachable),
            }
        }

        let bytes = key.as_bytes();

        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' => {
                    let mut num = unhex(bytes[index + 1]) * 16;
                    num += unhex(bytes[index + 2]);
                    new_key.push(num);
                    index += 3;
                },
                other => {
                    new_key.push(other);
                    index += 1;
                },
            }
        }

        Ok(String::from_utf8(new_key)?)
    }


    fn new_lift_time(&self) -> KVMeta {
        let now = std::time::SystemTime::now();
        let expire_at = now
            .add(self.lift_time)
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        KVMeta { expire_at: Some(expire_at.as_secs()) }
    }
}

#[async_trait]
impl<T: KVApi + Send> NamespaceApi for NamespaceMgr<T> {
    async fn add_node(&mut self, node: NodeInfo) -> Result<u64> {
        // Only when there are no record, i.e. seq=0
        let seq = MatchSeq::Exact(0);
        let meta = Some(self.new_lift_time());
        let value = Some(serde_json::to_vec(&node)?);
        let node_key = format!("{}/{}", self.namespace_prefix, Self::escape_for_key(&node.id)?);
        let upsert_node = self.kv_api.upsert_kv(&node_key, seq, value, meta);


        match upsert_node.await? {
            UpsertKVActionResult { prev: None, result: Some((s, _)) } => Ok(s),
            UpsertKVActionResult { prev: Some((s, _)), result: None } => Err(
                ErrorCode::NamespaceNodeAlreadyExists(format!(
                    "Namespace already exists, seq [{}]", s
                ))
            ),
            catch_result @ UpsertKVActionResult { .. } => Err(
                ErrorCode::UnknownException(format!(
                    "upsert result not expected (using version 0, got {:?})", catch_result
                ))
            )
        }
    }

    async fn get_nodes(&mut self) -> Result<Vec<NodeInfo>> {
        let values = self.kv_api.prefix_list_kv(&self.namespace_prefix).await?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, (_, value)) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.value)?;
            node_info.id = Self::unescape_for_key(&node_key)?;
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    async fn drop_node(&mut self, node_id: String, seq: Option<u64>) -> Result<()> {
        let node_key = format!("{}/{}", self.namespace_prefix, Self::escape_for_key(&node_id)?);
        let upsert_node = self.kv_api.upsert_kv(&node_key, seq.into(), None, None);

        match upsert_node.await? {
            UpsertKVActionResult { prev: Some(_), result: None } => Ok(()),
            UpsertKVActionResult { .. } => Err(ErrorCode::NamespaceUnknownNode(
                format!("unknown node {:?}", node_id)
            ))
        }
    }

    async fn heartbeat(&mut self, node_id: String, seq: Option<u64>) -> Result<u64> {
        let meta = Some(self.new_lift_time());
        let node_key = format!("{}/{}", self.namespace_prefix, Self::escape_for_key(&node_id)?);
        match seq {
            None => {
                let seq = MatchSeq::GE(1);
                let upsert_meta = self.kv_api.update_kv_meta(&node_key, seq, meta);

                match upsert_meta.await? {
                    UpsertKVActionResult { prev: Some(_), result: Some((s, _)) } => Ok(s),
                    UpsertKVActionResult { .. } => Err(ErrorCode::NamespaceUnknownNode(
                        format!("unknown node {:?}", node_id)
                    ))
                }
            }
            Some(exact) => {
                let seq = MatchSeq::Exact(exact);
                let upsert_meta = self.kv_api.update_kv_meta(&node_key, seq, meta);

                match upsert_meta.await? {
                    UpsertKVActionResult { prev: Some(_), result: Some((s, _)) } => Ok(s),
                    UpsertKVActionResult { .. } => Err(ErrorCode::NamespaceUnknownNode(
                        format!("unknown node {:?}", node_id)
                    ))
                }
            }
        }
    }
}
