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
use common_metatypes::MatchSeq;
use common_metatypes::SeqValue;
use common_store_api::KVApi;

use crate::namespace::Namespace;
use crate::namespace::NamespaceApi;

#[allow(dead_code)]
pub static NAMESPACE_API_KEY_PREFIX: &str = "__fd_namespaces";

#[allow(dead_code)]
pub struct NamespaceMgr<KV> {
    kv_api: KV,
}

impl<T> NamespaceMgr<T>
where T: KVApi
{
    #[allow(dead_code)]
    pub fn new(kv_api: T) -> Self {
        NamespaceMgr { kv_api }
    }

    pub fn key_prefix(&self, prefixes: &[String]) -> String {
        let mut res = NAMESPACE_API_KEY_PREFIX.to_string();
        for prefix in prefixes {
            res.push('/');
            res.push_str(prefix.as_str());
        }
        res
    }
}

#[async_trait]
impl<T: KVApi + Send> NamespaceApi for NamespaceMgr<T> {
    async fn add_namespace(&mut self, tenant: String, namespace: Namespace) -> Result<u64> {
        let id = namespace.id.clone();
        let key = self.key_prefix(&[tenant, id]);
        let value = serde_json::to_vec(&namespace)?;

        // Only when there are no record, i.e. seq=0
        let match_seq = MatchSeq::Exact(0);

        let res = self
            .kv_api
            .upsert_kv(&key, match_seq, Some(value), None)
            .await?;

        match (res.prev, res.result) {
            (None, Some((s, _))) => Ok(s), // do we need to check the seq returned?
            (Some((s, _)), None) => Err(ErrorCode::UserAlreadyExists(format!(
                "Namespace already exists, seq [{}]",
                s
            ))),
            r @ (_, _) => Err(ErrorCode::UnknownException(format!(
                "upsert result not expected (using version 0, got {:?})",
                r
            ))),
        }
    }

    async fn get_namespace(
        &mut self,
        _tenant: String,
        _namespace_id: String,
        _seq: Option<u64>,
    ) -> Result<SeqValue<Namespace>> {
        todo!()
    }

    async fn get_all_namespaces(
        &mut self,
        _tenant: String,
        _seq: Option<u64>,
    ) -> Result<Vec<SeqValue<Namespace>>> {
        todo!()
    }

    async fn exists_namespace(
        &mut self,
        _tenant: String,
        _namespace_id: String,
        _seq: Option<u64>,
    ) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &mut self,
        _tenant: String,
        _namespace: Namespace,
        _seq: Option<u64>,
    ) -> Result<Option<u64>> {
        todo!()
    }

    async fn drop_namespace(
        &mut self,
        _tenant: String,
        _namespace_id: String,
        _seq: Option<u64>,
    ) -> Result<()> {
        todo!()
    }
}
