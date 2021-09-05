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
use common_exception::Result;
use common_metatypes::SeqValue;
use common_store_api::KVApi;

use crate::namespace::Namespace;
use crate::namespace::NamespaceApi;

#[allow(dead_code)]
pub static NAMESPACE_API_KEY_PREFIX: &str = "__fd_namespaces/";

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
}

#[async_trait]
impl<T: KVApi + Send> NamespaceApi for NamespaceMgr<T> {
    async fn add_namespace(&mut self, _tenant: String, _namespace: Namespace) -> Result<u64> {
        todo!()
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
