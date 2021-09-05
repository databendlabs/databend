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
use common_metatypes::KVMeta;
use common_metatypes::MatchSeq;
use common_runtime::tokio;
use common_store_api::kv_api::MGetKVActionResult;
use common_store_api::kv_api::PrefixListReply;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use mockall::predicate::*;
use mockall::*;

use super::*;
use crate::namespace::namespace_mgr::NamespaceMgr;
use crate::namespace::namespace_mgr::NAMESPACE_API_KEY_PREFIX;

// and mock!
mock! {
    pub KV {}
    #[async_trait]
    impl KVApi for KV {
        async fn upsert_kv(
            &mut self,
            key: &str,
            seq: MatchSeq,
            value: Option<Vec<u8>>,
            value_meta: Option<KVMeta>
        ) -> Result<UpsertKVActionResult>;

    async fn get_kv(&mut self, key: &str) -> Result<GetKVActionResult>;

    async fn mget_kv(&mut self,key: &[String],) -> Result<MGetKVActionResult>;

    async fn prefix_list_kv(&mut self, prefix: &str) -> Result<PrefixListReply>;
    }
}

#[tokio::test]
async fn test_add_user() -> Result<()> {
    let id = "cluster1";
    let tenant = "tenant1";
    let key = format!("{}/{}/{}", NAMESPACE_API_KEY_PREFIX, tenant, id);
    let namespace = Namespace::new(id.to_string());
    let value = Some(serde_json::to_vec(&namespace)?);
    let seq = MatchSeq::Exact(0);

    // normal
    {
        let test_key = key.clone();
        let mut api = MockKV::new();
        api.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(seq),
                predicate::eq(value.clone()),
                predicate::eq(None),
            )
            .times(1)
            .return_once(|_u, _s, _, _| {
                Ok(UpsertKVActionResult {
                    prev: None,
                    result: None,
                })
            });

        let mut mgr = NamespaceMgr::new(api);
        let res = mgr.add_namespace(tenant.to_string(), namespace).await;

        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::UnknownException("").code()
        );
    }

    Ok(())
}
