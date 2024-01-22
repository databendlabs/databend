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

use async_trait::async_trait;
use databend_common_base::base::tokio;
use databend_common_management::*;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::ListKVReply;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use mockall::predicate::*;
use mockall::*;

// and mock!
mock! {
    pub KV {}
    #[async_trait]
    impl kvapi::KVApi for KV {
        type Error = MetaError;

        async fn upsert_kv(
            &self,
            act: UpsertKVReq,
        ) -> Result<UpsertKVReply, MetaError>;

        async fn get_kv(&self, key: &str) -> Result<GetKVReply,MetaError>;

        async fn mget_kv(
            &self,
            key: &[String],
        ) -> Result<MGetKVReply,MetaError>;

        async fn get_kv_stream(&self, key: &[String]) -> Result<KVStream<MetaError>, MetaError>;

        async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError>;

        async fn list_kv(&self, prefix: &str) -> Result<KVStream<MetaError>, MetaError>;

        async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError>;

        }
}

fn make_role_key(role: &str) -> String {
    format!("__fd_roles/admin/{}", role)
}

mod add {
    use databend_common_exception::ErrorCode;
    use databend_common_meta_app::principal::RoleInfo;
    use databend_common_meta_kvapi::kvapi::KVApi;
    use databend_common_meta_types::Operation;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_json_upgrade_to_pb() -> databend_common_exception::Result<()> {
        let role_name = "role1";
        let (kv_api, role_api) = new_role_api().await?;

        let role_key = make_role_key(role_name);
        let role_info = RoleInfo::new(role_name);

        let v = serde_json::to_vec(&role_info)?;
        let kv_api = kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVReq::new(
            &role_key,
            MatchSeq::Exact(0),
            Operation::Update(v),
            None,
        ));
        upsert_kv.await?.added_seq_or_else(|_v| {
            ErrorCode::RoleAlreadyExists(format!("Role '{}' already exists.", role_info.name))
        })?;

        let get = role_api
            .get_role(&role_name.to_owned(), MatchSeq::GE(1))
            .await?
            .data;

        assert_eq!("role1".to_string(), get.name);

        Ok(())
    }
}

async fn new_role_api() -> databend_common_exception::Result<(Arc<MetaEmbedded>, RoleMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = RoleMgr::create(test_api.clone(), "admin")?;
    Ok((test_api, mgr))
}
