use common_meta_api::KVApi;
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;

use crate::grpc::grpc_action::GetKVAction;
use crate::grpc::grpc_action::MGetKVAction;
use crate::grpc::grpc_action::PrefixListReq;
use crate::MetaGrpcClient;

#[async_trait::async_trait]
impl KVApi for MetaGrpcClient {
    async fn upsert_kv(
        &self,
        act: UpsertKVAction,
    ) -> common_exception::Result<UpsertKVActionReply> {
        self.do_write(act).await
    }

    async fn get_kv(&self, key: &str) -> common_exception::Result<GetKVActionReply> {
        self.do_get(GetKVAction {
            key: key.to_string(),
        })
        .await
    }

    async fn mget_kv(&self, keys: &[String]) -> common_exception::Result<MGetKVActionReply> {
        let keys = keys.to_vec();
        self.do_get(MGetKVAction { keys }).await
    }

    async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.do_get(PrefixListReq(prefix.to_string())).await
    }
}
