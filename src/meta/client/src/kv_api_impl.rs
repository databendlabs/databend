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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::ClientHandle;
use crate::Streamed;

#[tonic::async_trait]
impl kvapi::KVApi for ClientHandle {
    type Error = MetaError;

    #[fastrace::trace]
    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        let reply = self.request(act).await?;
        Ok(reply)
    }

    #[fastrace::trace]
    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        let keys = keys.to_vec();
        let strm = self.request(Streamed(MGetKVReq { keys })).await?;
        let strm = strm.map_err(MetaError::from);
        Ok(strm.boxed())
    }

    #[fastrace::trace]
    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        let strm = self
            .request(Streamed(ListKVReq {
                prefix: prefix.to_string(),
            }))
            .await?;

        let strm = strm.map_err(MetaError::from);
        Ok(strm.boxed())
    }

    #[fastrace::trace]
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let reply = self.request(txn).await?;
        Ok(reply)
    }
}
