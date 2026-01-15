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
use databend_common_meta_kvapi::kvapi::ListOptions;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::limit_stream;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;

use crate::ClientHandle;
use crate::Streamed;

#[tonic::async_trait]
impl kvapi::KVApi for ClientHandle {
    type Error = MetaError;

    #[fastrace::trace]
    async fn upsert_kv(&self, act: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
        let reply = self.upsert_via_txn(act).await?;
        Ok(reply)
    }

    #[fastrace::trace]
    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let strm = self
            .request(Streamed(ListKVReq {
                prefix: opts.prefix.to_string(),
            }))
            .await?;

        let strm = strm.map_err(MetaError::from);
        Ok(limit_stream(strm, opts.limit))
    }

    #[fastrace::trace]
    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        use databend_common_meta_kvapi::kvapi::fail_fast;

        // For remote client, collect keys first then use batch request.
        // fail_fast stops at first error; we save it to append to output stream.
        let mut collected = Vec::new();
        let mut input_error = None;
        let mut keys = std::pin::pin!(fail_fast(keys));
        while let Some(result) = keys.next().await {
            match result {
                Ok(key) => collected.push(key),
                Err(e) => input_error = Some(e),
            }
        }

        // Make batch request for successfully collected keys
        let strm = self
            .request(Streamed(MGetKVReq { keys: collected }))
            .await?;
        let strm = strm.map_err(MetaError::from);

        // If there was an input error, append it to the output stream
        match input_error {
            None => Ok(strm.boxed()),
            Some(e) => Ok(strm.chain(futures::stream::once(async { Err(e) })).boxed()),
        }
    }

    #[fastrace::trace]
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let reply = self.request(txn).await?;
        Ok(reply)
    }
}
