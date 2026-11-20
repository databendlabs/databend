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

//! A meta-service without keys: a read that finds a key with it was answered by a watched copy.

use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::protobuf::StreamItem;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;

pub(crate) struct EmptyKv;

#[async_trait::async_trait]
impl KVApi for EmptyKv {
    type Error = MetaError;

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, MetaError>>,
    ) -> Result<KVStream<MetaError>, MetaError> {
        Ok(keys.map_ok(|key| StreamItem::new(key, None)).boxed())
    }

    async fn list_kv(&self, _opts: ListOptions<'_, str>) -> Result<KVStream<MetaError>, MetaError> {
        Ok(stream::empty().boxed())
    }

    async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, MetaError> {
        unreachable!("watched reads do not write")
    }
}
