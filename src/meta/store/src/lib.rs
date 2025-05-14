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

pub(crate) mod local;

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use databend_common_grpc::RpcClientConf;
use databend_common_meta_client::errors::CreationError;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::errors::AcquireError;
use databend_common_meta_semaphore::Semaphore;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
pub use local::LocalMetaService;
use log::info;
use tokio_stream::Stream;

pub type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchResponse, MetaError>> + Send + 'static>>;

#[derive(Clone)]
pub struct MetaStoreProvider {
    rpc_conf: RpcClientConf,
}

/// MetaStore is impl with either a local meta-service, or a grpc-client of metasrv
#[derive(Clone)]
pub enum MetaStore {
    L(Arc<LocalMetaService>),
    R(Arc<ClientHandle>),
}

impl MetaStore {
    /// Create a local meta service for testing.
    ///
    /// It is required to assign a base port as the port number range.
    pub async fn new_local_testing() -> Self {
        MetaStore::L(Arc::new(
            LocalMetaService::new("MetaStore-new-local-testing")
                .await
                .unwrap(),
        ))
    }

    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn is_local(&self) -> bool {
        match self {
            MetaStore::L(_) => true,
            MetaStore::R(_) => false,
        }
    }

    pub async fn get_local_addr(&self) -> std::result::Result<Option<String>, MetaError> {
        let client = match self {
            MetaStore::L(l) => l.deref().deref(),
            MetaStore::R(grpc_client) => grpc_client,
        };

        let client_info = client.get_client_info().await?;
        Ok(Some(client_info.client_addr))
    }

    pub async fn watch(&self, request: WatchRequest) -> Result<WatchStream, MetaError> {
        let client = match self {
            MetaStore::L(l) => l.deref(),
            MetaStore::R(grpc_client) => grpc_client,
        };

        let streaming = client.request(request).await?;
        Ok(Box::pin(WatchResponseStream::create(streaming)))
    }

    pub async fn new_acquired(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let client = match self {
            MetaStore::L(l) => l.deref(),
            MetaStore::R(grpc_client) => grpc_client,
        };

        Semaphore::new_acquired(client.clone(), prefix, capacity, id, lease).await
    }

    pub async fn new_acquired_by_time(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let client = match self {
            MetaStore::L(l) => l.deref(),
            MetaStore::R(grpc_client) => grpc_client,
        };

        Semaphore::new_acquired_by_time(client.clone(), prefix, capacity, id, lease).await
    }
}

#[async_trait::async_trait]
impl kvapi::KVApi for MetaStore {
    type Error = MetaError;

    async fn upsert_kv(&self, act: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
        match self {
            MetaStore::L(x) => x.upsert_kv(act).await,
            MetaStore::R(x) => x.upsert_kv(act).await,
        }
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        match self {
            MetaStore::L(x) => x.get_kv_stream(keys).await,
            MetaStore::R(x) => x.get_kv_stream(keys).await,
        }
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        match self {
            MetaStore::L(x) => x.list_kv(prefix).await,
            MetaStore::R(x) => x.list_kv(prefix).await,
        }
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        match self {
            MetaStore::L(x) => x.transaction(txn).await,
            MetaStore::R(x) => x.transaction(txn).await,
        }
    }
}

impl MetaStoreProvider {
    pub fn new(rpc_conf: RpcClientConf) -> Self {
        MetaStoreProvider { rpc_conf }
    }

    pub async fn create_meta_store(&self) -> Result<MetaStore, CreationError> {
        if self.rpc_conf.local_mode() {
            info!(
                conf :? =(&self.rpc_conf);
                "use embedded meta, data will be removed when process exits"
            );

            // NOTE: This can only be used for test: data will be removed when program quit.
            Ok(MetaStore::L(Arc::new(
                LocalMetaService::new_with_fixed_dir(
                    self.rpc_conf.embedded_dir.clone(),
                    "MetaStoreProvider-created",
                )
                .await
                .unwrap(),
            )))
        } else {
            info!(conf :? =(&self.rpc_conf); "use remote meta");
            let client = MetaGrpcClient::try_new(&self.rpc_conf)?;
            Ok(MetaStore::R(client))
        }
    }
}

pub struct WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    inner: S,
}

impl<E, S> WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    pub fn create(inner: S) -> WatchResponseStream<E, S> {
        WatchResponseStream { inner }
    }
}

impl<E, S> Stream for WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    type Item = Result<WatchResponse, MetaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx).map(|x| match x {
            None => None,
            Some(Ok(resp)) => Some(Ok(resp)),
            Some(Err(e)) => Some(Err(e.into())),
        })
    }
}
