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
use std::time::Duration;

use databend_common_grpc::RpcClientConf;
use databend_common_meta_client::errors::CreationError;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::errors::AcquireError;
use databend_common_meta_semaphore::Semaphore;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::MetaError;
use futures::stream::TryStreamExt;
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

impl Deref for MetaStore {
    type Target = Arc<ClientHandle>;

    fn deref(&self) -> &Self::Target {
        match self {
            MetaStore::L(l) => l.deref(),
            MetaStore::R(r) => r,
        }
    }
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

    pub async fn get_local_addr(&self) -> Result<String, MetaError> {
        let client_info = self.get_client_info().await?;
        Ok(client_info.client_addr)
    }

    pub async fn watch(&self, request: WatchRequest) -> Result<WatchStream, MetaError> {
        let client = self.deref();

        let streaming = client.request(request).await?;
        Ok(Box::pin(streaming.map_err(MetaError::from)))
    }

    pub async fn new_acquired(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let client = self.deref();
        Semaphore::new_acquired(client.clone(), prefix, capacity, id, lease).await
    }

    pub async fn new_acquired_by_time(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let client = self.deref();
        Semaphore::new_acquired_by_time(client.clone(), prefix, capacity, id, lease).await
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
                LocalMetaService::new("MetaStoreProvider-created")
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
