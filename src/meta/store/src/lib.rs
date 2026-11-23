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

databend_common_tracing::register_module_tag!("[META_CLIENT]", "databend_meta_client");

pub(crate) mod local;

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_meta::runtime_api::RuntimeApi;
use databend_meta_client::ClientHandle;
use databend_meta_client::MGetKVReq;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::RpcClientConf;
use databend_meta_client::Streamed;
use databend_meta_client::errors::CreationError;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::fail_fast;
use databend_meta_client::kvapi::limit_stream;
use databend_meta_client::types::Change;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::WatchResponse;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_plugin_semaphore::errors::AcquireError;
use databend_meta_runtime::DatabendRuntime;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
pub use local::LocalMetaService;
use log::info;
use log::warn;
use tokio::time::Instant;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio_stream::Stream;

pub type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchResponse, MetaError>> + Send + 'static>>;

#[derive(Clone)]
pub struct MetaStoreProvider {
    rpc_conf: RpcClientConf,
}

/// MetaStore is impl with either a local meta-service, or a grpc-client of metasrv.
///
/// `MetaStore` implements `KVApi` directly (not via `Deref`) to decouple the KV API
/// from the underlying gRPC client. This allows `ClientHandle` to be a pure
/// communication layer without depending on `kvapi`.
#[derive(Clone)]
pub enum MetaStore {
    L(Arc<LocalMetaService>),
    R(Arc<ClientHandle<DatabendRuntime>>),
}

impl MetaStore {
    /// Returns a reference to the inner `ClientHandle`.
    ///
    /// This provides access to the underlying gRPC client for operations
    /// not covered by `KVApi`.
    pub fn inner(&self) -> &Arc<ClientHandle<DatabendRuntime>> {
        match self {
            MetaStore::L(l) => l.deref(),
            MetaStore::R(grpc_client) => grpc_client,
        }
    }

    /// Create a local meta service for testing.
    pub async fn new_local_testing<RT: RuntimeApi>() -> Self {
        MetaStore::L(Arc::new(
            LocalMetaService::new_testing::<RT>("MetaStore-new-local-testing")
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
        let client_info = self.inner().get_client_info().await?;
        Ok(client_info.client_addr)
    }

    /// Watch for changes on a key prefix.
    ///
    /// This method delegates to the underlying `ClientHandle::watch`.
    pub async fn watch(&self, watch: WatchRequest) -> Result<WatchStream, MetaError> {
        let strm = self.inner().watch(watch).await?;
        let strm = strm.map_err(MetaError::from);
        Ok(Box::pin(strm))
    }

    pub async fn new_acquired(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let client = self.inner();
        Semaphore::new_acquired(client.clone(), prefix, capacity, id, lease).await
    }

    pub async fn new_acquired_by_time(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
        retry_timeout: Option<Duration>,
    ) -> Result<Permit, AcquireError> {
        let prefix = prefix.to_string();
        let id = id.to_string();

        let name = format!("Semaphore-{prefix}-{id}");

        let permit = retry_acquire(&name, move || {
            self.do_new_acquired_by_time(prefix.clone(), capacity, id.clone(), lease, retry_timeout)
        })
        .await;
        Ok(permit)
    }

    pub async fn do_new_acquired_by_time(
        &self,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
        retry_timeout: Option<Duration>,
    ) -> Result<Result<Permit, AcquireError>, Elapsed> {
        let client = self.inner().clone();

        let timestamp = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let retry_timeout = retry_timeout.unwrap_or(Duration::from_secs(365 * 86400));

        timeout(
            retry_timeout,
            Semaphore::new_acquired_by_time(client, prefix, capacity, id, timestamp, lease),
        )
        .await
    }
}

/// The delay before the first retry of a failed attempt to acquire a semaphore. It doubles with
/// each failed attempt in a row, up to [`ACQUIRE_RETRY_MAX_DELAY`].
const ACQUIRE_RETRY_MIN_DELAY: Duration = Duration::from_millis(100);

const ACQUIRE_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

/// Run `attempt` until it acquires, and return what it acquired.
///
/// An attempt that timed out is retried at once: it lasted its whole retry timeout. An attempt
/// that failed is retried after a delay. It can fail as soon as it starts, such as when no
/// meta-service node serves its watch during a leader election, and each attempt writes a
/// permit entry and deletes it.
async fn retry_acquire<T, F, Fut>(name: &str, mut attempt: F) -> T
where
    T: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Result<T, AcquireError>, Elapsed>>,
{
    let mut delay = ACQUIRE_RETRY_MIN_DELAY;

    let mut ith = 0;
    loop {
        ith += 1;

        let start = Instant::now();

        let timeout_res = attempt().await;

        info!(
            "{} attempt {} to acquire semaphore, result:{:?}",
            name, ith, timeout_res
        );

        let res = match timeout_res {
            Ok(acquire_res) => acquire_res,
            Err(_elapsed) => {
                warn!(
                    "{} attempt {} to acquire semaphore; timeout after: {:?}; retry at once",
                    name,
                    ith,
                    start.elapsed()
                );
                continue;
            }
        };

        match res {
            Ok(acquired) => return acquired,
            Err(err) => {
                warn!(
                    "{} attempt {} to acquire semaphore failed: {:?}; retry in {:?}",
                    name, ith, err, delay
                );
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, ACQUIRE_RETRY_MAX_DELAY);
            }
        }
    }
}

impl MetaStoreProvider {
    pub fn new(rpc_conf: RpcClientConf) -> Self {
        MetaStoreProvider { rpc_conf }
    }

    /// [`Self::create_meta_store`] for the production runtime.
    ///
    /// The embedded meta-service behind it is a whole openraft node. An `async fn`'s state
    /// machine is code-generated in every crate that polls it, so returning it as a boxed
    /// `dyn Future` makes this crate the only one that generates the raft code; callers only go
    /// through the vtable.
    pub fn create_default_meta_store(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, CreationError>> + Send + '_>> {
        Box::pin(self.create_meta_store::<DatabendRuntime>())
    }

    pub async fn create_meta_store<RT: RuntimeApi>(&self) -> Result<MetaStore, CreationError> {
        if self.rpc_conf.local_mode() {
            let local = match &self.rpc_conf.embedded_dir {
                Some(dir) => {
                    info!(conf :? =(&self.rpc_conf), path = dir; "use persistent embedded meta");
                    LocalMetaService::new_with_fixed_dir::<RT>(
                        dir.clone(),
                        "MetaStoreProvider-created",
                    )
                    .await
                }
                None => {
                    info!(
                        conf :? =(&self.rpc_conf);
                        "use temporary embedded meta; data will be removed when the service exits"
                    );
                    LocalMetaService::new_testing::<RT>("MetaStoreProvider-created").await
                }
            };
            Ok(MetaStore::L(Arc::new(local.unwrap())))
        } else {
            info!(conf :? =(&self.rpc_conf); "use remote meta");
            let client = MetaGrpcClient::try_new(&self.rpc_conf)?;
            Ok(MetaStore::R(client))
        }
    }
}

#[tonic::async_trait]
impl kvapi::KVApi for MetaStore {
    type Error = MetaError;

    #[fastrace::trace]
    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        self.inner().upsert_kv(req).await
    }

    #[fastrace::trace]
    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let strm = self.inner().list(opts.prefix).await?;

        let strm = strm.map_err(MetaError::from);
        Ok(limit_stream(strm, opts.limit))
    }

    #[fastrace::trace]
    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
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

        // Make batch request for successfully collected keys.
        // Use the streaming request directly to preserve keys in the response.
        let strm = self
            .inner()
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
        self.inner().transaction(txn).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use databend_meta_plugin_semaphore::errors::AcquireError;
    use databend_meta_plugin_semaphore::errors::ConnectionClosed;
    use tokio::time::Instant;
    use tokio::time::error::Elapsed;

    use super::retry_acquire;

    /// A failed attempt is retried after a delay that doubles with each failure in a row, not at
    /// once: an attempt that fails as soon as it starts would otherwise run in a tight loop.
    #[tokio::test]
    async fn test_retry_acquire_waits_before_retrying_a_failed_attempt() {
        let attempts = &AtomicUsize::new(0);
        let start = Instant::now();

        let acquired = retry_acquire("test", move || async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                let refused = ConnectionClosed::new_str("no meta-service node serves the watch");
                Ok::<_, Elapsed>(Err(AcquireError::from(refused)))
            } else {
                Ok(Ok("permit"))
            }
        })
        .await;

        assert_eq!(acquired, "permit");
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        // 100 ms, 200 ms and 400 ms before the second, third and fourth attempts.
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(700),
            "elapsed: {elapsed:?}"
        );
    }
}
