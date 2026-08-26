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

use std::fmt;
use std::fs;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_meta::api::grpc::grpc_service::MetaServiceImpl;
use databend_meta::configs;
use databend_meta::meta_node::meta_handle::MetaHandle;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::runtime_api::RuntimeApi;
use databend_meta::types::protobuf::meta_service_server::MetaServiceServer;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime::InProcessGrpcEndpoint;
use futures::StreamExt;
use log::error;
use log::info;
use log::warn;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic_013::transport::Server;

struct InProcessGrpcServer<RT: RuntimeApi> {
    _endpoint: InProcessGrpcEndpoint,
    _meta_handle: Arc<MetaHandle<RT>>,
    _shutdown_tx: oneshot::Sender<()>,
    _join_handle: tokio::task::JoinHandle<()>,
}

/// A container for a locally started embedded meta service.
///
/// The service will be shutdown if this struct is dropped.
/// It deref to `ClientHandle` thus it can be used as a client.
pub struct LocalMetaService {
    _temp_dir: Option<tempfile::TempDir>,

    /// For debugging
    name: String,

    pub config: configs::MetaServiceConfig,

    grpc_addr: String,

    /// Kept alive for shutdown; dropped when `LocalMetaService` is dropped.
    _grpc_server: Option<Box<dyn Send + Sync>>,

    client: Arc<ClientHandle<DatabendRuntime>>,
}

impl fmt::Display for LocalMetaService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocalMetaService({}: raft={} grpc={})",
            self.name, self.config.raft_config.raft_api_port, self.grpc_addr
        )
    }
}

/// The [LocalMetaService] implements the [Deref] trait, so it can be used as a [ClientHandle].
impl Deref for LocalMetaService {
    type Target = Arc<ClientHandle<DatabendRuntime>>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Drop for LocalMetaService {
    fn drop(&mut self) {
        if self._temp_dir.is_some() {
            Self::rm_raft_dir(
                &self.config,
                format_args!("Drop LocalMetaService: {}", self),
            );
        }
    }
}

impl LocalMetaService {
    pub async fn new<RT: RuntimeApi>(name: impl fmt::Display) -> anyhow::Result<LocalMetaService> {
        Self::new_with_fixed_dir::<RT>(None, name).await
    }

    /// Create an embedded meta service for tests and local mode.
    ///
    /// Client RPCs use an in-process gRPC channel. Raft still starts the
    /// listener required by `databend-meta`, but binds port 0 atomically.
    /// If `dir` is set, data is kept for databend-local and bendpy.
    pub async fn new_with_fixed_dir<RT: RuntimeApi>(
        dir: Option<String>,
        name: impl fmt::Display,
    ) -> anyhow::Result<LocalMetaService> {
        let name = name.to_string();
        let (temp_dir, dir_path) = if let Some(dir_path) = dir {
            (None, dir_path)
        } else {
            let temp_dir = tempfile::tempdir().unwrap();
            let dir_path = format!("{}", temp_dir.path().display());
            (Some(temp_dir), dir_path)
        };

        let instance_id = GlobalUniq::unique();
        let mut config = configs::MetaServiceConfig::default();

        config.raft_config.id = 0;

        config.raft_config.config_id = instance_id.clone();

        // Use a unique dir for each instance.
        config.raft_config.raft_dir = format!("{}/{}-{}/raft_dir", dir_path, name, instance_id);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        // A single embedded node does not make Raft RPCs. Let the OS assign the
        // listener port atomically instead of probing a free port and racing a
        // later bind in databend-meta.
        config.raft_config.raft_api_port = 0;
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "localhost".to_string();

        let host = "127.0.0.1";

        // The in-process server uses this config for gRPC limits, not for listening.
        config.grpc = configs::GrpcConfig::new_local(host);

        info!("new LocalMetaService({}) with config: {:?}", name, config);

        // Clean up the raft dir if it exists.
        if temp_dir.is_some() {
            Self::rm_raft_dir(&config, "new LocalMetaService");
        }

        // Bring up the services
        let runtime = RT::new_embedded("meta-io-rt-embedded");
        let meta_handle = MetaWorker::create_meta_worker(config.clone(), Arc::new(runtime)).await?;
        let meta_handle = Arc::new(meta_handle);

        let grpc_server = Self::start_in_process_grpc_server::<RT>(&config, meta_handle);
        let grpc_addr = grpc_server._endpoint.address().to_string();
        let client = Self::grpc_client(&grpc_addr).await?;

        let local = LocalMetaService {
            _temp_dir: temp_dir,
            name,
            config,
            grpc_addr,
            _grpc_server: Some(Box::new(grpc_server)),
            client,
        };

        Ok(local)
    }
}

impl LocalMetaService {
    pub fn rm_raft_dir(config: &configs::MetaServiceConfig, msg: impl fmt::Display + Copy) {
        let raft_dir = &config.raft_config.raft_dir;

        info!("{}: about to remove raft_dir: {:?}", msg, raft_dir);

        let res = fs::remove_dir_all(raft_dir);
        if let Err(e) = res {
            warn!("{}: can not remove raft_dir {:?}, {:?}", msg, raft_dir, e);
        } else {
            info!("{}: OK removed raft_dir {:?}", msg, raft_dir)
        }
    }

    fn start_in_process_grpc_server<RT: RuntimeApi>(
        config: &configs::MetaServiceConfig,
        meta_handle: Arc<MetaHandle<RT>>,
    ) -> InProcessGrpcServer<RT> {
        let mut endpoint = InProcessGrpcEndpoint::new();
        let incoming =
            UnboundedReceiverStream::new(endpoint.take_incoming()).map(Ok::<_, std::io::Error>);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let grpc_impl = MetaServiceImpl::create(
            *databend_meta::version::version(),
            Arc::downgrade(&meta_handle),
        );
        let max_msg_size = config.grpc.max_message_size();
        let grpc_service = MetaServiceServer::new(grpc_impl)
            .max_decoding_message_size(max_msg_size)
            .max_encoding_message_size(max_msg_size);

        let join_handle = RT::spawn(
            async move {
                let result = Server::builder()
                    .add_service(grpc_service)
                    .serve_with_incoming_shutdown(incoming, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                if let Err(cause) = result {
                    error!("embedded meta in-process gRPC server stopped: {cause}");
                }
            },
            Some("embedded-meta-in-process-grpc".to_string()),
        );

        InProcessGrpcServer {
            _endpoint: endpoint,
            _meta_handle: meta_handle,
            _shutdown_tx: shutdown_tx,
            _join_handle: join_handle,
        }
    }

    async fn grpc_client(addr: &str) -> Result<Arc<ClientHandle<DatabendRuntime>>, CreationError> {
        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr.to_string()],
            "root",
            "xxx",
            None,
            Some(Duration::from_secs(10)),
            None,
            DEFAULT_GRPC_MESSAGE_SIZE,
        )?;

        Ok(client)
    }
}
