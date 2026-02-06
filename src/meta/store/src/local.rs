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

use databend_base::testutil::next_port;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::RuntimeApi;
use log::info;
use log::warn;

/// A container for a locally started meta service, mainly for testing purpose.
///
/// The service will be shutdown if this struct is dropped.
/// It deref to `ClientHandle` thus it can be used as a client.
pub struct LocalMetaService {
    _temp_dir: Option<tempfile::TempDir>,

    /// For debugging
    name: String,

    pub config: configs::MetaServiceConfig,

    /// Kept alive for shutdown; dropped when `LocalMetaService` is dropped.
    _grpc_server: Option<Box<dyn Send + Sync>>,

    client: Arc<ClientHandle<DatabendRuntime>>,
}

impl fmt::Display for LocalMetaService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let grpc_addr = self
            .config
            .grpc
            .api_address()
            .unwrap_or_else(|| "-".to_string());
        write!(
            f,
            "LocalMetaService({}: raft={} grpc={})",
            self.name, self.config.raft_config.raft_api_port, grpc_addr
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

    /// Create a new Config for test, with unique port assigned
    ///
    /// It brings up a meta-service process with the port number based on the base_port.
    /// If it is None, 19_000 is used.
    /// If dir is not empty, we should persistent the dir without cleanup, this could be used in databend-local and bendpy
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

        let raft_port = next_port();
        let mut config = configs::MetaServiceConfig::default();

        config.raft_config.id = 0;

        config.raft_config.config_id = raft_port.to_string();

        // Use a unique dir for each instance.
        config.raft_config.raft_dir = format!("{}/{}-{}/raft_dir", dir_path, name, raft_port);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        config.raft_config.raft_api_port = raft_port;
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "localhost".to_string();

        let host = "127.0.0.1";

        // Use OS-assigned port for gRPC
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

        let mut grpc_server = GrpcServer::create(&config, meta_handle);
        grpc_server.do_start().await?;

        // Update config with the actual bound port
        config.grpc = grpc_server.grpc_config().clone();

        let client = Self::grpc_client(&config).await?;

        let local = LocalMetaService {
            _temp_dir: temp_dir,
            name,
            config,
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

    async fn grpc_client(
        config: &configs::MetaServiceConfig,
    ) -> Result<Arc<ClientHandle<DatabendRuntime>>, CreationError> {
        let addr = config.grpc.api_address().expect("gRPC port should be set");
        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr],
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
