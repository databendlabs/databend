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
use std::net::TcpListener;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::GlobalSequence;
use databend_common_base::base::Stoppable;
use databend_common_meta_client::errors::CreationError;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::meta_service::MetaNode;
use log::info;
use log::warn;
use tokio::time::sleep;

/// A container for a locally started meta service, mainly for testing purpose.
///
/// The service will be shutdown if this struct is dropped.
/// It deref to `ClientHandle` thus it can be used as a client.
pub struct LocalMetaService {
    _temp_dir: Option<tempfile::TempDir>,

    /// For debugging
    name: String,

    pub config: configs::Config,

    pub grpc_server: Option<Box<GrpcServer>>,

    client: Arc<ClientHandle>,
}

impl fmt::Display for LocalMetaService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocalMetaService({}: raft={} grpc={})",
            self.name, self.config.raft_config.raft_api_port, self.config.grpc_api_address
        )
    }
}

/// The [LocalMetaService] implements the [Deref] trait, so it can be used as a [ClientHandle].
impl Deref for LocalMetaService {
    type Target = Arc<ClientHandle>;

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
    pub async fn new(
        name: impl fmt::Display,
        version: BuildInfoRef,
    ) -> anyhow::Result<LocalMetaService> {
        Self::new_with_fixed_dir(None, name, version).await
    }

    /// Create a new Config for test, with unique port assigned
    ///
    /// It brings up a meta-service process with the port number based on the base_port.
    /// If it is None, 19_000 is used.
    /// If dir is not empty, we should persistent the dir without cleanup, this could be used in databend-local and bendpy
    pub async fn new_with_fixed_dir(
        dir: Option<String>,
        name: impl fmt::Display,
        version: BuildInfoRef,
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
        let mut config = configs::Config::default();

        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta service. fsync on mac is quite slow");
            config.raft_config.no_sync = true;
        }

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

        {
            let grpc_port = next_port();
            config.grpc_api_address = format!("{}:{}", host, grpc_port);
            config.grpc_api_advertise_host = Some(host.to_string());
        }

        {
            let http_port = next_port();
            config.admin_api_address = format!("{}:{}", host, http_port);
        }

        info!("new LocalMetaService({}) with config: {:?}", name, config);

        // Clean up the raft dir if it exists.
        if temp_dir.is_some() {
            Self::rm_raft_dir(&config, "new LocalMetaService");
        }

        // Bring up the services
        let meta_node = MetaNode::start(&config, version).await?;
        let mut grpc_server = GrpcServer::create(config.clone(), meta_node);
        grpc_server.start().await?;

        let client = Self::grpc_client(&config, version).await?;

        let local = LocalMetaService {
            _temp_dir: temp_dir,
            name,
            config,
            grpc_server: Some(Box::new(grpc_server)),
            client,
        };

        Ok(local)
    }

    pub fn rm_raft_dir(config: &configs::Config, msg: impl fmt::Display + Copy) {
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
        config: &configs::Config,
        version: BuildInfoRef,
    ) -> Result<Arc<ClientHandle>, CreationError> {
        let addr = config.grpc_api_address.clone();
        let client = MetaGrpcClient::try_create(
            vec![addr],
            version,
            "root",
            "xxx",
            None,
            Some(Duration::from_secs(10)),
            None,
        )?;

        Ok(client)
    }

    pub async fn raft_client(
        &self,
    ) -> anyhow::Result<RaftServiceClient<tonic::transport::Channel>> {
        let addr = self.config.raft_config.raft_api_addr().await?;

        let mut last_error = None;

        for _ in 0..6 {
            let client = RaftServiceClient::connect(format!("http://{}", addr)).await;
            match client {
                Ok(x) => return Ok(x),
                Err(err) => {
                    warn!("can not yet connect to {}, {}, sleep a while", addr, err);
                    last_error = Some(err);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }

        Err(last_error.unwrap().into())
    }

    pub async fn assert_raft_server_connection(&self) -> anyhow::Result<()> {
        let mut client = self.raft_client().await?;

        let req = ForwardRequest {
            forward_to_leader: 0,
            body: ForwardRequestBody::Ping,
        };

        client.forward(req).await?;
        Ok(())
    }
}

fn next_port() -> u16 {
    let base = get_machine_unique_base_port();

    base + (GlobalSequence::next() as u16)
}

fn get_machine_unique_base_port() -> u16 {
    static mut BASE: u16 = 19_000;
    static BASE_ONCE: std::sync::Once = std::sync::Once::new();
    unsafe {
        BASE_ONCE.call_once(|| {
            let (port, listener) = try_bind();
            Box::leak(Box::new(listener));
            BASE = port;
        });
        BASE
    }
}

/// Occupy a port
fn try_bind() -> (u16, TcpListener) {
    let mut port = 19_000u16;

    while port < 30_000 {
        let address = format!("127.0.0.1:{port}");
        let res = TcpListener::bind(&address);
        match res {
            Ok(listener) => {
                info!("bind to {address} OK");
                return (port, listener);
            }
            Err(e) => {
                port += 500;
                info!("bind to {address} failed: {e}; try next port: {}", port);
            }
        }
    }

    unreachable!("can not find available port")
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_local_meta_service() -> anyhow::Result<()> {
        Ok(())
    }
}
