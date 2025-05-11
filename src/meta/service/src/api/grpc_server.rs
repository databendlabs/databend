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

use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::base::tokio::sync::oneshot::Sender;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::base::Stoppable;
use databend_common_meta_types::protobuf::meta_service_server::MetaServiceServer;
use databend_common_meta_types::protobuf::FILE_DESCRIPTOR_SET;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::MetaNetworkError;
use fastrace::prelude::*;
use futures::future::select;
use futures::future::Either;
use log::info;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;

use crate::api::grpc::grpc_service::MetaServiceImpl;
use crate::configs::Config;
use crate::meta_service::MetaNode;
use crate::util::DropDebug;

pub struct GrpcServer {
    conf: Config,
    /// GrpcServer is the main container of the gRPC service.
    /// [`MetaNode`] should never be dropped while [`GrpcServer`] is alive.
    /// Therefore, it is held by a strong reference (Arc) to ensure proper lifetime management.
    pub(crate) meta_node: Arc<MetaNode>,
    join_handle: Option<JoinHandle<()>>,
    stop_grpc_tx: Option<Sender<()>>,
}

impl Drop for GrpcServer {
    fn drop(&mut self) {
        info!("GrpcServer::drop: id={}", self.conf.raft_config.id);
    }
}

impl GrpcServer {
    pub fn create(conf: Config, meta_node: Arc<MetaNode>) -> Self {
        Self {
            conf,
            meta_node,
            join_handle: None,
            stop_grpc_tx: None,
        }
    }

    pub fn get_meta_node(&self) -> Arc<MetaNode> {
        self.meta_node.clone()
    }

    async fn do_start(&mut self) -> Result<(), MetaNetworkError> {
        let conf = self.conf.clone();
        let meta_node = self.meta_node.clone();
        // For sending signal when server started.
        let (started_tx, started_rx) = oneshot::channel::<()>();
        // For receive stop signal.
        let (stop_grpc_tx, stop_rx) = oneshot::channel::<()>();

        let reflect_srv = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        let builder = Server::builder();

        let tls_conf = Self::tls_config(&self.conf)
            .await
            .map_err(|e| MetaNetworkError::TLSConfigError(AnyError::new(&e)))?;

        let mut builder = if let Some(tls_conf) = tls_conf {
            info!("gRPC TLS enabled");
            let _ = rustls::crypto::ring::default_provider().install_default();
            builder
                .tls_config(tls_conf)
                .map_err(|e| MetaNetworkError::TLSConfigError(AnyError::new(&e)))?
        } else {
            builder
        };

        let addr = conf.grpc_api_address.parse::<std::net::SocketAddr>()?;

        info!("start gRPC listening: {}", addr);

        let grpc_impl = MetaServiceImpl::create(Arc::downgrade(&meta_node));
        let grpc_srv = MetaServiceServer::new(grpc_impl)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        let id = conf.raft_config.id;

        let j = databend_common_base::runtime::spawn(
            async move {
                let _d = DropDebug::new(format!("GrpcServer(id={}) spawned service task", id));

                let res = builder
                    .add_service(reflect_srv)
                    .add_service(grpc_srv)
                    .serve_with_shutdown(addr, async move {
                        let _ = started_tx.send(());
                        info!(
                            "meta-service gRPC(on {}) starts to wait for stop signal",
                            addr
                        );
                        let _ = stop_rx.await;
                        info!("meta-service gRPC(on {}) receives stop signal", addr);
                    })
                    .await;

                info!(
                    "meta-service gRPC(on {}) task returned res: {:?}",
                    addr, res
                );
            }
            .in_span(Span::enter_with_local_parent("spawn-grpc")),
        );

        started_rx
            .await
            .expect("maybe address already in use, try to use another port");

        self.join_handle = Some(j);
        self.stop_grpc_tx = Some(stop_grpc_tx);

        Ok(())
    }

    async fn do_stop(&mut self, _force: Option<tokio::sync::broadcast::Receiver<()>>) {
        let id = self.meta_node.raft_store.id;
        let ctx = format!("gRPC-task(id={id})");

        if let Some(stop_grpc_tx) = self.stop_grpc_tx.take() {
            info!("Sending stop signal to {ctx}");
            let _ = stop_grpc_tx.send(());
        }

        if let Some(jh) = self.join_handle.take() {
            info!("Waiting for {ctx} to stop");

            let timeout = Duration::from_millis(1_000);
            let sleep = tokio::time::sleep(timeout);

            let slp = std::pin::pin!(sleep);

            match select(slp, jh).await {
                Either::Left((_v1, jh)) => {
                    info!("{ctx} stop timeout after {:?}; force abort", timeout);
                    jh.abort();
                    jh.await.ok();
                    info!("Done: waiting for {ctx} force stop");
                }
                Either::Right((_v2, _slp)) => {
                    info!("Done: waiting for {ctx} normal stop");
                }
            }
        }

        info!("Waiting for meta_node(id={id}) to stop");
        let x = tokio::time::timeout(Duration::from_millis(1_000), self.meta_node.stop()).await;
        info!("Done: waiting for meta_node(id={id}) stop: res: {:?}", x);
    }

    async fn tls_config(conf: &Config) -> Result<Option<ServerTlsConfig>, std::io::Error> {
        if conf.tls_rpc_server_enabled() {
            let cert = tokio::fs::read(conf.grpc_tls_server_cert.as_str()).await?;
            let key = tokio::fs::read(conf.grpc_tls_server_key.as_str()).await?;
            let server_identity = Identity::from_pem(cert, key);

            let tls = ServerTlsConfig::new().identity(server_identity);
            Ok(Some(tls))
        } else {
            Ok(None)
        }
    }
}

#[tonic::async_trait]
impl Stoppable for GrpcServer {
    type Error = AnyError;

    async fn start(&mut self) -> Result<(), Self::Error> {
        info!("GrpcServer::start");
        let res = self.do_start().await;

        res.map_err(|e: MetaNetworkError| AnyError::new(&e))?;
        info!("Done GrpcServer::start");
        Ok(())
    }

    async fn stop(
        &mut self,
        force: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> Result<(), Self::Error> {
        info!("GrpcServer::stop");
        self.do_stop(force).await;
        info!("Done GrpcServer::stop");
        Ok(())
    }
}
