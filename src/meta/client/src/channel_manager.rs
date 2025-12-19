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
use databend_common_base::base::BuildInfoRef;
use databend_common_base::containers::ItemManager;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::GrpcConnectionError;
use databend_common_grpc::RpcClientTlsConfig;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tonic::async_trait;
use tonic::transport::Channel;

use crate::FeatureSpec;
use crate::MetaGrpcClient;
use crate::endpoints::Endpoints;
use crate::established_client::EstablishedClient;
use crate::grpc_client::AuthInterceptor;
use crate::grpc_client::RealClient;

#[derive(Debug)]
pub struct MetaChannelManager {
    version: BuildInfoRef,
    username: String,
    password: String,
    timeout: Option<Duration>,
    tls_config: Option<RpcClientTlsConfig>,

    required_features: &'static [FeatureSpec],

    /// The endpoints of the meta-service cluster.
    ///
    /// The endpoints will be added to a built client item
    /// and will be updated when a error or successful response is received.
    endpoints: Arc<Mutex<Endpoints>>,
}

impl MetaChannelManager {
    pub fn new(
        version: BuildInfoRef,
        username: impl ToString,
        password: impl ToString,
        timeout: Option<Duration>,
        tls_config: Option<RpcClientTlsConfig>,
        required_features: &'static [FeatureSpec],
        endpoints: Arc<Mutex<Endpoints>>,
    ) -> Self {
        Self {
            version,
            username: username.to_string(),
            password: password.to_string(),
            timeout,
            tls_config,
            required_features,
            endpoints,
        }
    }

    #[async_backtrace::framed]
    async fn new_established_client(
        &self,
        addr: &String,
    ) -> Result<EstablishedClient, MetaClientError> {
        let chan = self.build_channel(addr).await?;

        let (mut real_client, once) = Self::new_real_client(chan);

        info!(
            "MetaChannelManager done building RealClient to {}, start handshake",
            addr
        );

        let handshake_res = MetaGrpcClient::handshake(
            &mut real_client,
            &self.version.semantic,
            self.required_features,
            &self.username,
            &self.password,
        )
        .await;

        info!(
            "MetaChannelManager done handshake to {}, result.err(): {:?}",
            addr,
            handshake_res.as_ref().err()
        );

        let (token, server_version, features) = handshake_res?;

        // Update the token for the client interceptor.
        // Safe unwrap(): it is the first time setting it.
        once.set(token).unwrap();

        Ok(EstablishedClient::new(
            real_client,
            server_version,
            features,
            addr,
            self.endpoints.clone(),
        ))
    }

    /// Create a MetaServiceClient with authentication interceptor
    ///
    /// The returned `OnceCell` is used to fill in a token for the interceptor.
    pub fn new_real_client(chan: Channel) -> (RealClient, Arc<OnceCell<Vec<u8>>>) {
        let once = Arc::new(OnceCell::new());

        let interceptor = AuthInterceptor {
            token: once.clone(),
        };

        let client = MetaServiceClient::with_interceptor(chan, interceptor)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        (client, once)
    }

    #[async_backtrace::framed]
    async fn build_channel(&self, addr: &String) -> Result<Channel, MetaNetworkError> {
        info!("MetaChannelManager::build_channel to {}", addr);

        let ch = ConnectionFactory::create_rpc_channel(addr, self.timeout, self.tls_config.clone())
            .await
            .map_err(|e| match e {
                GrpcConnectionError::InvalidUri { .. } => MetaNetworkError::BadAddressFormat(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::TLSConfigError { .. } => MetaNetworkError::TLSConfigError(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::CannotConnect { .. } => MetaNetworkError::ConnectionError(
                    ConnectionError::new(e, "while creating rpc channel"),
                ),
            })?;
        Ok(ch)
    }
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = String;
    type Item = EstablishedClient;
    type Error = MetaClientError;

    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build(&self, addr: &Self::Key) -> Result<Self::Item, Self::Error> {
        self.new_established_client(addr).await
    }

    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn check(&self, ch: Self::Item) -> Result<Self::Item, Self::Error> {
        // The underlying `tonic::transport::channel::Channel` reconnects when server is down.
        // But we still need to assert the readiness, e.g., when handshake token expires
        // If there was an error occurred, the channel will be closed.
        if let Some(e) = ch.take_error() {
            return Err(MetaNetworkError::from(e).into());
        }
        Ok(ch)
    }
}
