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

use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::task;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use hickory_resolver::TokioResolver;
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::Name;
use log::info;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

use crate::RpcClientTlsConfig;

pub struct DNSResolver {
    inner: TokioResolver,
}

static INSTANCE: LazyLock<Result<Arc<DNSResolver>>> =
    LazyLock::new(|| match TokioResolver::builder_tokio() {
        Err(error) => Result::Err(ErrorCode::DnsParseError(format!(
            "DNS resolver create error: {}",
            error
        ))),
        Ok(resolver) => Ok(Arc::new(DNSResolver {
            inner: resolver.build(),
        })),
    });

impl DNSResolver {
    pub fn instance() -> Result<Arc<DNSResolver>> {
        match INSTANCE.as_ref() {
            Ok(resolver) => Ok(resolver.clone()),
            Err(error) => Err(ErrorCode::create(
                error.code(),
                error.name(),
                error.message(),
                String::new(),
                None,
                error.backtrace(),
            )),
        }
    }

    pub async fn resolve(&self, hostname: impl Into<String>) -> Result<Vec<IpAddr>> {
        let hostname = hostname.into();
        match self.inner.lookup_ip(hostname.clone()).await {
            Ok(lookup_ip) => Ok(lookup_ip.iter().collect::<Vec<_>>()),
            Err(error) => Err(ErrorCode::DnsParseError(format!(
                "Cannot lookup ip {} : {}",
                hostname, error
            ))),
        }
    }
}

#[derive(Clone)]
pub struct DNSService;

impl tower_service::Service<Name> for DNSService {
    type Response = DNSServiceAddrs;
    type Error = ErrorCode;
    type Future = DNSServiceFuture;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let blocking = runtime::spawn(async move {
            let resolver = DNSResolver::instance()?;
            match resolver.resolve(name.to_string()).await {
                Err(err) => Err(err),
                Ok(addrs) => Ok(DNSServiceAddrs {
                    inner: addrs.into_iter(),
                }),
            }
        });

        DNSServiceFuture { inner: blocking }
    }
}

pub struct DNSServiceFuture {
    inner: JoinHandle<Result<DNSServiceAddrs>>,
}

pub struct DNSServiceAddrs {
    inner: std::vec::IntoIter<IpAddr>,
}

impl Iterator for DNSServiceAddrs {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|addr| SocketAddr::new(addr, 0))
    }
}

impl Future for DNSServiceFuture {
    type Output = Result<DNSServiceAddrs>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx).map(|res| match res {
            Ok(Err(err)) => Err(err),
            Ok(Ok(addrs)) => Ok(addrs),
            Err(join_err) => Err(ErrorCode::TokioError(format!(
                "Interrupted future: {}",
                join_err
            ))),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TcpKeepAliveConfig {
    pub time: Option<Duration>,
    pub interval: Option<Duration>,
    pub retries: Option<u32>,
}

pub struct ConnectionFactory;

impl ConnectionFactory {
    pub async fn create_rpc_channel(
        addr: impl ToString,
        timeout: Option<Duration>,
        rpc_client_config: Option<RpcClientTlsConfig>,
        keep_alive: Option<TcpKeepAliveConfig>,
    ) -> std::result::Result<Channel, GrpcConnectionError> {
        let endpoint = Self::create_rpc_endpoint(addr, timeout, rpc_client_config)?;

        let mut inner_connector = HttpConnector::new_with_resolver(DNSService);
        inner_connector.set_nodelay(true);
        match keep_alive {
            Some(config) => {
                inner_connector.set_keepalive(config.time);
                inner_connector.set_keepalive_interval(config.interval);
                inner_connector.set_keepalive_retries(config.retries);
            }
            None => {
                inner_connector.set_keepalive(None);
                inner_connector.set_keepalive_interval(None);
                inner_connector.set_keepalive_retries(None);
            }
        }
        inner_connector.enforce_http(false);
        inner_connector.set_connect_timeout(timeout);

        // check connection immediately
        match endpoint.connect_with_connector(inner_connector).await {
            Ok(channel) => Ok(channel),
            Err(error) => Err(GrpcConnectionError::CannotConnect {
                uri: endpoint.uri().to_string(),
                source: AnyError::new(&error),
            }),
        }
    }

    pub fn create_rpc_endpoint(
        addr: impl ToString,
        timeout: Option<Duration>,
        rpc_client_tls_config: Option<RpcClientTlsConfig>,
    ) -> std::result::Result<Endpoint, GrpcConnectionError> {
        let u = if rpc_client_tls_config.is_some() {
            format!("https://{}", addr.to_string())
        } else {
            format!("http://{}", addr.to_string())
        };
        match u.parse::<Uri>() {
            Err(error) => Err(GrpcConnectionError::InvalidUri {
                uri: addr.to_string(),
                source: AnyError::new(&error),
            }),
            Ok(uri) => {
                let builder = Channel::builder(uri);
                let mut endpoint = if let Some(conf) = rpc_client_tls_config {
                    info!("tls rpc enabled");
                    let client_tls_config = Self::client_tls_config(&conf).map_err(|e| {
                        GrpcConnectionError::TLSConfigError {
                            action: "loading".to_string(),
                            source: AnyError::new(&e),
                        }
                    })?;
                    builder.tls_config(client_tls_config).map_err(|e| {
                        GrpcConnectionError::TLSConfigError {
                            action: "building".to_string(),
                            source: AnyError::new(&e),
                        }
                    })?
                } else {
                    builder
                };

                if let Some(timeout) = timeout {
                    endpoint = endpoint.timeout(timeout);
                }

                // To avoid too_many_internal_resets
                endpoint = endpoint
                    .tcp_nodelay(true)
                    .http2_adaptive_window(true)
                    .http2_keep_alive_interval(std::time::Duration::from_secs(30))
                    .keep_alive_timeout(std::time::Duration::from_secs(10))
                    .keep_alive_while_idle(true);

                Ok(endpoint)
            }
        }
    }

    fn client_tls_config(conf: &RpcClientTlsConfig) -> Result<ClientTlsConfig> {
        let server_root_ca_cert = std::fs::read(conf.rpc_tls_server_root_ca_cert.as_str())?;
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);

        let tls = ClientTlsConfig::new()
            .domain_name(conf.domain_name.to_string())
            .ca_certificate(server_root_ca_cert);
        Ok(tls)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrpcConnectionError {
    #[error("invalid uri: {uri}, error: {source}")]
    InvalidUri {
        uri: String,
        #[source]
        source: AnyError,
    },

    #[error("{action} client tls config, error: {source}")]
    TLSConfigError {
        action: String,
        #[source]
        source: AnyError,
    },

    #[error("can not connect to {uri}, error: {source}")]
    CannotConnect {
        uri: String,
        #[source]
        source: AnyError,
    },
}

impl From<GrpcConnectionError> for ErrorCode {
    fn from(ge: GrpcConnectionError) -> Self {
        match ge {
            GrpcConnectionError::InvalidUri { .. } => ErrorCode::BadAddressFormat(ge.to_string()),
            GrpcConnectionError::TLSConfigError { .. } => {
                ErrorCode::TLSConfigurationFailure(ge.to_string())
            }
            GrpcConnectionError::CannotConnect { .. } => {
                ErrorCode::CannotConnectNode(ge.to_string())
            }
        }
    }
}
