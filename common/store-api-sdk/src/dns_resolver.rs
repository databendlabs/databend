// Copyright 2020 Datafuse Labs.
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
use std::task;
use std::task::Poll;
use std::time::Duration;

use common_base::tokio;
use common_base::tokio::task::JoinHandle;
use common_exception::ErrorCode;
use common_exception::Result;
use hyper::client::connect::dns::Name;
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Uri;
use lazy_static::lazy_static;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use trust_dns_resolver::TokioAsyncResolver;

use crate::common::RpcClientTlsConfig;

pub struct DNSResolver {
    inner: TokioAsyncResolver,
}

lazy_static! {
    static ref INSTANCE: Result<Arc<DNSResolver>> = {
        match TokioAsyncResolver::tokio_from_system_conf() {
            Err(error) => Result::Err(ErrorCode::DnsParseError(format!(
                "DNS resolver create error: {}",
                error
            ))),
            Ok(resolver) => Ok(Arc::new(DNSResolver { inner: resolver })),
        }
    };
}

impl DNSResolver {
    pub fn instance() -> Result<Arc<DNSResolver>> {
        match INSTANCE.as_ref() {
            Ok(resolver) => Ok(resolver.clone()),
            Err(error) => Err(ErrorCode::create(
                error.code(),
                error.message(),
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
struct DNSService;

impl Service<Name> for DNSService {
    type Response = DNSServiceAddrs;
    type Error = ErrorCode;
    type Future = DNSServiceFuture;

    fn poll_ready(&mut self, _cx: &mut task::Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let blocking = tokio::spawn(async move {
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

struct DNSServiceFuture {
    inner: JoinHandle<Result<DNSServiceAddrs>>,
}

struct DNSServiceAddrs {
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

pub struct ConnectionFactory;

impl ConnectionFactory {
    pub fn create_flight_channel(
        addr: impl ToString,
        timeout: Option<Duration>,
        rpc_client_config: Option<RpcClientTlsConfig>,
    ) -> Result<Channel> {
        match format!("http://{}", addr.to_string()).parse::<Uri>() {
            Err(error) => Result::Err(ErrorCode::BadAddressFormat(format!(
                "Node address format is not parse: {}",
                error
            ))),
            Ok(uri) => {
                let mut inner_connector = HttpConnector::new_with_resolver(DNSService);
                inner_connector.set_nodelay(true);
                inner_connector.set_keepalive(None);
                inner_connector.enforce_http(false);

                let builder = Channel::builder(uri.clone());

                let mut endpoint = if let Some(conf) = rpc_client_config {
                    log::info!("tls rpc enabled");
                    let client_tls_config = Self::client_tls_config(&conf).map_err(|e| {
                        ErrorCode::TLSConfigurationFailure(format!(
                            "loading client tls config failure: {} ",
                            e.to_string()
                        ))
                    })?;
                    builder.tls_config(client_tls_config).map_err(|e| {
                        ErrorCode::TLSConfigurationFailure(format!(
                            "builder tls_config failure: {}",
                            e.to_string()
                        ))
                    })?
                } else {
                    builder
                };

                if let Some(timeout) = timeout {
                    endpoint = endpoint.timeout(timeout);
                }

                match endpoint.connect_with_connector_lazy(inner_connector) {
                    Ok(channel) => Result::Ok(channel),
                    Err(error) => Result::Err(ErrorCode::CannotConnectNode(format!(
                        "Cannot to RPC server: {:?} error: {}",
                        uri, error
                    ))),
                }
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
