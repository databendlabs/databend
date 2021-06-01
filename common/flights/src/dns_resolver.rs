// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task;
use std::task::Poll;
use std::time::Duration;

use common_exception::ErrorCodes;
use common_exception::Result;
use hyper::client::connect::dns::Name;
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Uri;
use lazy_static::lazy_static;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use trust_dns_resolver::TokioAsyncResolver;

pub struct DNSResolver {
    inner: TokioAsyncResolver
}

lazy_static! {
    static ref INSTANCE: Result<Arc<DNSResolver>> = {
        match TokioAsyncResolver::tokio_from_system_conf() {
            Err(error) => Result::Err(ErrorCodes::DnsParseError(format!(
                "DNS resolver create error: {}",
                error
            ))),
            Ok(resolver) => Ok(Arc::new(DNSResolver { inner: resolver }))
        }
    };
}

impl DNSResolver {
    pub fn instance() -> Result<Arc<DNSResolver>> {
        match INSTANCE.as_ref() {
            Ok(resolver) => Ok(resolver.clone()),
            Err(error) => Err(ErrorCodes::create(
                error.code(),
                error.message(),
                error.backtrace()
            ))
        }
    }

    pub async fn resolve(&self, hostname: impl Into<String>) -> Result<Vec<IpAddr>> {
        let hostname = hostname.into();
        match self.inner.lookup_ip(hostname.clone()).await {
            Ok(lookup_ip) => Ok(lookup_ip.iter().collect::<Vec<_>>()),
            Err(error) => Err(ErrorCodes::DnsParseError(format!(
                "Cannot lookup ip {} : {}",
                hostname, error
            )))
        }
    }
}

#[derive(Clone)]
struct DNSService;

impl Service<Name> for DNSService {
    type Response = DNSServiceAddrs;
    type Error = ErrorCodes;
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
                    inner: addrs.into_iter()
                })
            }
        });

        DNSServiceFuture { inner: blocking }
    }
}

struct DNSServiceFuture {
    inner: JoinHandle<Result<DNSServiceAddrs>>
}

struct DNSServiceAddrs {
    inner: std::vec::IntoIter<IpAddr>
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
            Err(join_err) => Err(ErrorCodes::TokioError(format!(
                "Interrupted future: {}",
                join_err
            )))
        })
    }
}

pub struct ConnectionFactory;

impl ConnectionFactory {
    pub async fn create_flight_channel(
        addr: impl ToString,
        timeout: Option<Duration>
    ) -> Result<Channel> {
        match format!("http://{}", addr.to_string()).parse::<Uri>() {
            Err(error) => Result::Err(ErrorCodes::BadAddressFormat(format!(
                "Node address format is not parse: {}",
                error
            ))),
            Ok(uri) => {
                let mut inner_connector = HttpConnector::new_with_resolver(DNSService);
                inner_connector.set_nodelay(true);
                inner_connector.set_keepalive(None);
                inner_connector.enforce_http(false);

                let mut endpoint = Channel::builder(uri);

                if let Some(timeout) = timeout {
                    endpoint = endpoint.timeout(timeout);
                }

                match endpoint.connect_with_connector(inner_connector).await {
                    Ok(channel) => Result::Ok(channel),
                    Err(error) => Result::Err(ErrorCodes::CannotConnectNode(format!(
                        "Cannot to RPC server: {}",
                        error
                    )))
                }
            }
        }
    }
}
