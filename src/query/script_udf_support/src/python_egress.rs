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

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arrow_udf_python_worker::NetworkProxy;
use bytes::Bytes;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::DNSResolver;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use http::HeaderMap;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::Limited;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::net::UnixStream;
use url::Url;

const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_LIFETIME: Duration = Duration::from_secs(300);
const MAX_BODY: usize = 16 * 1024 * 1024;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;
type Tunnel = (hyper::upgrade::OnUpgrade, TcpStream);

/// Bound to a tenant and function by the query plan, never by proxy headers or
/// worker-supplied identifiers. Each worker has a private proxy transport.
pub(super) struct PythonEgressProxy {
    tenant: Tenant,
    integrations: Vec<String>,
    provider: Arc<UserApiProvider>,
    ceiling: Vec<String>,
}

impl std::fmt::Debug for PythonEgressProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonEgressProxy")
            .field("tenant", &self.tenant)
            .field("integrations", &self.integrations)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct Target {
    url: Url,
}

impl Target {
    fn parse<B>(request: &Request<B>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !request.headers().contains_key(header::UPGRADE),
            "HTTP Upgrade is not supported"
        );
        let uri = request.uri();
        let url = if request.method() == Method::CONNECT {
            anyhow::ensure!(
                uri.scheme().is_none() && uri.authority().is_some(),
                "CONNECT requires host:port"
            );
            let authority = uri.authority().unwrap();
            anyhow::ensure!(
                authority.port_u16().is_some(),
                "CONNECT requires an explicit port"
            );
            Url::parse(&format!("https://{authority}"))?
        } else {
            anyhow::ensure!(
                uri.scheme_str() == Some("http"),
                "expected an absolute HTTP proxy URI or HTTPS CONNECT"
            );
            Url::parse(&uri.to_string())?
        };
        anyhow::ensure!(
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
            "credentials and fragments are not allowed"
        );
        anyhow::ensure!(
            url.host_str().is_some() && url.port_or_known_default().is_some_and(|port| port != 0),
            "expected host and nonzero port"
        );
        Ok(Self { url })
    }

    fn origin(&self) -> String {
        self.url.origin().ascii_serialization()
    }

    async fn connect(&self) -> anyhow::Result<TcpStream> {
        // URL parsing canonicalizes IPv4/IPv6/IDNA. DNS belongs to the host and
        // the checked endpoint, never the worker's resolver or Host header.
        // Use Databend's async resolver, not libc getaddrinfo on Tokio's
        // blocking pool: timing out a lookup must not strand blocking threads.
        tokio::time::timeout(CONNECT_TIMEOUT, async {
            let port = self.url.port_or_known_default().unwrap();
            let addresses = match self.url.host().unwrap() {
                url::Host::Ipv4(address) => vec![SocketAddr::new(address.into(), port)],
                url::Host::Ipv6(address) => vec![SocketAddr::new(address.into(), port)],
                url::Host::Domain(host) => DNSResolver::instance()?
                    .resolve(host)
                    .await?
                    .into_iter()
                    .map(|address| SocketAddr::new(address, port))
                    .collect(),
            };
            Ok::<_, anyhow::Error>(TcpStream::connect(addresses.as_slice()).await?)
        })
        .await
        .context("Python UDF egress connect timeout")?
    }
}

impl PythonEgressProxy {
    pub(super) fn for_bindings(
        tenant: &str,
        integrations: &[String],
    ) -> Result<Option<Arc<dyn NetworkProxy>>> {
        if integrations.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            tenant: Tenant::new_or_err(tenant, "Python UDF external access")
                .map_err(|e| ErrorCode::InvalidArgument(e.to_string()))?,
            integrations: integrations.to_vec(),
            provider: UserApiProvider::instance(),
            ceiling: GlobalConfig::try_get_instance()
                .map(|config| config.query.common.udf_server_allow_list.clone())
                .unwrap_or_default(),
        })))
    }

    async fn authorize(&self, target: &Target) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.ceiling.is_empty()
                || self.ceiling.iter().any(|value| Url::parse(value)
                    .is_ok_and(|allowed| allowed.host_str() == target.url.host_str())),
            "Python UDF target is outside the operator udf_server_allow_list"
        );
        // Reuse remote UDF policy semantics, including enabled/missing objects,
        // tenant isolation and asynchronous meta-watch invalidation. Query/path
        // parameters are forwarded but are not HOST_PORT rule identities.
        tokio::time::timeout(
            CHECK_TIMEOUT,
            self.provider.validate_external_access(
                &self.tenant,
                &target.origin(),
                &self.integrations,
            ),
        )
        .await
        .context("Python UDF egress policy check timeout")??;
        Ok(())
    }

    async fn forward(
        &self,
        mut request: Request<Incoming>,
        target: &Target,
        tunnel: &Mutex<Option<Tunnel>>,
        driver_guard: &Mutex<Option<AbortDriver>>,
    ) -> anyhow::Result<Response<ProxyBody>> {
        if let Err(error) = self.authorize(target).await {
            return Ok(response(
                StatusCode::FORBIDDEN,
                &format!("Python UDF external access denied: {error:#}"),
            ));
        }
        let upstream = target.connect().await?;
        // DNS/connect may have waited while the policy changed.
        if let Err(error) = self.authorize(target).await {
            return Ok(response(
                StatusCode::FORBIDDEN,
                &format!("Python UDF external access denied: {error:#}"),
            ));
        }
        if request.method() == Method::CONNECT {
            *tunnel.lock() = Some((hyper::upgrade::on(&mut request), upstream));
            return Ok(response(StatusCode::OK, ""));
        }
        strip_hop_headers(request.headers_mut());
        // Never route using a potentially conflicting user-supplied Host header.
        request.headers_mut().insert(
            header::HOST,
            target.url[url::Position::BeforeHost..url::Position::AfterPort].parse()?,
        );
        request
            .headers_mut()
            .insert(header::CONNECTION, "close".parse()?);
        *request.uri_mut() =
            target.url[url::Position::BeforePath..url::Position::AfterQuery].parse()?;
        let request = request.map(|body| Limited::new(body, MAX_BODY));
        let (mut sender, driver) =
            hyper::client::conn::http1::handshake(TokioIo::new(upstream)).await?;
        // The driver must stay alive while the response body is streamed, and
        // must be aborted if policy checks, query cancellation or the client fail.
        *driver_guard.lock() = Some(AbortDriver(databend_common_base::runtime::spawn(
            async move {
                if let Err(error) = driver.await {
                    log::debug!("Python UDF upstream HTTP connection failed: {error}");
                }
            },
        )));
        let mut response = sender.send_request(request).await?;
        anyhow::ensure!(
            response.status() != StatusCode::SWITCHING_PROTOCOLS,
            "HTTP Upgrade is not supported"
        );
        strip_hop_headers(response.headers_mut());
        Ok(response.map(|body| Limited::new(body, MAX_BODY).boxed_unsync()))
    }

    async fn serve_stream(&self, stream: UnixStream) -> anyhow::Result<()> {
        let target = Mutex::new(None::<Target>);
        let tunnel = Mutex::new(None::<Tunnel>);
        let driver = Mutex::new(None::<AbortDriver>);
        let operation = async {
            let service = service_fn(|request: Request<Incoming>| async {
                let response = match Target::parse(&request) {
                    Ok(parsed) => {
                        *target.lock() = Some(parsed.clone());
                        match self.forward(request, &parsed, &tunnel, &driver).await {
                            Ok(response) => response,
                            Err(error) => response(
                                StatusCode::BAD_GATEWAY,
                                &format!("Python UDF external access failed: {error:#}"),
                            ),
                        }
                    }
                    Err(error) => response(StatusCode::BAD_REQUEST, &error.to_string()),
                };
                Ok::<_, Infallible>(response)
            });
            hyper::server::conn::http1::Builder::new()
                .keep_alive(false)
                .max_buf_size(16 * 1024)
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(5))
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await?;
            let tunnel = tunnel.lock().take();
            if let Some((upgrade, mut upstream)) = tunnel {
                let mut client = TokioIo::new(upgrade.await?);
                tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
            }
            anyhow::Ok(())
        };
        let revocation = async {
            loop {
                tokio::time::sleep(CHECK_INTERVAL).await;
                let target = target.lock().clone();
                if let Some(target) = target
                    && let Err(error) = self.authorize(&target).await
                {
                    break Err(error.context("Python UDF connection revoked"));
                }
            }
        };
        // Applies to plain HTTP streams AND opaque CONNECT tunnels, even when
        // Python keeps an idle pooled HTTPS socket after a batch completes.
        tokio::select! {
            result = tokio::time::timeout(MAX_LIFETIME, operation) => result.context("Python UDF proxy connection lifetime exceeded")?,
            result = revocation => result,
        }
    }
}

impl NetworkProxy for PythonEgressProxy {
    fn serve(
        &self,
        stream: UnixStream,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(self.serve_stream(stream))
    }
}

struct AbortDriver(tokio::task::JoinHandle<()>);

impl Drop for AbortDriver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn response(status: StatusCode, message: &str) -> Response<ProxyBody> {
    let body = Full::new(Bytes::copy_from_slice(message.as_bytes()))
        .map_err(|never| match never {})
        .boxed_unsync();
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let connection_headers = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_owned())
        .collect::<Vec<_>>();
    for name in connection_headers {
        headers.remove(name);
    }
    for name in [
        "connection",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "keep-alive",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "content-length",
    ] {
        headers.remove(name);
    }
}
