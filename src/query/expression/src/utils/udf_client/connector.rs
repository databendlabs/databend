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

use std::error::Error;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use databend_common_grpc::DNSService;
use http::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tower::Service;
use tower::service_fn;

use super::UDF_TCP_KEEP_ALIVE_SEC;
use super::find_io_error_kind;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UdfConnectResult {
    #[default]
    NotStarted,
    NotApplicable,
    Ok,
    Failed,
    Timeout,
    Unknown,
}

impl UdfConnectResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::NotApplicable => "not_applicable",
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Unknown => "unknown",
        }
    }
}

/// Observations of the initial dial for this channel, not of a subsequent tonic reconnect.
#[derive(Clone, Copy, Default)]
pub struct UdfConnectDiagnostics {
    pub dns: UdfConnectResult,
    pub tcp: UdfConnectResult,
    /// Includes tonic's HTTP/2 ALPN check. Plaintext connections report `NotApplicable`.
    pub tls: UdfConnectResult,
    pub initial_peer_ip: Option<IpAddr>,
    pub redial_started: bool,
}

impl fmt::Debug for UdfConnectDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdfConnectDiagnostics")
            .field("dns", &self.dns)
            .field("tcp", &self.tcp)
            .field("tls", &self.tls)
            .field("redial_started", &self.redial_started)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct State {
    started: bool,
    diagnostics: UdfConnectDiagnostics,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ConnectObservation(Arc<Mutex<State>>);

impl ConnectObservation {
    pub(super) fn snapshot(&self) -> UdfConnectDiagnostics {
        self.0.lock().unwrap().diagnostics
    }

    fn update(&self, f: impl FnOnce(&mut UdfConnectDiagnostics)) {
        f(&mut self.0.lock().unwrap().diagnostics);
    }

    fn start(&self, uri: &Uri) -> Option<Self> {
        let mut state = self.0.lock().unwrap();
        if state.started {
            state.diagnostics.redial_started = true;
            return None;
        }
        state.started = true;
        if uri.scheme_str() != Some("https") {
            state.diagnostics.tls = UdfConnectResult::NotApplicable;
        }
        // Same literal check as Hyper, including bracketed IPv6. No DNS call in this case.
        if uri.host().is_some_and(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .is_ok()
        }) {
            state.diagnostics.dns = UdfConnectResult::NotApplicable;
            state.diagnostics.tcp = UdfConnectResult::Unknown;
        }
        Some(self.clone())
    }

    pub(super) async fn connect(
        &self,
        endpoint: &Endpoint,
        timeout: Duration,
    ) -> Result<Channel, tonic::transport::Error> {
        let observation = self.clone();
        let connector = service_fn(move |uri: Uri| {
            let dial = observation.start(&uri);
            let resolver = service_fn({
                let dial = dial.clone();
                move |name| {
                    let lookup = DNSService.call(name);
                    let dial = dial.clone();
                    async move {
                        if let Some(dial) = &dial {
                            dial.update(|d| d.dns = UdfConnectResult::Unknown);
                        }
                        let result = lookup.await;
                        if let Some(dial) = &dial {
                            dial.update(|d| {
                                if result.is_ok() {
                                    d.dns = UdfConnectResult::Ok;
                                    d.tcp = UdfConnectResult::Unknown;
                                } else {
                                    d.dns = UdfConnectResult::Failed;
                                }
                            });
                        }
                        result
                    }
                }
            });
            // Keep the existing resolver, address selection, socket options and timeouts.
            let mut tcp = HttpConnector::new_with_resolver(resolver);
            tcp.enforce_http(false);
            tcp.set_nodelay(true);
            tcp.set_keepalive(Some(Duration::from_secs(UDF_TCP_KEEP_ALIVE_SEC)));
            tcp.set_connect_timeout(Some(timeout));
            tcp.set_reuse_address(true);
            let connect = tcp.call(uri);
            async move {
                let result = connect.await;
                if let Some(dial) = &dial {
                    dial.update(|d| match &result {
                        Ok(socket) => {
                            d.tcp = UdfConnectResult::Ok;
                            d.initial_peer_ip = socket.inner().peer_addr().ok().map(|a| a.ip());
                            if d.tls != UdfConnectResult::NotApplicable {
                                d.tls = UdfConnectResult::Unknown;
                            }
                        }
                        Err(error) if d.tcp == UdfConnectResult::Unknown => {
                            d.tcp = if find_io_error_kind(error) == Some(io::ErrorKind::TimedOut) {
                                UdfConnectResult::Timeout
                            } else {
                                UdfConnectResult::Failed
                            };
                        }
                        Err(_) => {}
                    });
                }
                result
            }
        });
        let result = endpoint.connect_with_connector(connector).await;
        self.update(|d| match &result {
            Ok(_) if d.tls == UdfConnectResult::Unknown => d.tls = UdfConnectResult::Ok,
            Err(error) => d.finish_error(error),
            _ => {}
        });
        result
    }
}

impl UdfConnectDiagnostics {
    fn finish_error(&mut self, error: &tonic::transport::Error) {
        if find_io_error_kind(error) == Some(io::ErrorKind::TimedOut) {
            // Tonic's connector timeout covers DNS, TCP and TLS, but not HTTP/2 setup.
            if let Some(stage) = [&mut self.dns, &mut self.tcp, &mut self.tls]
                .into_iter()
                .find(|stage| **stage == UdfConnectResult::Unknown)
            {
                *stage = UdfConnectResult::Timeout;
            }
        } else if self.tls == UdfConnectResult::Unknown
            && error
                .source()
                .is_some_and(|e| e.is::<tonic::ConnectError>())
        {
            // In tonic 0.14 the connector wraps TLS/ALPN errors in ConnectError;
            // HTTP/2 setup runs outside that wrapper. Don't classify from error text.
            self.tls = UdfConnectResult::Failed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redial_does_not_replace_initial_peer() {
        let observation = ConnectObservation::default();
        let initial = observation
            .start(&Uri::from_static("https://initial.example.com"))
            .unwrap();
        let peer: IpAddr = "192.0.2.1".parse().unwrap();
        initial.update(|d| {
            d.dns = UdfConnectResult::Ok;
            d.tcp = UdfConnectResult::Ok;
            d.tls = UdfConnectResult::Ok;
            d.initial_peer_ip = Some(peer);
        });

        assert!(!observation.snapshot().redial_started);
        assert!(
            observation
                .start(&Uri::from_static("https://retry.example.com"))
                .is_none()
        );
        let result = observation.snapshot();
        assert!(result.redial_started);
        assert_eq!(result.initial_peer_ip, Some(peer));
        assert_eq!(result.dns, UdfConnectResult::Ok);
        assert_eq!(result.tcp, UdfConnectResult::Ok);
        assert_eq!(result.tls, UdfConnectResult::Ok);
        assert!(!format!("{result:?}").contains(&peer.to_string()));
    }
}
