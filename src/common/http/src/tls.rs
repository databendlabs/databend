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

//! TLS for poem HTTP servers, backed by rustls + ring.
//!
//! poem's own `rustls` listener hard-codes the aws-lc crypto provider, and its `openssl-tls`
//! listener requires building OpenSSL. Both are heavy C builds on the critical path of a
//! clean build, so we provide a small acceptor on top of `tokio-rustls` instead.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use futures::future::BoxFuture;
use poem::listener::Acceptor;
use poem::web::LocalAddr;
use poem::web::RemoteAddr;
use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio_rustls::server::TlsStream;

/// Server certificate and private key (PEM files) for an HTTPS listener.
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    cert_path: String,
    key_path: String,
}

impl TlsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// PEM file containing the certificate chain, leaf first.
    #[must_use]
    pub fn cert_from_file(mut self, path: impl Into<String>) -> Self {
        self.cert_path = path.into();
        self
    }

    /// PEM file containing a PKCS#1, PKCS#8 or SEC1 private key.
    #[must_use]
    pub fn key_from_file(mut self, path: impl Into<String>) -> Self {
        self.key_path = path.into();
        self
    }

    pub(crate) fn server_config(&self) -> io::Result<ServerConfig> {
        let certs = CertificateDer::pem_file_iter(&self.cert_path)
            .and_then(|iter| iter.collect::<Result<Vec<_>, _>>())
            .map_err(|e| invalid_input(format!("{}: {e}", self.cert_path)))?;
        if certs.is_empty() {
            return Err(invalid_input(format!(
                "no certificate found in {}",
                self.cert_path
            )));
        }

        let key = PrivateKeyDer::from_pem_file(&self.key_path)
            .map_err(|e| invalid_input(format!("{}: {e}", self.key_path)))?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| invalid_input(format!("invalid certificate or key: {e}")))?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(config)
    }
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// Wraps a plain acceptor and performs the TLS handshake lazily, on first read/write of the
/// accepted stream, so that a slow client cannot block the accept loop.
pub(crate) struct TlsAcceptor<A> {
    inner: A,
    tls: tokio_rustls::TlsAcceptor,
}

impl<A> TlsAcceptor<A> {
    pub(crate) fn new(inner: A, config: ServerConfig) -> Self {
        Self {
            inner,
            tls: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
        }
    }
}

impl<A: Acceptor> Acceptor for TlsAcceptor<A> {
    type Io = HandshakeStream<TlsStream<A::Io>>;

    fn local_addr(&self) -> Vec<LocalAddr> {
        self.inner.local_addr()
    }

    async fn accept(&mut self) -> io::Result<(Self::Io, LocalAddr, RemoteAddr, http::uri::Scheme)> {
        let (stream, local_addr, remote_addr, _) = self.inner.accept().await?;
        let handshake = self.tls.accept(stream);
        Ok((
            HandshakeStream::Handshaking(handshake.boxed()),
            local_addr,
            remote_addr,
            http::uri::Scheme::HTTPS,
        ))
    }
}

pub(crate) enum HandshakeStream<S> {
    Handshaking(BoxFuture<'static, io::Result<S>>),
    Ready(S),
    Failed,
}

impl<S: Unpin> HandshakeStream<S> {
    /// Drives the handshake; returns the established stream once ready.
    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut S>> {
        if let HandshakeStream::Handshaking(fut) = self {
            match fut.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(s)) => *self = HandshakeStream::Ready(s),
                Poll::Ready(Err(e)) => {
                    *self = HandshakeStream::Failed;
                    return Poll::Ready(Err(e));
                }
            }
        }
        match self {
            HandshakeStream::Ready(s) => Poll::Ready(Ok(s)),
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tls handshake failed",
            ))),
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for HandshakeStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(s)) => Pin::new(s).poll_read(cx, buf),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for HandshakeStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(s)) => Pin::new(s).poll_write(cx, buf),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(s)) => Pin::new(s).poll_flush(cx),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            // Nothing to shut down if the handshake never completed.
            HandshakeStream::Ready(s) => Pin::new(s).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}
