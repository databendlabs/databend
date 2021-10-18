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

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[cfg(not(target_os = "windows"))]
use common_base::tokio::signal::unix::signal;
#[cfg(not(target_os = "windows"))]
use common_base::tokio::signal::unix::Signal;
#[cfg(not(target_os = "windows"))]
use common_base::tokio::signal::unix::SignalKind;
#[cfg(target_os = "windows")]
use common_base::tokio::signal::windows::ctrl_c;
#[cfg(target_os = "windows")]
use common_base::tokio::signal::windows::CtrlC;
use common_exception::Result;
use futures::stream::Abortable;
use futures::Future;
use futures::Stream;
use futures::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::sessions::SessionManagerRef;

pub type ListeningStream = Abortable<TcpListenerStream>;

#[async_trait::async_trait]
pub trait Server {
    async fn shutdown(&mut self);

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr>;
}

pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
    sessions: SessionManagerRef,
    services: Vec<Box<dyn Server>>,
}

impl ShutdownHandle {
    pub fn create(sessions: SessionManagerRef) -> ShutdownHandle {
        ShutdownHandle {
            services: vec![],
            sessions,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn shutdown(&mut self, signal: Option<SignalStream>) -> impl Future<Output = ()> + '_ {
        let mut shutdown_jobs = vec![];
        for service in &mut self.services {
            shutdown_jobs.push(service.shutdown());
        }

        let sessions = self.sessions.clone();
        let join_all = futures::future::join_all(shutdown_jobs);
        async move {
            let cluster_discovery = sessions.get_cluster_discovery();
            cluster_discovery.unregister_to_metastore().await;

            join_all.await;
            sessions.shutdown(signal).await;
        }
    }

    pub async fn wait_for_termination_request(&mut self) {
        match ShutdownSignalStream::create() {
            Err(cause) => {
                log::error!("Cannot set shutdown signal handler, {:?}", cause);
                std::process::exit(1);
            }
            Ok(mut stream) => {
                stream.next().await;

                log::info!("Received termination signal.");
                log::info!("You can press Ctrl + C again to force shutdown.");
                if let Ok(false) =
                    self.shutdown
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
                {
                    let shutdown_services = self.shutdown(Some(stream));
                    shutdown_services.await;
                }
            }
        }
    }

    pub fn add_service(&mut self, service: Box<dyn Server>) {
        self.services.push(service);
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Ok(false) =
            self.shutdown
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
        {
            futures::executor::block_on(self.shutdown(None));
        }
    }
}

#[cfg(not(target_os = "windows"))]
struct ShutdownSignalStream {
    hangup_signal: Signal,
    sigint_signal: Signal,
    sigterm_signal: Signal,
}

#[cfg(target_os = "windows")]
struct ShutdownSignalStream {
    ctrl_c: CtrlC,
}

#[cfg(not(target_os = "windows"))]
impl ShutdownSignalStream {
    pub fn create() -> Result<SignalStream> {
        Ok(Box::pin(ShutdownSignalStream {
            hangup_signal: signal(SignalKind::hangup())?,
            sigint_signal: signal(SignalKind::interrupt())?,
            sigterm_signal: signal(SignalKind::terminate())?,
        }))
    }
}

#[cfg(target_os = "windows")]
impl ShutdownSignalStream {
    pub fn create() -> Result<SignalStream> {
        Ok(Box::pin(ShutdownSignalStream { ctrl_c: ctrl_c()? }))
    }
}

#[cfg(not(target_os = "windows"))]
impl Stream for ShutdownSignalStream {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut_self = self.get_mut();
        if let Poll::Ready(res) = mut_self.hangup_signal.poll_recv(cx) {
            return Poll::Ready(res);
        }

        if let Poll::Ready(res) = mut_self.sigint_signal.poll_recv(cx) {
            return Poll::Ready(res);
        }

        if let Poll::Ready(res) = mut_self.sigterm_signal.poll_recv(cx) {
            return Poll::Ready(res);
        }

        Poll::Pending
    }
}

#[cfg(target_os = "windows")]
impl Stream for ShutdownSignalStream {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().ctrl_c.poll_recv(cx)
    }
}

type SignalStream = Pin<Box<dyn Stream<Item = ()>>>;
