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

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::signal_stream;
use databend_common_base::base::DummySignalStream;
use databend_common_base::base::SignalStream;
use databend_common_base::base::SignalType;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use futures::stream::Abortable;
use futures::StreamExt;
use log::error;
use log::info;
use tokio_stream::wrappers::TcpListenerStream;

use crate::clusters::ClusterDiscovery;
use crate::sessions::SessionManager;

pub type ListeningStream = Abortable<TcpListenerStream>;

#[async_trait::async_trait]
pub trait Server: Send {
    async fn shutdown(&mut self, graceful: bool);
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr>;
}

pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
    sessions: Arc<SessionManager>,
    services: Vec<(&'static str, Box<dyn Server>)>,
}

impl ShutdownHandle {
    pub fn create() -> Result<ShutdownHandle> {
        Ok(ShutdownHandle {
            sessions: SessionManager::instance(),
            services: vec![],
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }
    #[async_backtrace::framed]
    async fn shutdown_services(&mut self, graceful: bool) {
        let mut shutdown_jobs = vec![];
        for (name, service) in &mut self.services {
            shutdown_jobs.push(async move {
                info!("Stopping {} service", name);
                service.shutdown(graceful).await;
                info!("Stopped {} service", name);
            });
        }
        futures::future::join_all(shutdown_jobs).await;
    }

    #[async_backtrace::framed]
    pub async fn shutdown(&mut self, mut signal: SignalStream, timeout: Option<Duration>) {
        self.shutdown_services(true).await;
        ClusterDiscovery::instance()
            .unregister_to_metastore(&mut signal)
            .await;
        self.sessions.graceful_shutdown(signal, timeout).await;
        self.shutdown_services(false).await;
    }

    #[async_backtrace::framed]
    pub async fn wait_for_termination_request(&mut self, timeout: Option<Duration>) {
        match signal_stream() {
            Err(cause) => {
                error!("Cannot set shutdown signal handler, {:?}", cause);
                std::process::exit(1);
            }
            Ok(mut stream) => {
                stream.next().await;

                info!("Received termination signal.");
                if let Ok(false) =
                    self.shutdown
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
                {
                    let shutdown_services = self.shutdown(stream, timeout);
                    shutdown_services.await;
                }
            }
        }
    }

    pub fn add_service(&mut self, name: &'static str, service: Box<dyn Server>) {
        self.services.push((name, service));
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        drop_guard(move || {
            if let Ok(false) =
                self.shutdown
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            {
                let signal_stream = DummySignalStream::create(SignalType::Exit);
                futures::executor::block_on(
                    self.shutdown(signal_stream, Some(Duration::from_secs(5))),
                );
            }
        })
    }
}
