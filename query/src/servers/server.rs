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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::signal_stream;
use common_base::DummySignalStream;
use common_base::SignalStream;
use common_base::SignalType;
use common_exception::Result;
use futures::stream::Abortable;
use futures::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::sessions::SessionManagerRef;

pub type ListeningStream = Abortable<TcpListenerStream>;

#[async_trait::async_trait]
pub trait Server: Send {
    async fn shutdown(&mut self, graceful: bool);
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
            sessions,
            services: vec![],
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
    async fn shutdown_services(&mut self, graceful: bool) {
        let mut shutdown_jobs = vec![];
        for service in &mut self.services {
            shutdown_jobs.push(service.shutdown(graceful));
        }
        futures::future::join_all(shutdown_jobs).await;
    }

    pub async fn shutdown(&mut self, mut signal: SignalStream) {
        self.shutdown_services(true).await;
        self.sessions
            .get_cluster_discovery()
            .unregister_to_metastore(&mut signal)
            .await;
        self.sessions.graceful_shutdown(signal, 5).await;
        self.shutdown_services(false).await;
    }

    pub async fn wait_for_termination_request(&mut self) {
        match signal_stream() {
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
                    let shutdown_services = self.shutdown(stream);
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
            let signal_stream = DummySignalStream::create(SignalType::Exit);
            futures::executor::block_on(self.shutdown(signal_stream));
        }
    }
}
