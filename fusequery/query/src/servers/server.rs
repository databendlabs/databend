// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use common_exception::Result;
use futures::stream::Abortable;
use tokio_stream::wrappers::TcpListenerStream;
use crate::sessions::SessionManagerRef;
use futures::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::Receiver;

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

    pub fn shutdown(&mut self, signal: Option<Receiver<()>>) -> impl Future<Output=()> + '_ {
        let mut shutdown_jobs = vec![];
        for service in &mut self.services {
            shutdown_jobs.push(service.shutdown());
        }

        let shutdown = self.shutdown.clone();
        let sessions = self.sessions.clone();
        let join_all = futures::future::join_all(shutdown_jobs);
        async move {
            if !shutdown.load(Ordering::Relaxed) {
                join_all.await;
                sessions.shutdown(signal).await;
                shutdown.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn wait_for_termination_request(&mut self) -> impl Future<Output=()> + '_ {
        let mut receiver = Self::register_termination_handle();
        async move {
            receiver.recv().await;

            log::info!("Received termination signal.");
            log::info!("You can press Ctrl + C again to force shutdown.");
            let shutdown_services = self.shutdown(Some(receiver));
            shutdown_services.await;
        }
    }

    pub fn register_termination_handle() -> Receiver<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        ctrlc::set_handler(move || {
            if let Err(error) = tx.blocking_send(()) {
                log::error!("Could not send signal on channel {}", error);
                std::process::exit(1);
            }
        }).expect("Error setting Ctrl-C handler");

        rx
    }

    pub fn add_service(&mut self, service: Box<dyn Server>) {
        self.services.push(service);
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if !self.shutdown.load(Ordering::Relaxed) {
            futures::executor::block_on(self.shutdown(None));
        }
    }
}
