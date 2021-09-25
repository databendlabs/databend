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

use common_base::tokio;
use common_base::tokio::sync::mpsc::Receiver;
use common_exception::Result;
use futures::stream::Abortable;
use futures::Future;
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

    pub fn shutdown(&mut self, signal: Option<Receiver<()>>) -> impl Future<Output = ()> + '_ {
        let mut shutdown_jobs = vec![];
        for service in &mut self.services {
            shutdown_jobs.push(service.shutdown());
        }

        let sessions = self.sessions.clone();
        let join_all = futures::future::join_all(shutdown_jobs);
        async move {
            join_all.await;
            sessions.shutdown(signal).await;
        }
    }

    pub fn wait_for_termination_request(&mut self) -> impl Future<Output = ()> + '_ {
        let mut receiver = Self::register_termination_handle();
        async move {
            receiver.recv().await;

            log::info!("Received termination signal.");
            log::info!("You can press Ctrl + C again to force shutdown.");
            if let Ok(false) =
                self.shutdown
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            {
                let shutdown_services = self.shutdown(Some(receiver));
                shutdown_services.await;
            }
        }
    }

    pub fn register_termination_handle() -> Receiver<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        ctrlc::set_handler(move || {
            if let Err(error) = tx.blocking_send(()) {
                log::error!("Could not send signal on channel {}", error);
                std::process::exit(1);
            }
        })
        .expect("Error setting Ctrl-C handler");

        rx
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

// TODO(winter & sundy), this is just a mock
// Introduce user_mgr and block on to get user by user_name
pub(crate) mod mock {
    use common_exception::ErrorCode;
    use common_exception::Result;
    use common_management::AuthType;
    use common_management::NewUser;
    use common_management::UserInfo;

    pub fn get_mock_user(user: &str) -> Result<UserInfo> {
        match user {
            "default" | "" | "root" => {
                let user = NewUser::new(user, "", AuthType::None);
                Ok(user.into())
            }
            "default_plain" => {
                let user = NewUser::new(user, "default", AuthType::PlainText);
                Ok(user.into())
            }

            "default_double_sha1" => {
                let user = NewUser::new(user, "default", AuthType::DoubleSha1);
                Ok(user.into())
            }

            "default_sha2" => {
                let user = NewUser::new(user, "default", AuthType::Sha256);
                Ok(user.into())
            }

            _ => Err(ErrorCode::UnknownUser(format!("User: {} not found", user))),
        }
    }
}
