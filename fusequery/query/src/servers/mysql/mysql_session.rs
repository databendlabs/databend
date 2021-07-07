// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_exception::exception::ABORT_SESSION;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::exit_scope;
use common_infallible::Mutex;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpStream;
use msql_srv::MysqlIntermediary;

use crate::configs::Config;
use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::ISession;
use crate::sessions::SessionCreator;
use crate::sessions::SessionMgrRef;
use crate::sessions::SessionStatus;

pub struct Session {
    conf: Config,
    session_id: String,
    session_manager: SessionMgrRef,
    session_status: Arc<Mutex<SessionStatus>>,

    aborted_notify: Arc<tokio::sync::Notify>,
}

impl ISession for Session {
    fn get_id(&self) -> String {
        self.session_id.clone()
    }

    fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        self.session_status
            .lock()
            .try_create_context(self.conf.clone(), self.session_manager.get_datasource())
    }

    fn get_status(&self) -> Arc<Mutex<SessionStatus>> {
        self.session_status.clone()
    }
}

#[async_trait::async_trait]
impl AbortableService<TcpStream, ()> for Session {
    fn abort(&self, force: bool) -> Result<()> {
        self.session_status.lock().abort_session(force)
    }

    async fn start(&self, stream: TcpStream) -> Result<()> {
        let abort_notify = self.aborted_notify.clone();
        let session_manager = self.session_manager.clone();
        let stream = stream
            .into_std()
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;
        stream
            .set_nonblocking(false)
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;

        let cloned_stream = stream.try_clone()?;
        let session = session_manager.get_session(&self.session_id)?;

        std::thread::spawn(move || {
            session.get_status().lock().enter_init(cloned_stream);

            let session_ref = session.clone();
            exit_scope!({
                session_ref.get_status().lock().enter_aborted();
                session_manager.destroy_session(session_ref.get_id());
                abort_notify.notify_waiters();
            });

            if let Err(error) =
                MysqlIntermediary::run_on_tcp(InteractiveWorker::create(session.clone()), stream)
            {
                if error.code() != ABORT_SESSION {
                    log::error!(
                        "Unexpected error occurred during query execution: {:?}",
                        error
                    );
                }
            };
        });

        Ok(())
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        if self.session_status.lock().is_aborted() {
            return Ok(instant.elapsed());
        }

        match duration {
            None => {
                self.aborted_notify.notified().await;
            }
            Some(duration) => {
                match tokio::time::timeout(duration, self.aborted_notify.notified()).await {
                    Ok(_) => { /* do nothing */ }
                    Err(_) => {
                        return Err(ErrorCode::Timeout(format!(
                            "Session did not close in {:?}",
                            duration
                        )))
                    }
                };
            }
        };

        Ok(instant.elapsed())
    }
}

impl SessionCreator for Session {
    type Session = Self;

    fn create(
        conf: Config,
        session_id: String,
        sessions: SessionMgrRef,
    ) -> Result<Arc<Box<dyn ISession>>> {
        Ok(Arc::new(Box::new(Session {
            conf,
            session_id,
            session_manager: sessions,
            session_status: Arc::new(Mutex::new(SessionStatus::try_create()?)),
            aborted_notify: Arc::new(tokio::sync::Notify::new()),
        })))
    }
}
