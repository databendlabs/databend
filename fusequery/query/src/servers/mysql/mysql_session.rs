use std::io;
use std::ops::Sub;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::future::{Abortable, Aborted, AbortHandle};
use futures::TryFutureExt;
use msql_srv::{ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter};
use msql_srv::MysqlIntermediary;
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;

use common_datablocks::DataBlock;
use common_exception::{ErrorCode, ToErrorCode};
use common_exception::Result;
use common_infallible::Mutex;

use crate::clusters::{Cluster, ClusterRef};
use crate::interpreters::{IInterpreter, InterpreterFactory};
use crate::servers::{Elapsed, RunnableService};
use crate::servers::mysql::endpoints::{IMySQLEndpoint, MySQLOnInitEndpoint, MySQLOnQueryEndpoint};
use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef, ISession, SessionCreator, SessionManagerRef, SessionStatus};
use crate::sql::PlanParser;

pub struct Session {
    session_id: String,
    session_manager: SessionManagerRef,
    session_status: Arc<Mutex<SessionStatus>>,

    terminal_sender: Arc<Mutex<Option<Sender<()>>>>,
}

impl ISession for Session {
    fn get_id(&self) -> String {
        self.session_id.clone()
    }

    fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        let context = self.session_manager
            .try_create_context()?
            .with_cluster(self.session_manager.get_cluster())?;

        return Ok(context)
        // context.set_current_database(database.clone());
    }

    fn get_status(&self) -> Arc<Mutex<SessionStatus>> {
        self.session_status.clone()
    }
}

#[async_trait::async_trait]
impl RunnableService<TcpStream, ()> for Session {
    fn abort(&self, force: bool) {
        let mut session_status = self.session_status.lock();
        session_status.enter_abort(force);
    }

    async fn start(&self, stream: TcpStream) -> Result<()> {
        let stream = stream.into_std().map_err_to_code(ErrorCode::TokioError, || "")?;
        stream.set_nonblocking(false).map_err_to_code(ErrorCode::TokioError, || "")?;

        let session = self.session_manager.get_session(&self.session_id)?;

        let handler = std::thread::spawn(move || {
            if let Err(error) = MysqlIntermediary::run_on_tcp(InteractiveWorker::create(session), stream) {
                log::error!("Unexpected error occurred during query execution: {:?}", error);
            };
        });

        self.started(handler)
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        let subscribe = || {
            let terminal_sender = self.terminal_sender.lock();
            match &*terminal_sender {
                None => Err(ErrorCode::LogicalError("Logical error, call wait_terminal before start.")),
                Some(terminal_watchers) => Ok(Some(terminal_watchers.clone().subscribe()))
            }
        };

        match duration {
            None => {
                if let Some(mut rx) = subscribe()? {
                    rx.recv().await;
                }
            }
            Some(duration) => {
                if let Some(mut rx) = subscribe()? {
                    tokio::time::timeout(duration, rx.recv())
                        .await
                        .map_err_to_code(ErrorCode::Timeout, || "")?;
                }
            }
        };

        Ok(instant.elapsed())
    }
}

impl SessionCreator for Session {
    type Session = Self;

    fn create(session_id: String, sessions: SessionManagerRef) -> Arc<Box<dyn ISession>> {
        Arc::new(Box::new(
            Session {
                session_id,
                session_manager: sessions,
                session_status: Arc::new(Mutex::new(SessionStatus::create())),
                terminal_sender: Arc::new(Mutex::new(None)),
            }
        ))
    }
}

impl Session {
    fn started(&self, handler: JoinHandle<()>) -> Result<()> {
        {
            let (terminal_sender, _) = tokio::sync::broadcast::channel(1);
            *self.terminal_sender.lock() = Some(terminal_sender);
        }

        // let session_manager = self.session_manager.clone();
        let terminal_sender = self.terminal_sender.clone();

        tokio::spawn(async move {
            handler.join().expect("Cannot join.");
            // session_manager.destroy_session::<Self>(self);

            if let Some(terminal_watchers) = terminal_sender.lock().take() {
                let _ = terminal_watchers.send(());
            }
        });

        Ok(())
    }
}
