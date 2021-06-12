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
use crate::servers::mysql::endpoints::{IMySQLEndpoint, MySQLOnInitEndpoint};
use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef, ISession, SessionCreator, SessionManagerRef, SessionStatus};
use crate::sql::PlanParser;

pub struct Session {
    session_id: String,
    session_manager: SessionManagerRef,
    session_status: Arc<Mutex<SessionStatus>>,

    aborted_notify: Arc<tokio::sync::Notify>
}

impl ISession for Session {
    fn get_id(&self) -> String {
        self.session_id.clone()
    }

    fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        let mut context = FuseQueryContext::try_create()?;
        context = context.with_cluster(self.session_manager.get_cluster())?;
        // TODO: init context with session status

        return Ok(context);
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
        let abort_notify = self.aborted_notify.clone();
        let stream = stream.into_std().map_err_to_code(ErrorCode::TokioError, || "")?;
        stream.set_nonblocking(false).map_err_to_code(ErrorCode::TokioError, || "")?;

        let session = self.session_manager.get_session(&self.session_id)?;

        std::thread::spawn(move || {
            if let Err(error) = MysqlIntermediary::run_on_tcp(InteractiveWorker::create(session), stream) {
                log::error!("Unexpected error occurred during query execution: {:?}", error);
            };

            abort_notify.notify_waiters();
        });

        Ok(())
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        match duration {
            None => {
                self.aborted_notify.notified().await;
                self.session_manager.destroy_session(self);
            }
            Some(duration) => {
                tokio::time::timeout(duration, self.aborted_notify.notified())
                    .await
                    .map_err_to_code(ErrorCode::Timeout, || "")?;

                self.session_manager.destroy_session(self);
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
                aborted_notify: Arc::new(tokio::sync::Notify::new())
            }
        ))
    }
}
