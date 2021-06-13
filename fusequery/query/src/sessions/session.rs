use std::sync::Arc;

use tokio::net::TcpStream;

use common_exception::Result;
use common_infallible::Mutex;

use crate::servers::AbortableService;
use crate::sessions::{FuseQueryContextRef, SessionManagerRef, SessionStatus};

pub trait SessionCreator {
    type Session: ISession;

    fn create(id: String, sessions: SessionManagerRef) -> Result<Arc<Box<dyn ISession>>>;
}

pub trait ISession: AbortableService<TcpStream, ()> + Send + Sync {
    fn get_id(&self) -> String;

    fn try_create_context(&self) -> Result<FuseQueryContextRef>;

    fn get_status(&self) -> Arc<Mutex<SessionStatus>>;
}
