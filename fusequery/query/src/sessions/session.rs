// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_infallible::Mutex;
use common_runtime::tokio::net::TcpStream;

use crate::configs::Config;
use crate::servers::AbortableService;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionMgrRef;
use crate::sessions::SessionStatus;

pub trait SessionCreator {
    type Session: ISession;

    fn create(conf: Config, id: String, sessions: SessionMgrRef) -> Result<Arc<Box<dyn ISession>>>;
}

pub trait ISession: AbortableService<TcpStream, ()> + Send + Sync {
    fn get_id(&self) -> String;

    fn try_create_context(&self) -> Result<FuseQueryContextRef>;

    fn get_status(&self) -> Arc<Mutex<SessionStatus>>;
}
