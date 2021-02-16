// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use metrics::counter;

use crate::error::FuseQueryResult;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef};

pub struct Session {
    sessions: Mutex<HashMap<String, FuseQueryContextRef>>,
}

pub type SessionRef = Arc<Session>;

impl Session {
    pub fn create() -> SessionRef {
        Arc::new(Session {
            sessions: Mutex::new(HashMap::new()),
        })
    }

    pub fn try_create_context(&self) -> FuseQueryResult<FuseQueryContextRef> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let ctx = FuseQueryContext::try_create()?;
        self.sessions.lock()?.insert(ctx.get_id()?, ctx.clone());
        Ok(ctx)
    }

    pub fn try_remove_context(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<()> {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.sessions.lock()?.remove(&*ctx.get_id()?);
        Ok(())
    }
}
