// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::FuseQueryResult;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef};

pub struct SessionManager {
    sessions: Mutex<HashMap<String, FuseQueryContextRef>>,
}

pub type SessionManagerRef = Arc<SessionManager>;

impl SessionManager {
    pub fn create() -> SessionManagerRef {
        Arc::new(SessionManager {
            sessions: Mutex::new(HashMap::new()),
        })
    }

    pub fn try_create_context(&self) -> FuseQueryResult<FuseQueryContextRef> {
        let ctx = FuseQueryContext::try_create_ctx()?;
        self.sessions.lock()?.insert(ctx.get_id(), ctx.clone());
        Ok(ctx)
    }

    pub fn try_remove_context(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<()> {
        self.sessions.lock()?.remove(&*ctx.get_id());
        Ok(())
    }
}
