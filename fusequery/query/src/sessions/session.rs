// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use common_infallible::Mutex;
use common_planners::Partitions;
use metrics::counter;

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

    pub fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let ctx = FuseQueryContext::try_create()?;
        self.sessions.lock().insert(ctx.get_id()?, ctx.clone());
        Ok(ctx)
    }

    pub fn try_remove_context(&self, ctx: FuseQueryContextRef) -> Result<()> {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.sessions.lock().remove(&*ctx.get_id()?);
        Ok(())
    }

    /// Fetch nums partitions from session manager by context id.
    pub fn try_fetch_partitions(&self, ctx_id: String, nums: usize) -> Result<Partitions> {
        let session_map = self.sessions.lock();
        let ctx = session_map.get(&*ctx_id).ok_or_else(|| {
            return anyhow::Error::msg(format!("Unsupported context id: {}", ctx_id));
        })?;
        ctx.try_get_partitions(nums)
    }
}
