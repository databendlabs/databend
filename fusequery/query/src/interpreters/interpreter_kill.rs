// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::{UseDatabasePlan, KillPlan};
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct KillInterpreter {
    ctx: FuseQueryContextRef,
    plan: KillPlan,
}

impl KillInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: KillPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(KillInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for KillInterpreter {
    fn name(&self) -> &str {
        "KillInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let sessions = self.ctx.get_sessions_manager();
        let kill_session = sessions.get_session(&self.plan.id)?;

        match self.plan.kill_connection {
            true => kill_session.force_kill_session(),
            false => kill_session.force_kill_query(),
        }

        let schema = Arc::new(DataSchema::empty());
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
